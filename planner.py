"""单库自增序列迁移到分片的规划器。

纯内存计算：根据当前序列值、步长、分片范围、预分配大小、并发申请与
保留区间，产出 预占(PREALLOCATE) / 切换(SWITCH) / 补号(BACKFILL) /
回滚(ROLLBACK) 四类步骤。本模块不连接、不修改任何真实数据库序列。

SegmentRegistry 提供号段发放的运行时模拟，分片范围校验与号段回收
均为幂等操作，可安全重复执行。
"""

import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

PREALLOCATE = "PREALLOCATE"
SWITCH = "SWITCH"
BACKFILL = "BACKFILL"
ROLLBACK = "ROLLBACK"


class PlanError(ValueError):
    """规划输入不合法。"""


class ExhaustedError(RuntimeError):
    """号段边界耗尽，无法再发号。"""


class RolledBackError(RuntimeError):
    """已回滚，禁止继续发号（已提交号码不得重新发放）。"""


@dataclass(frozen=True)
class ShardRange:
    shard_id: str
    start: int
    end: int  # 含端点

    def slots(self, step: int) -> int:
        if self.end < self.start:
            return 0
        return (self.end - self.start) // step + 1


@dataclass(frozen=True)
class PlannerInput:
    current_value: int          # 旧库下一个待发放的号码
    step: int                   # 序列步长
    shard_ranges: Tuple[ShardRange, ...]
    prealloc_size: int          # 每个分片预占的号码个数
    concurrent_requests: int    # 切换期间旧库仍可能在途的申请数
    reserved_ranges: Tuple[Tuple[int, int], ...] = ()

    @property
    def fence_high(self) -> int:
        """切换决策后旧库仍可能提交的最大号码（含一个在途缓冲）。"""
        return self.current_value + self.step * self.concurrent_requests


@dataclass(frozen=True)
class Step:
    kind: str
    shard_id: Optional[str]
    start: Optional[int]
    end: Optional[int]
    detail: str


@dataclass
class Plan:
    steps: List[Step] = field(default_factory=list)

    def to_dict(self) -> List[Dict]:
        return [
            {"kind": s.kind, "shard_id": s.shard_id,
             "start": s.start, "end": s.end, "detail": s.detail}
            for s in self.steps
        ]


def validate(inp: PlannerInput) -> None:
    """分片范围校验。重复调用结果一致（幂等，无副作用）。"""
    problems = []
    if inp.step < 1:
        problems.append("step 必须 >= 1")
    if inp.current_value < 0:
        problems.append("current_value 不能为负")
    if inp.prealloc_size < 1:
        problems.append("prealloc_size 必须 >= 1")
    if inp.concurrent_requests < 0:
        problems.append("concurrent_requests 不能为负")

    ranges = sorted(inp.shard_ranges, key=lambda r: (r.start, r.shard_id))
    seen_ids = set()
    for r in ranges:
        if r.shard_id in seen_ids:
            problems.append("分片 id 重复: %s" % r.shard_id)
        seen_ids.add(r.shard_id)
        if r.start > r.end:
            problems.append("分片 %s 范围倒置: [%d, %d]" % (r.shard_id, r.start, r.end))
            continue
        if inp.step >= 1 and r.start <= inp.fence_high:
            problems.append(
                "分片 %s 起点 %d 未越过安全围栏 %d（当前值 + 步长*在途申请）"
                % (r.shard_id, r.start, inp.fence_high))
        if inp.step >= 1 and r.slots(inp.step) < inp.prealloc_size:
            problems.append(
                "分片 %s 容量不足: 可用 %d 个号码 < 预占 %d 个（边界耗尽）"
                % (r.shard_id, r.slots(inp.step), inp.prealloc_size))

    for prev, nxt in zip(ranges, ranges[1:]):
        if prev.end >= nxt.start:
            problems.append(
                "分片范围重叠: %s[%d,%d] 与 %s[%d,%d]"
                % (prev.shard_id, prev.start, prev.end,
                   nxt.shard_id, nxt.start, nxt.end))

    reserved = sorted(inp.reserved_ranges)
    for lo, hi in reserved:
        if lo > hi:
            problems.append("保留区间倒置: [%d, %d]" % (lo, hi))
    for (lo, hi), r in ((rv, r) for rv in reserved for r in ranges):
        if lo <= r.end and r.start <= hi:
            problems.append(
                "保留区间 [%d,%d] 与分片 %s[%d,%d] 冲突"
                % (lo, hi, r.shard_id, r.start, r.end))

    if problems:
        raise PlanError("; ".join(problems))


class MigrationPlanner:
    """根据输入产出确定性的迁移步骤；同一输入重复规划结果完全一致。"""

    def plan(self, inp: PlannerInput) -> Plan:
        validate(inp)
        ranges = sorted(inp.shard_ranges, key=lambda r: (r.start, r.shard_id))
        plan = Plan()

        for r in ranges:
            seg_end = r.start + (inp.prealloc_size - 1) * inp.step
            plan.steps.append(Step(
                PREALLOCATE, r.shard_id, r.start, seg_end,
                "为分片 %s 预占 %d 个号码 [%d, %d]，步长 %d"
                % (r.shard_id, inp.prealloc_size, r.start, seg_end, inp.step)))

        for r in ranges:
            first_new = r.start + inp.prealloc_size * inp.step
            plan.steps.append(Step(
                SWITCH, r.shard_id, first_new, r.end,
                "分片 %s 切换后从 %d 起发号，旧库围栏于 %d（含 %d 个在途申请）"
                % (r.shard_id, first_new, inp.fence_high,
                   inp.concurrent_requests)))

        for lo, hi in self._gaps(inp, ranges):
            plan.steps.append(Step(
                BACKFILL, None, lo, hi,
                "空洞 [%d, %d] 共 %d 个号码，回收至补号池，禁止直接复用已提交号码"
                % (lo, hi, hi - lo + 1)))

        for r in reversed(ranges):
            plan.steps.append(Step(
                ROLLBACK, r.shard_id, r.start, r.end,
                "回滚分片 %s：未发放号码进入隔离区，已提交号码（<= 高水位）"
                "永久封存不得重发" % r.shard_id))
        return plan

    @staticmethod
    def _gaps(inp: PlannerInput, ranges: List[ShardRange]) -> List[Tuple[int, int]]:
        covered = sorted(
            [(r.start, r.end) for r in ranges] +
            [(lo, hi) for lo, hi in inp.reserved_ranges if lo <= hi])
        gaps = []
        cursor = inp.fence_high + 1
        for lo, hi in covered:
            if hi < cursor:
                continue
            if lo > cursor:
                gaps.append((cursor, lo - 1))
            cursor = max(cursor, hi + 1)
        return gaps


class SegmentRegistry:
    """号段发放模拟器。全部状态可 dump/restore，重启后不重复发号。

    - allocate 按 request_id 幂等：重复申请返回同一号码。
    - reclaim / rollback 幂等：重复执行是 no-op。
    - rollback 后已提交号码永久封存，未发放号码进入隔离区。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._segments: Dict[str, Dict] = {}
        self._requests: Dict[str, int] = {}
        self._issued: set = set()
        self._quarantine: set = set()
        self._rolled_back = False
        self.high_water_mark = 0

    def add_segment(self, shard_id: str, start: int, end: int, step: int) -> None:
        """幂等登记号段：相同参数重复登记是 no-op，参数冲突报错。"""
        with self._lock:
            existing = self._segments.get(shard_id)
            if existing is not None:
                same = (existing["start"] == start and existing["end"] == end
                        and existing["step"] == step)
                if not same:
                    raise PlanError("分片 %s 号段重复登记且参数不一致" % shard_id)
                return
            if start > end or step < 1:
                raise PlanError("分片 %s 号段非法: [%d, %d] step=%d"
                                % (shard_id, start, end, step))
            self._segments[shard_id] = {
                "start": start, "end": end, "step": step,
                "next": start, "reclaimed": False,
            }

    def allocate(self, shard_id: str, request_id: str) -> int:
        with self._lock:
            if self._rolled_back:
                raise RolledBackError("已回滚，禁止发号")
            if request_id in self._requests:
                return self._requests[request_id]  # 重复申请幂等返回
            seg = self._segments.get(shard_id)
            if seg is None:
                raise PlanError("分片 %s 未登记号段" % shard_id)
            if seg["reclaimed"] or seg["next"] > seg["end"]:
                raise ExhaustedError("分片 %s 号段耗尽" % shard_id)
            number = seg["next"]
            seg["next"] += seg["step"]
            self._requests[request_id] = number
            self._issued.add(number)
            self.high_water_mark = max(self.high_water_mark, number)
            return number

    def reclaim(self, shard_id: str) -> Optional[Tuple[int, int]]:
        """回收分片未发放的尾部号段，幂等：重复调用返回相同结果。"""
        with self._lock:
            seg = self._segments.get(shard_id)
            if seg is None:
                raise PlanError("分片 %s 未登记号段" % shard_id)
            if seg["reclaimed"]:
                tail = seg.get("reclaimed_tail")
                return tuple(tail) if tail else None
            if seg["next"] > seg["end"]:
                seg["reclaimed"] = True
                seg["reclaimed_tail"] = None
                return None
            tail = (seg["next"], seg["end"])
            n = seg["next"]
            while n <= seg["end"]:
                self._quarantine.add(n)
                n += seg["step"]
            seg["reclaimed"] = True
            seg["reclaimed_tail"] = tail
            return tail

    def rollback(self) -> int:
        """幂等回滚：回收全部号段，返回高水位。已提交号码永不重发。"""
        with self._lock:
            if self._rolled_back:
                return self.high_water_mark
            for shard_id in list(self._segments):
                seg = self._segments[shard_id]
                if not seg["reclaimed"]:
                    n = seg["next"]
                    while n <= seg["end"]:
                        self._quarantine.add(n)
                        n += seg["step"]
                    seg["reclaimed"] = True
                    seg["reclaimed_tail"] = (
                        (seg["next"], seg["end"]) if seg["next"] <= seg["end"] else None)
            self._rolled_back = True
            return self.high_water_mark

    def is_quarantined(self, number: int) -> bool:
        return number in self._quarantine

    def dump(self) -> Dict:
        """导出 JSON 可序列化状态，用于模拟数据库重启后的恢复。"""
        with self._lock:
            return {
                "segments": {k: dict(v) for k, v in self._segments.items()},
                "requests": dict(self._requests),
                "issued": sorted(self._issued),
                "quarantine": sorted(self._quarantine),
                "rolled_back": self._rolled_back,
                "high_water_mark": self.high_water_mark,
            }

    @classmethod
    def restore(cls, state: Dict) -> "SegmentRegistry":
        reg = cls()
        reg._segments = {k: dict(v) for k, v in state["segments"].items()}
        reg._requests = dict(state["requests"])
        reg._issued = set(state["issued"])
        reg._quarantine = set(state["quarantine"])
        reg._rolled_back = state["rolled_back"]
        reg.high_water_mark = state["high_water_mark"]
        return reg
