"""python -m unittest 执行。全部用例纯内存运行，不触碰真实数据库。"""

import json
import unittest
from concurrent.futures import ThreadPoolExecutor

from planner import (
    BACKFILL,
    PREALLOCATE,
    ROLLBACK,
    SWITCH,
    ExhaustedError,
    MigrationPlanner,
    PlanError,
    PlannerInput,
    RolledBackError,
    SegmentRegistry,
    ShardRange,
    validate,
)


def make_input(**overrides):
    base = dict(
        current_value=1000,
        step=10,
        shard_ranges=(
            ShardRange("shard-a", 2000, 2990),
            ShardRange("shard-b", 4000, 4990),
        ),
        prealloc_size=5,
        concurrent_requests=3,
        reserved_ranges=((3000, 3090),),
    )
    base.update(overrides)
    return PlannerInput(**base)


class PlanShapeTest(unittest.TestCase):
    def test_plan_contains_all_four_phases_in_order(self):
        plan = MigrationPlanner().plan(make_input())
        kinds = [s.kind for s in plan.steps]
        self.assertEqual(
            kinds,
            [PREALLOCATE, PREALLOCATE, SWITCH, SWITCH,
             BACKFILL, BACKFILL, BACKFILL, ROLLBACK, ROLLBACK],
        )

    def test_prealloc_segment_math(self):
        plan = MigrationPlanner().plan(make_input())
        pre = [s for s in plan.steps if s.kind == PREALLOCATE]
        self.assertEqual((pre[0].start, pre[0].end), (2000, 2040))  # 5 个号, 步长 10
        switch = [s for s in plan.steps if s.kind == SWITCH]
        self.assertEqual(switch[0].start, 2050)  # 预占之后的下一个可发号码

    def test_backfill_gaps_skip_reserved(self):
        plan = MigrationPlanner().plan(make_input())
        gaps = [(s.start, s.end) for s in plan.steps if s.kind == BACKFILL]
        # 围栏 1030 之后到 shard-a 之前；shard-a 与保留区间之间；保留区间与 shard-b 之间
        self.assertIn((1031, 1999), gaps)
        self.assertIn((2991, 2999), gaps)
        self.assertIn((3091, 3999), gaps)

    def test_plan_is_deterministic_and_repeatable(self):
        planner = MigrationPlanner()
        inp = make_input()
        self.assertEqual(planner.plan(inp).to_dict(), planner.plan(inp).to_dict())
        json.dumps(planner.plan(inp).to_dict())  # 输出可序列化


class ValidationTest(unittest.TestCase):
    def test_overlapping_shard_ranges_rejected(self):
        inp = make_input(shard_ranges=(
            ShardRange("a", 2000, 2990),
            ShardRange("b", 2500, 3490),
        ))
        with self.assertRaises(PlanError):
            validate(inp)

    def test_reserved_range_conflict_rejected(self):
        inp = make_input(reserved_ranges=((2500, 2600),))
        with self.assertRaises(PlanError):
            validate(inp)

    def test_range_below_fence_rejected(self):
        # 围栏 = 1000 + 10*3 = 1030，分片起点 1020 仍在旧库可能发号区间内
        inp = make_input(shard_ranges=(ShardRange("a", 1020, 2990),))
        with self.assertRaises(PlanError):
            validate(inp)

    def test_validation_is_repeatable(self):
        inp = make_input(shard_ranges=(
            ShardRange("a", 2000, 2990),
            ShardRange("b", 2000, 2990),
        ))
        for _ in range(3):
            with self.assertRaises(PlanError):
                validate(inp)

    def test_plan_level_exhaustion_rejected(self):
        # 边界耗尽：分片只有 3 个号码容量，却要求预占 5 个
        inp = make_input(
            shard_ranges=(ShardRange("a", 2000, 2020),),
            reserved_ranges=(),
        )
        with self.assertRaises(PlanError):
            MigrationPlanner().plan(inp)


class RegistryExhaustionTest(unittest.TestCase):
    def test_boundary_exhaustion_exact_fit_then_error(self):
        reg = SegmentRegistry()
        reg.add_segment("a", 2000, 2020, 10)  # 恰好 3 个号码
        got = [reg.allocate("a", "req-%d" % i) for i in range(3)]
        self.assertEqual(got, [2000, 2010, 2020])
        with self.assertRaises(ExhaustedError):
            reg.allocate("a", "req-3")

    def test_reclaim_idempotent(self):
        reg = SegmentRegistry()
        reg.add_segment("a", 2000, 2040, 10)
        reg.allocate("a", "req-0")
        first = reg.reclaim("a")
        second = reg.reclaim("a")
        self.assertEqual(first, second)
        self.assertEqual(first, (2010, 2040))
        self.assertTrue(reg.is_quarantined(2030))
        with self.assertRaises(ExhaustedError):
            reg.allocate("a", "req-1")  # 回收后该分片不可再发号

    def test_add_segment_idempotent_but_conflict_rejected(self):
        reg = SegmentRegistry()
        reg.add_segment("a", 2000, 2040, 10)
        reg.add_segment("a", 2000, 2040, 10)  # 重复登记 no-op
        with self.assertRaises(PlanError):
            reg.add_segment("a", 2000, 2990, 10)


class RestartTest(unittest.TestCase):
    def test_restart_does_not_reissue_numbers(self):
        reg = SegmentRegistry()
        reg.add_segment("a", 2000, 2060, 10)
        before = [reg.allocate("a", "req-%d" % i) for i in range(3)]

        state = json.loads(json.dumps(reg.dump()))  # 模拟落盘 + 数据库重启
        reg2 = SegmentRegistry.restore(state)

        after = [reg2.allocate("a", "req-%d" % i) for i in range(3, 6)]
        self.assertEqual(after, [2030, 2040, 2050])
        self.assertFalse(set(before) & set(after))
        self.assertEqual(reg2.high_water_mark, 2050)

    def test_duplicate_request_survives_restart(self):
        reg = SegmentRegistry()
        reg.add_segment("a", 2000, 2060, 10)
        first = reg.allocate("a", "req-dup")
        reg2 = SegmentRegistry.restore(json.loads(json.dumps(reg.dump())))
        self.assertEqual(reg2.allocate("a", "req-dup"), first)  # 重启后仍幂等


class DuplicateRequestTest(unittest.TestCase):
    def test_same_request_id_returns_same_number(self):
        reg = SegmentRegistry()
        reg.add_segment("a", 2000, 2060, 10)
        n1 = reg.allocate("a", "req-x")
        n2 = reg.allocate("a", "req-x")
        n3 = reg.allocate("a", "req-y")
        self.assertEqual(n1, n2)
        self.assertNotEqual(n1, n3)

    def test_concurrent_allocations_are_unique(self):
        reg = SegmentRegistry()
        reg.add_segment("a", 2000, 2990, 10)  # 100 个号码

        def grab(i):
            return reg.allocate("a", "req-%d" % i)

        with ThreadPoolExecutor(max_workers=16) as pool:
            got = list(pool.map(grab, range(100)))
        self.assertEqual(len(set(got)), 100)
        self.assertEqual(min(got), 2000)
        self.assertEqual(max(got), 2990)


class RollbackTest(unittest.TestCase):
    def test_rollback_never_reissues_committed_numbers(self):
        reg = SegmentRegistry()
        reg.add_segment("a", 2000, 2060, 10)
        committed = [reg.allocate("a", "req-%d" % i) for i in range(3)]
        high_water = reg.rollback()
        self.assertEqual(high_water, max(committed))
        with self.assertRaises(RolledBackError):
            reg.allocate("a", "req-new")  # 回滚后禁止发号
        # 未发放的尾部进入隔离区，不会被再次发放
        for n in (2030, 2040, 2050, 2060):
            self.assertTrue(reg.is_quarantined(n))

    def test_rollback_is_idempotent(self):
        reg = SegmentRegistry()
        reg.add_segment("a", 2000, 2020, 10)
        reg.allocate("a", "req-0")
        first = reg.rollback()
        second = reg.rollback()
        self.assertEqual(first, second)

    def test_rollback_state_survives_restart(self):
        reg = SegmentRegistry()
        reg.add_segment("a", 2000, 2020, 10)
        reg.allocate("a", "req-0")
        reg.rollback()
        reg2 = SegmentRegistry.restore(json.loads(json.dumps(reg.dump())))
        with self.assertRaises(RolledBackError):
            reg2.allocate("a", "req-1")


if __name__ == "__main__":
    unittest.main()
