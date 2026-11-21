# database-sequence-migration-planner
规划跨分片序列迁移、范围分配和冲突回滚

## 概述

单库迁移到分片后，自增序列必须拆成互不重叠的号码段。本工具接收当前序列值、
步长、分片范围、预分配大小、并发申请数和保留区间，输出确定性的迁移步骤：

- `PREALLOCATE` 预占：为每个分片划出预分配号段
- `SWITCH` 切换：按 `当前值 + 步长 × 在途申请数` 设置安全围栏，围栏以下
  仍归旧库发号，围栏以上归各分片
- `BACKFILL` 补号：把分片范围与保留区间之外的空洞回收至补号池
- `ROLLBACK` 回滚：未发放号码进入隔离区，已提交号码（≤ 高水位）永久封存，
  绝不重新发放

**重要：本工具只做纯内存的规划与模拟，不连接、不读取、不修改任何真实
数据库或序列。** 输出的步骤需由运维/迁移系统自行审核后执行。

## 用法

```python
from planner import MigrationPlanner, PlannerInput, ShardRange

plan = MigrationPlanner().plan(PlannerInput(
    current_value=1000,          # 旧库下一个待发放号码
    step=10,                     # 序列步长
    shard_ranges=(ShardRange("shard-a", 2000, 2990),
                  ShardRange("shard-b", 4000, 4990)),
    prealloc_size=5,             # 每分片预占 5 个号码
    concurrent_requests=3,       # 切换期间旧库仍在途的申请数
    reserved_ranges=((3000, 3090),),
))
for step in plan.steps:
    print(step.kind, step.shard_id, step.start, step.end, step.detail)
```

`SegmentRegistry` 提供号段发放的运行时模拟：`allocate` 按 `request_id`
幂等（重复申请返回同一号码），`reclaim` / `rollback` 可安全重复执行，
`dump()` / `restore()` 支持模拟数据库重启后的状态恢复。

## 测试

```bash
python -m unittest
```

覆盖边界耗尽、数据库重启（dump/restore）、重复申请、并发申请、范围重叠、
保留区间冲突、回滚不重发已提交号码等用例。兼容 Python 3.8.2+。
