# 多维指标 Resolver 调度器（MDS）— 独立审查报告与修复记录

## 审查方式

- 独立审查：由未参与实现的只读 subagent 对完整 diff 做一次全新审查（12 AC 逐项判定 + 并发/契约/语义核查）。
- 修复后验证：实现者逐条修复，新增针对性回归测试，并对关键回归测试做「回退即失败」实证；全仓 `-race` 回归。

## 发现与修复状态

| ID | 级别 | 问题 | 状态 |
|---|---|---|---|
| C-1 | Critical | `candidatesLocked()` 在 `RLock` 下懒写 `metrics` map；observability/monitoring 三处 scrape 并发时构成 `concurrent map writes`，可崩溃。原测试因预建条目恰好绕过，`-race` 全绿为假象 | **已修 + 回归测试实证** |
| M-1 | Major | 被插件 Drop 的响应（旧代码提前 return，不调 noticeSuccess）在统一反馈点污染 legacy RTT EWMA，造成 wp2 选择行为回归 | **已修 + 回归测试** |
| M-2 | Major | 滑窗计数以 `_total` + counter 类型暴露但数值会随窗口回落，违反 Prometheus counter 单调契约 | **已修 + 契约断言** |
| M-3 | Major | 冷启动边界（样本数到达 warmup 瞬间的硬失败连击延续）与恢复观察期语义不严：观察期内硬失败未立即中止 probation | **已修 + 测试更新** |
| M-4 | Minor | resolver 删除后 `circuits`/`switchesByName`/`metrics` 按名残留（按名有界；热重载重建策略时清空） | 记录为后续跟进 |
| M-5 | Minor | 热重载修改 `metrics_window` 不影响存量 resolver（store 创建时固化） | **已文档化**（example toml 注明生效范围） |
| M-6 | Minor | 热路径每候选一次 42 桶堆分配（与 NFR-1「短名单」字面偏差）；observeOutcome 每次额外 dns.Unpack | **已优化**：`PercentileBuf` 共享累加器，benchmark 降至 1 alloc/op；Unpack 保留（单次/查询，锁外） |
| M-7 | Minor | 完全平分时按 name 字典序决胜，等价节点长期负载不均 | 记录为后续跟进（建议随机/轮转 tie-break） |
| M-8 | Major | 半开探测成功后的恢复观察期：首个硬失败应立即中止 probation 重新武装断路器 | **已修**（与 M-3 同批改） |
| M-9 | Minor | 全局 ECS 插件启用态三渠道未暴露；MDS 日志行无 conn_reused 绝对计数；dashboard Features 无 NoLog 徽标 | **部分已修**：NoLog 徽标已加（JSON `feature_nolog` 早已提供）；其余记录跟进 |
| M-10 | Minor | 成功 RTT 锚点 `exchangeStart` 在 Encrypt 之前（旧 lastActionTS 在其后），差异仅本地加密微秒级 | 记录为后续跟进（微秒级，不影响调度） |

## 修复细节

### C-1：RLock 下懒写 map（Critical）

- [serversInfo.go](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/serversInfo.go)：`candidatesLocked()` 改为只读路径——缺键时构造一次性 throwaway store 返回，**绝不写 map**；map 写只允许在持有写锁的 `metricsForLocked()` 发生。
- `refreshServer()` 在同一写锁临界区内（inner append/replace 之后）调 `metricsForLocked(name)` 预发布条目，维持「inner 中可见的名字必有注册 store」不变量。
- 回归测试：`TestCandidatesLockedConcurrentScrape`（4 scrape goroutine × 200 轮 RLock + 1 writer goroutine × 100 次写锁发布）。**实证**：临时把只读路径回退成懒写版本，`go test -race` 立即报 `WARNING: DATA RACE` 并 FAIL；恢复修复版后通过。
- 同步更新 `TestMetricsRegistrySurvivesServerRefresh`：第二节点显式经 `metricsForLocked` 预发布并断言候选持有的就是发布实例（镜像生产不变量）。

### M-1：Drop 响应不写 legacy EWMA

- [scheduler_feedback.go](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/scheduler_feedback.go)：legacy EWMA switch 增加 `case o.DNSSECBogus || ps.action == PluginsActionDrop` → 不更新 RTT（与旧 noticeSuccess 语义一致）；total/failed 统计与新指标窗仍如实记录。
- 回归测试：`TestObserveOutcomeDropSkipsLegacyRTT`（800ms Drop 响应后 rtt EWMA 数值不变、totalQueries=1/failedQueries=0、新指标窗 Samples=1/Success=1）。

### M-2：Prometheus 命名/类型契约

- [scheduler_observability.go](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/scheduler_observability.go)：10 个滑窗族改名为 `dnscrypt_proxy_resolver_{timeout,errors,servfail,dnssec_bogus,truncated,tcp_fallback,quic_fallback,conn_new,conn_reused,ecs_returned}_window`，TYPE 为 gauge（回落合法）；`dnscrypt_proxy_scheduler_switch_total` TYPE 由 gauge 改为 counter（保留单调的 `dnscrypt_proxy_resolver_switch_total`）。
- JSON 字段名（`tcp_fallback_total` 等）**未改**，monitoring.js 读 JSON 不受影响（NFR-2）。
- [scheduler_observability_test.go](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/scheduler_observability_test.go)：名称断言同步，新增 TYPE gauge/counter 契约断言。

### M-3 / M-8：冷启动边界与恢复观察期

- [scheduler_mds.go](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/scheduler_mds.go) `observeFeedbackLocked` 硬失败分支：
  1. warming 内（`totalSamples < WarmupSamples`，或 circuit 处于 recovery probation）硬失败**不累加** `consecHardFails`（保持 0），消除「样本数到阈值那一拍的连击延续」冷启动边界。
  2. 恢复观察期内首个硬失败立即中止 probation（`recoverUntilTotal=0, consecHardFails=1, return`），之后按成熟节点正常计数。
  3. 半开探测失败路径仍 ++consecHardFails 并再等一个 HalfOpenInterval。
- `TestMDSRecoveryProbation` 尾部断言更新为新语义：观察期第 1 次硬失败中止 probation 但不熔断，再 2 次硬失败（达 BreakerThreshold=3）才熔断。

### M-6：热路径分配

- `ResolverMetrics.PercentileBuf(p, buf)`：调用方提供 42 桶累加器，容量不足时才分配并返回新切片；`scoreAll` 全候选复用一个缓冲。
- 实测：`BenchmarkSchedulerMDSSelect`（7 候选）**1 alloc/op、467 B/op、726 ns/op**（M4），此前每候选 1 次桶分配。

### M-9（部分）：NoLog 徽标

- [monitoring.js](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/static/js/monitoring.js) Features 列在 DNSSEC 与 NoFilter 之间增加 NoLog 徽标（后端 JSON `feature_nolog` 早已提供）。

## AC 判定（修复后终判）

| AC | 判定 | 依据 |
|---|---|---|
| AC-1 策略可插拔默认不变 | **PASS** | TestApplyMDSConfigValidation/TestMDSConfigTOMLDecode 覆盖 mds/wp2/缺省/非法四态 |
| AC-2 多维采集数值正确 | **PASS** | ResolverMetrics 表驱动单测（分位/计数/比率/抖动/老化/有界内存/race）；observeOutcome 10 维表 |
| AC-3 冷启动不误淘汰 | **PASS** | TestMDSWarmupProtection（惩罚中性）+ ColdStart 仿真（warming 份额 0.855，2 次早期硬失败不熔断）；M-3 修复后边界连击也消除 |
| AC-4 短暂抖动不震荡 | **PASS（贴线）** | NetworkJitter 仿真 mds primaryMoves=1，wp2 churn=15（≤1 且 ≤25%）；余量为零，靠 dwell=5s + margin=15% 保证 |
| AC-5 切换不显著增加 | **PASS** | LongTail primarySwitches=5 vs wp2 churn≈3187；Stable 11 vs ≈1876 |
| AC-6 尾延迟降低 | **PASS** | p95 15.2<16.8ms；p99 40ms vs 200ms（**降 80%**，远超 ≥10%）；p50 持平（10.95 vs 10.99ms） |
| AC-7 持续故障熔断+半开恢复 | **PASS** | CircuitBreaker 仿真 failover=1 拍（100ms）、recovery=63 拍（6.3s）、结束闭合；M-8 修复后 probation 内复发即时重新熔断 |
| AC-8 属性/ECS 软偏好不硬淘汰 | **PASS** | TestMDSSoftPreferences：候选集大小恒定，软偏好仅 tie band 内改次序；快照含特征字段 |
| AC-9 三渠道暴露 | **PASS** | 日志/Prom/JSON+JS 断言测试全绿；M-2 修正 Prom 契约、M-9 补 NoLog 徽标。残留：全局 ECS 插件启用态、日志行 conn_reused 绝对计数（Minor 跟进） |
| AC-10 质量门禁 | **PASS** | `go vet .`、`go test ./...`、`go test -race ./...`、`gofmt -l .` 全绿（见下） |
| AC-11 设计质量 rubric | **PASS（≥4）** | C-1 修复后并发模型自洽：map 写只在写锁、只读路径不分配共享状态；新旧策略零相互回归（M-1 修复 Drop 回归后）；热路径 1 alloc/op。残余 M-4/M-7 不影响阈值 |
| AC-12 仿真可信度 rubric | **PASS（5/5）** | 五场景齐全、双策略同流、种子跨进程可复现、阈值与 G2–G5/G10 一一对应；harness 真实抓出 recovery probation 产品缺陷 |

## 最终验证（修复后）

```
$ GOTOOLCHAIN=auto go vet .                         # exit 0
$ GOTOOLCHAIN=auto go test ./...                    # ok（仓库全包）
$ GOTOOLCHAIN=auto go test -race ./...              # ok（全仓无数据竞争）
$ GOTOOLCHAIN=auto go test -count=2 .               # 连跑两轮全绿
$ gofmt -l .                                        # 无输出
```

仿真复跑（同种子可复现）：

```
long-tail (4000): wp2 p50=10.99ms p95=16.81ms p99=200ms churn=3187
                  mds p50=10.95ms p95=15.17ms p99=40ms primarySwitches=5
stable:           wp2 churn=1876 / mds primarySwitches=11
jitter window:    wp2 churn=15 / mds primaryMoves=1
cold start:       warming share=0.855（171/200），全程不熔断
breaker:          failover=1 步(100ms)，recovery=63 步(6.3s)，结束闭合
BenchmarkSchedulerMDSSelect-10   726 ns/op   467 B/op   1 allocs/op
```

## 后续跟进（不阻塞验收）

1. **M-4**：resolver 注销时清理 `mds.circuits`/`mds.switchesByName`/`serversInfo.metrics`（当前按名有界，热重载重建策略即清空）。
2. **M-7**：tie band 内完全平分时的 name 字典序决胜改为随机/轮转，消除等价节点的长期负载偏置。
3. **M-9 残留**：三渠道暴露全局 ECS 插件启用态；MDS 周期日志行补 `conn_reused` 绝对计数。
4. **M-10**：如需与旧 RTT 锚点完全对齐，可把 `exchangeStart` 移至 Encrypt 之后（差异仅本地加密微秒级）。
5. **AC-4 贴线**：抖动预算余量为零，未来调参时应以 NetworkJitter 仿真为回归门禁。
