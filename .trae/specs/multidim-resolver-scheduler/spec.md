# 多维指标 Resolver 调度器（MDS）- 产品需求文档

## Overview

- **Summary**：在 dnscrypt-proxy 现有 `lb_strategy` 策略体系（`p2` / `ph` / `p<n>` / `first` / `random` / `wp2`）之上，新增一个**可插拔**的多维度调度策略 `mds`（Multi-Dimensional Scheduler）。它以每个 resolver 的多维运行时指标驱动选择：延迟分布（p50/p95/p99）、抖动、超时率、SERVFAIL 比例、DNSSEC 验证失败率、大响应截断率、QUIC/TCP 回退次数、连接建立成本、resolver 过滤属性与 ECS 特征。指标通过结构化日志、Prometheus 端点与监控 Dashboard 暴露；并附带确定性仿真 harness，对 `wp2` 与 `mds` 做 A/B 评估。
- **Purpose**：现有默认策略 WP2（[serversInfo.go](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/serversInfo.go#L484-L543)）仅用 RTT 均值 EWMA（70%）+ 全期成功率（30%）打分，存在四类缺陷：
  1. 对尾延迟不敏感（只有均值，无 p95/p99/抖动/质量信号）；
  2. Power-of-Two-Choices 每查询随机重抽两个候选，无粘性，两个近分节点间反复横跳，切换次数多；
  3. 单个坏样本即可让成功率从默认 100% 大幅跌落（冷启动 1 次失败 = 成功率 0%），短暂共享抖动会引起频繁震荡；
  4. 冷启动节点样本不足时与成熟节点同标尺竞争，容易被错误降权/淘汰。
- **Target Users**：配置多个上游 resolver、关注查询尾延迟与故障稳定性的 dnscrypt-proxy 运营者。

## Goals

- **G1**：新增 `mds` 策略，可通过 `lb_strategy = 'mds'` 插拔启用；默认策略仍为 `wp2`（用户已决策），旧策略全部保留可回退。
- **G2**：尾延迟降低——在存在「长尾节点」的仿真场景中，端到端 p95/p99 显著低于 `wp2`。
- **G3**：切换次数不显著增加——稳定场景与尾延迟场景下切换次数 ≤ `wp2`。
- **G4**：抗抖动——短暂网络抖动（所有节点在秒级窗口内同时变差后恢复）期间不发生频繁震荡。
- **G5**：冷启动保护——样本不足的 resolver 不会因少量失败被错误降权或排除出候选集。
- **G6**：多维指标运行时按 resolver 采集，并作为调度输入。
- **G7**：指标经三个渠道暴露：周期性结构化日志、Prometheus `/metrics`、监控 Dashboard Resolver 表格。
- **G8**：确定性仿真 harness，可重复地 A/B 对比 `wp2` 与 `mds`。
- **G9**：resolver 的过滤属性（DNSSEC / NoLog / NoFilter stamp 标记）与 ECS 特征仅作软偏好与展示，不硬淘汰任何节点。
- **G10**：真实持续故障（区别于短暂抖动）仍能快速熔断并切换到健康节点。

## Non-Goals

- 不修改、不删除任何现有策略；`wp2` 继续作为默认策略。
- 不做指标跨进程持久化：重启后 warmup 重新累积；初始延迟仍来自证书探测的 `initialRtt`。
- 不引入主动探测流量：调度完全基于真实查询的被动观测（warmup 置信来自证书探测 + 真实样本）。
- 不改变 DNS 解析正确性、缓存、过滤/转发插件的既有语义。
- 不新增外部告警系统或独立导出进程。

## Background & Context

- 选择入口：[ServersInfo.getOne()](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/serversInfo.go#L451-L481)，持有 `serversInfo.Lock()` 时完成选择。
- 现有反馈：`noticeBegin/noticeSuccess/noticeFailure` 更新 RTT EWMA；`updateServerStats(name, success)` 更新 total/failed 计数（[proxy.go L893-L897](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/proxy.go#L893-L897)）。
- 可观测信号位置：
  - 超时 / UDP→TCP 回退 / 上游 TC 标志：[processDNSCryptQuery](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/query_processing.go#L37-L106)；
  - SERVFAIL 与 DNSSEC bogus：[processPlugins L336-L345](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/query_processing.go#L336-L345)（`dnssec` 标记 + SERVFAIL 即现有「invalid DNSSEC signature」语义）；
  - 本地截断：[sendResponse](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/query_processing.go#L370-L384)；
  - QUIC(HTTP/3)→HTTP/2 回退发生在 [XTransport.Fetch L794-L818](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/xtransport.go#L794-L818)，当前不向调用方暴露；
  - DNSCrypt UDP 连接复用：[UDPConnPool.Get](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/udp_conn_pool.go#L94-L111)（池命中复用 vs 新建 DialUDP）；
  - stamp 属性常量：`ServerInformalPropertyDNSSEC/NoLog/NoFilter`，但当前 `ServerInfo` 未保留 stamp 属性；
  - ECS 出站注入：[plugin_ecs.go](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/plugin_ecs.go)；入站 ECS 选项需从响应报文中解析（非 0 Scope 表示 resolver 回传了 ECS）。
- 指标既有暴露面：`logWP2Stats`（5 分钟周期 debug 日志）、监控 UI 的 `resolverSnapshot` 与 Prometheus 文本生成（[monitoring_ui.go](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/monitoring_ui.go#L649-L714)）。
- 证书周期刷新会用新的 `ServerInfo` 指针替换旧对象（[refreshServer](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/serversInfo.go#L272-L312)），因此指标必须按 resolver 名字独立存续，不能内联在 `ServerInfo` 中。

## Functional Requirements

- **FR-1（策略插拔与配置）**：`mds` 与现有策略并列注册于 `configureLoadBalancing`；`lb_strategy` 缺省/空值仍解析为 `wp2`；未知值保留现有告警与回退行为。MDS 的调参（warmup 样本数、切换裕度、驻留时间、探索概率、评估窗口、各惩罚权重）提供带默认值的可选配置项，零配置即可用。
- **FR-2（多维指标采集）**：每个 resolver 维护一个有界指标对象，按 resolver 名字存储且在证书刷新后延续，至少包含：
  - 延迟分布：固定桶直方图，支持滑动窗口分位数查询（p50/p95/p99）；另维护延迟 EWMA 与抖动 EWMA（相邻样本差的指数加权）；
  - 比率类计数（在滑动窗口内）：超时、SERVFAIL、DNSSEC bogus、大响应截断（上游 TC 或本地截断分别计数并合计）、TCP 回退、QUIC 回退；
  - 连接建立：新建连接次数与复用次数（DNSCrypt UDP 池命中、TCP 直连、TLS 全量握手 vs 会话复用、QUIC 新建 dial），可得出新建占比；
  - ECS：响应携带非 0 Scope ECS 选项的观测次数/比率，以及全局 ECS 插件是否启用；
  - 静态特征：stamp 的 DNSSEC / NoLog / NoFilter 位；
  - 样本计数与首/末次采样时间（用于 warmup 置信判定与空闲重置）。
- **FR-3（统一结果反馈）**：一次上游交换结束后，以单一 outcome 记录接入：交换耗时、最终传输形态（dnscrypt-udp/tcp、doh-h2/h3、odoh）、是否超时、SERVFAIL、DNSSEC bogus、上游/本地截断、TCP 回退、QUIC 回退、连接是否复用、ECS 回传。该反馈同时驱动 MDS 指标与旧策略沿用的 RTT EWMA（旧策略行为不回归）。
- **FR-4（MDS 选择算法）**：
  - **粘性主节点 + 挑战者**：维护一个按名字记录的 primary；每次查询以高概率将「primary + 1 个随机挑战者」列入短名单，低概率纯随机探索两个候选；warmup 中的节点必须在探索槽位中保有保底出现机会。
  - **尾延迟感知打分**：以尾延迟估计（延迟 EWMA 与抖动/高分位的组合，或以窗口直方图高分位）为主项；候选之间采用**相对归一化**（比值/差值尺度），避免跨 RTT 量级失真。
  - **质量惩罚**：超时率、SERVFAIL 率、DNSSEC bogus 率、截断率、回退率、新建连接占比作为加性惩罚；所有比率必须在最小分母（有效样本数）之上才生效，否则该项中性。
  - **软偏好**：总分接近时对 NoFilter / DNSSEC 标记节点与 ECS 行为一致节点给予微小加分；任何情况下不得据此从候选集剔除节点。
  - **冷启动置信门**：真实样本数低于 warmup 阈值的节点处于 warming 状态：质量惩罚不适用、不参与淘汰性降权、延迟以证书探测 `initialRtt` 为先验、给予中性偏好分与保底探索份额。
  - **抗震荡滞后**：仅当挑战者相对 primary 的改善超过切换裕度（margin）且 primary 驻留时间已超过最小驻留窗口（dwell）时才切换；近 ties 不切。
  - **故障熔断**：primary 出现连续硬失败（如连续超时）达到阈值时，允许立即切换（不受 dwell/margin 限制）；被熔断节点进入半开恢复，周期性接收探索流量，成功后回到正常评分。
  - **切换计数**：全局与每节点记录 primary 切换次数，供观测与评估。
- **FR-5（传输层 trace）**：`XTransport.Fetch` 及其 DoH/ODoH 包装返回本次交换的 trace（是否发生 H3→H2 回退、TLS 是否会话复用/全量握手、协商协议）；DNSCrypt UDP 池 Get 暴露复用/新建；这些信号进入 FR-3 的 outcome。
- **FR-6（指标暴露）**：
  - 周期性结构化日志（沿用现有 5 分钟周期）逐 resolver 输出全部维度与策略名、primary、切换次数、warmup 状态；
  - Prometheus 文本中新增按 `server` 标签区分的延迟分位数、各比率/计数、抖动、回退、建连、切换次数与 warming 状态指标；
  - Dashboard 的 Resolver Health 表格与后端 JSON snapshot 增加上述关键列（至少 p95、超时率、SERVFAIL、截断、回退、抖动、特征标记）。
- **FR-7（仿真 A/B harness）**：在测试代码中提供确定性仿真器（可注入时钟与种子 RNG），合成 resolver 群体与延迟/失败分布，至少覆盖：稳定基线、长尾节点、短暂全网抖动、冷启动新节点（早期偶发失败）、持续故障切换五类场景；同一事件流分别驱动 `wp2` 与 `mds`，输出端到端 p50/p95/p99、切换次数、抖动窗口内切换数、冷启动节点选择份额等对比指标，并以断言形式固化需求阈值。

## Non-Functional Requirements

- **NFR-1（热路径开销）**：选择在已有 `serversInfo.Lock()` 内完成；每次选择仅对短名单（≤2 候选）做直方图分位查询，复杂度为 O(桶数)，内存按 resolver 数有界，不随 QPS 增长。
- **NFR-2（向后兼容）**：默认配置、旧策略选择结果、监控 UI 既有字段与 Prometheus 既有指标名称保持不变；新增字段不得破坏既有 JSON 消费者。
- **NFR-3（可测试性）**：指标与策略不依赖隐式全局状态；时钟、随机源、窗口滚动可在测试中注入或推进。
- **NFR-4（质量门禁）**：`go build ./...`、`go vet ./...`、`go test ./...` 全部通过；不引入 vendor 之外的新依赖。
- **NFR-5（并发安全）**：所有新增共享状态在现有锁模型（`serversInfo` RWMutex / 指标内部锁或原子量）下保证无数据竞争，`go test -race` 覆盖的测试通过。

## Constraints

- **技术**：Go 1.27，`package main`；仅使用现有 vendor 依赖（VividCortex/ewma、codeberg.org/miekg/dns、quic-go 等）。
- **配置**：遵循现有 TOML 扁平/分节风格与 `example-dnscrypt-proxy.toml` 文档惯例。
- **业务**：默认行为不变（仍为 wp2），用户显式 opt-in。

## Dependencies

- 现有 `LBStrategy` 注册点、`ServersInfo` 选择/反馈点、`XTransport.Fetch`、监控 UI/Prometheus 生成逻辑（均为仓内改造，无外部依赖）。

## Assumptions

- DNSSEC 验证失败沿用现有语义：查询置 `dnssec`（DO+AD 期望）且响应为 SERVFAIL 时计为 bogus；不在代理内新增本地 DNSSEC 验签逻辑。
- 「连接建立成本」以可观测代理量度量：H2 的 TLS `ConnectionState.DidResume`、自定义 QUIC Dial 的调用计数、DNSCrypt UDP 池命中率、DNSCrypt TCP 直连次数。
- 「大响应截断率」同时统计上游 UDP 响应 TC=1（即使随后 TCP 重试成功）与代理对客户端的本地截断。
- 仿真结果是算法回归与**相对比较**的确定性证据（合成分布），用于验收 G2–G5/G10；绝对数值改善仍以真实线上观测为最终参考。
- 各阈值默认值（warmup 样本数、margin、dwell、探索概率、熔断连续失败数等）在实现期由仿真标定，并通过配置暴露。

## Acceptance Criteria

### AC-1：mds 策略可插拔且默认不变
- **Type**：`rule`
- **Given**：配置文件分别设置 `lb_strategy = 'mds'`、`'wp2'`、缺省、非法值
- **When**：加载配置
- **Then**：'mds' 启用 MDS；'wp2' 与缺省启用 WP2；非法值告警并回退（沿用现有行为）
- **Pass Condition**：存在配置加载单测断言四种情况下 `serversInfo.lbStrategy` 的具体类型；缺省类型为 WP2
- **Evidence**：`go test` 中配置加载测试输出

### AC-2：多维指标采集数值正确
- **Type**：`rule`
- **Given**：一个全新的 per-resolver 指标对象
- **When**：注入一组已知耗时与结果类型的合成 outcome（含成功、超时、SERVFAIL、DNSSEC bogus、TC 截断、本地截断、TCP/QUIC 回退、新建/复用连接、ECS 回传）
- **Then**：直方图分位数（p50/p95/p99）、各窗口计数/比率、抖动 EWMA、新建占比与手算值一致；窗口滚动后过期样本被剔除
- **Pass Condition**：表驱动单测全部通过
- **Evidence**：指标采集单元测试结果

### AC-3：冷启动不发生样本不足误淘汰
- **Type**：`rule`
- **Given**：一个真实样本数 < warmup 阈值的 resolver，与若干成熟 resolver 并存，且新节点早期出现少量失败（如 20 样本内 1–2 次超时）
- **When**：MDS 在冷启动阶段持续做选择
- **Then**：warming 节点的质量惩罚项为中性、不会被排除；其选择份额不低于保底探索份额；达到 warmup 阈值后惩罚项才开始生效
- **Pass Condition**：冷启动单测 + 仿真冷启动场景断言选择份额 ≥ 保底值，且 warming 期评分不含惩罚
- **Evidence**：单测与仿真报告

### AC-4：短暂抖动期间不震荡
- **Type**：`rule`
- **Given**：仿真「短暂全网抖动」场景——所有节点在 ≤2 秒窗口内延迟同步飙升后恢复
- **When**：分别运行 wp2 与 mds
- **Then**：mds 在抖动窗口内 primary 切换次数 ≤ 1，抖动结束后 primary 恢复/保持稳定；窗口内切换数显著少于 wp2（≤ wp2 的 25%）
- **Pass Condition**：仿真断言通过
- **Evidence**：仿真 harness 输出（两种策略切换次数对比）

### AC-5：切换次数不显著增加
- **Type**：`rule`
- **Given**：稳定基线场景与长尾节点场景，相同事件流
- **When**：分别运行 wp2 与 mds
- **Then**：两场景下 mds 的累计切换次数均 ≤ wp2
- **Pass Condition**：仿真断言通过
- **Evidence**：仿真 harness 输出

### AC-6：尾延迟相对 wp2 降低
- **Type**：`rule`
- **Given**：长尾节点场景（部分节点延迟分布具明显 p99 长尾），相同事件流
- **When**：分别运行 wp2 与 mds
- **Then**：mds 端到端 p95 与 p99 均低于 wp2，其中 p99 降幅 ≥ 10%
- **Pass Condition**：仿真断言通过
- **Evidence**：仿真 harness 输出（p50/p95/p99 对比表）

### AC-7：持续故障可快速熔断切换
- **Type**：`rule`
- **Given**：仿真场景中 primary 从某时刻起持续超时
- **When**：mds 运行
- **Then**：连续硬失败达到熔断阈值后立即切换（不受 dwell/margin 限制）；被熔断节点经半开探索且成功后可恢复候选资格
- **Pass Condition**：仿真断言切换时延上限与半开恢复行为
- **Evidence**：仿真 harness 输出

### AC-8：过滤属性与 ECS 为软偏好且可观测，不硬淘汰
- **Type**：`rule`
- **Given**：候选集包含 NoFilter/DNSSEC 标记不同、ECS 回传行为不同的节点
- **When**：mds 打分选择
- **Then**：任何标记组合下所有节点始终留在候选集；标记仅在总分之差小于软偏好生效区间时影响并列次序；指标快照中可读取这些特征
- **Pass Condition**：单测断言候选集大小不变且同分时软偏好次序符合预期；快照含特征字段
- **Evidence**：单测结果

### AC-9：指标三渠道完整暴露
- **Type**：`rule`
- **Given**：注入合成指标后的运行态
- **When**：触发周期日志渲染、Prometheus 文本生成、Dashboard snapshot 序列化
- **Then**：三处均包含 FR-2 全部维度（p50/p95/p99、超时、SERVFAIL、DNSSEC bogus、截断、TCP/QUIC 回退、建连、抖动、特征、切换次数/warming 状态）；既有指标名与 JSON 字段不被移除
- **Pass Condition**：对日志字符串、Prometheus 文本、snapshot JSON 的断言单测通过；Dashboard 表格新增列存在于模板与渲染 JS
- **Evidence**：单测结果与 dashboard.html / monitoring.js 改动

### AC-10：工程质量门禁
- **Type**：`rule`
- **Given**：全部改动完成
- **When**：运行 `go build ./...`、`go vet ./...`、`go test ./...`（必要处 `-race`）
- **Then**：全部成功，无新增告警
- **Pass Condition**：命令退出码均为 0
- **Evidence**：命令输出

### AC-11：调度与指标设计质量
- **Type**：`rubric`
- **Dimension**：可插拔接口清晰度、并发模型正确性、对旧策略的非侵入性、热路径开销与内存有界性
- **Scale**：1-5
- **Anchors**：1 = 新逻辑与旧代码耦合混乱、引入全局可变状态或锁内重计算；3 = 功能正确但抽象一般、存在可接受的冗余；5 = 接口最小自洽、新旧策略零相互回归风险、锁与内存边界清晰且有注释说明
- **Pass Threshold**：>= 4
- **Evidence**：独立代码审查

### AC-12：仿真 harness 可信度
- **Type**：`rubric`
- **Dimension**：场景真实性覆盖、确定性（种子可复现）、断言与需求的对应程度、结果可读性
- **Scale**：1-5
- **Anchors**：1 = 仅 happy path 或结果不可复现；3 = 覆盖主要场景但部分阈值缺乏依据；5 = 五类场景齐全、双策略同流对比、阈值与 G2–G5/G10 一一对应并输出可读对比报告
- **Pass Threshold**：>= 4
- **Evidence**：独立代码审查仿真代码与一次实际运行输出

## Open Questions

- 无阻塞性开放问题。MDS 默认阈值（warmup≈20、margin≈15%、dwell≈5s、探索概率≈15%、熔断连续失败≈3、评估窗口≈5min）在实现期以仿真标定并固化为默认配置，用户可通过配置覆盖。
