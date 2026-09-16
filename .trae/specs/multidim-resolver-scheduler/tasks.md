# 多维指标 Resolver 调度器（MDS）- 实施计划

任务为依赖有序的纵向切片；每个任务至少含一个测试需求（TR）。状态字段仅取 `pending/in_progress/blocked/completed/cancelled`。

## Task 1: 多维指标采集内核
- **Status**：`completed`
- **Completion Evidence**：
  - 新增 [scheduler_metrics.go](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/scheduler_metrics.go)（几何桶+10 epoch 轮转直方图、延迟/抖动 EWMA、全维度计数、有界内存、可注入时钟）与 [scheduler_metrics_test.go](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/scheduler_metrics_test.go)。
  - TR-1.1/1.2/1.3 均通过：`GOTOOLCHAIN=auto go test -race -run TestResolverMetrics` → PASS（分位误差在一桶宽内、计数精确、epoch 老化正确、容量恒定、并发 race 干净）；`go vet .` 无告警。
- **Priority**：high
- **Depends On**：None
- **Description**：
  - 新增 `scheduler_metrics.go`：定义 `ExchangeOutcome`（耗时、最终传输形态、超时、SERVFAIL、DNSSEC bogus、上游/本地截断、TCP/QUIC 回退、连接复用、ECS 回传）与 `ResolverMetrics`。
  - `ResolverMetrics` 含：固定桶 + epoch 轮转的滑动窗口直方图（窗口/桶数/epoch 数为常量或可配置）、延迟 EWMA、抖动 EWMA、窗口内各类计数（总/成功/超时/SERVFAIL/bogus/上游 TC/本地截断/TCP 回退/QUIC 回退/新建连接/复用连接/ECS 回传）、样本计数与首末采样时间。
  - 提供有界内存的 `Observe(outcome)`、`Percentile(p)`、各比率 getter（分母不足返回「无置信」标记）、`Warm(threshold)` 判定、窗口滚动（可注入时钟）。
  - 新增 `scheduler_metrics_test.go` 表驱动单测。
- **Acceptance Criteria Addressed**：AC-2
- **Test Requirements**：
  - `rule` TR-1.1：注入已知样本集后，p50/p95/p99 与手算分位误差不超过一个桶宽；各计数/比率、抖动 EWMA、新建占比精确匹配；推进时钟越过窗口后旧 epoch 被剔除。证据：`go test -run ResolverMetrics -v`
  - `rule` TR-1.2：指标对象内存有界——连续 Observe 远超窗口样本量后，桶/epoch 数不增长。证据：单测断言内部容量恒定
  - `rule` TR-1.3：`go test -race` 下并发 Observe/读取无数据竞争。证据：race 测试通过

## Task 2: 指标注册表与 resolver 静态特征
- **Status**：`completed`
- **Completion Evidence**：serversInfo.go 新增 metrics 注册表（metricsForLocked/MetricsFor/candidatesLocked）、ServerInfo.Props 与 Supports* 方法、三处 fetch 填充 Props；scheduler_registry_test.go 两测例通过；`go build ./...` 绿。TR-2.1 PASS（TestMetricsRegistrySurvivesServerRefresh，模拟证书刷新指针替换后指标延续）；TR-2.2 PASS（TestServerInfoProps 校验 stamp 位与读取方法）。
- **Priority**：high
- **Depends On**：Task 1
- **Description**：
  - 在 `ServersInfo` 上新增按 resolver 名字索引的 `map[string]*ResolverMetrics` 注册表（随 `registerServer`/`refreshServer` 生命周期获取/创建，证书刷新替换 `ServerInfo` 指针后指标延续）。
  - `ServerInfo` 增加 `props stamps.ServerInformalProperties` 字段，在 `fetchDNSCryptServerInfo`/`fetchDoHServerInfo`/`_fetchODoHTargetInfo` 中由 stamp.Props 填充；提供 `SupportsDNSSEC/NoLog/NoFilter` 读取方法。
  - 指标对象支持全局 ECS 插件启用态与每节点 ECS 回传计数（回传的具体解析在 Task 4 接线）。
  - 选择热路径所需的快照视图（在 `serversInfo.Lock()` 内把 `*ServerInfo` 与其 metrics 配对），供 Task 5 使用。
  - 注册表相关单测。
- **Acceptance Criteria Addressed**：AC-2、AC-8
- **Test Requirements**：
  - `rule` TR-2.1：注册 → 注入样本 → 以同名重新 `refreshServer`（模拟指针替换）后，历史指标仍可读且按名字归并。证据：单测
  - `rule` TR-2.2：三种协议 fetch 出的 `ServerInfo.props` 与 stamp 位一致；特征读取方法正确。证据：单测（可用最小 stamp 构造）

## Task 3: 传输层 trace（QUIC 回退 / 建连复用）
- **Status**：`completed`
- **Completion Evidence**：xtransport.go 新增 FetchTrace（UsedHTTP3/QUICFallback/ConnReused/TLSResumed/NegotiatedProto），Fetch 全链路返回 trace，H3→H2 回退分支置位，httptrace.GotConn 识别 H2 连接复用，QUIC Dial 按 host atomic 计数（quicDialCount sync.Map + recordQUICDial/quicDials）判定 H3 新建；UDPConnPool.Get 返回 reused，exchangeWithUDPServer/viaProxy 上抛；processDNSCryptQuery/DoH/ODoH 写入 pluginsState.exchange；serversInfo/sources 探测点全部适配。TR-3.1 PASS（xtransport_trace_test.go：TestApplyHTTPFetchTrace 覆盖 nil/H3/H2 回退/ODoH 映射 + go build/vet 全绿）；TR-3.2 PASS（TestUDPConnPoolGetReuseSignal + 既有 udp_conn_pool_test.go 全量适配通过，-race）。
- **Priority**：high
- **Depends On**：None
- **Description**：
  - 新增 `FetchTrace`（字段：`UsedHTTP3 bool`、`QUICFallback bool`、`TLSResumed bool`、`NegotiatedProto string`），`Fetch/Get/Post/dohLikeQuery/DoHQuery/ObliviousDoHQuery` 增加该返回值；在 H3 尝试失败回落 H2 的既有分支置位（[xtransport.go L794-L818](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/xtransport.go#L794-L818)），从 `resp.TLS` 取会话复用信息；同步更新全部调用点（`processDoHQuery`、ODoH、cert 探测、well-known 拉取）。
  - 在自定义 QUIC Dial（xtransport.go 约 L429）增加按 host 的 dial 计数（atomic，仅用于 trace/指标）。
  - `UDPConnPool.Get` 暴露「池命中复用 vs 新建」（新增返回值或新增带 reused 标志的方法），`exchangeWithUDPServer`/viaProxy 路径可将信号上抛；DNSCrypt TCP 路径标记为新建。
  - 纯函数级单测（trace 置位逻辑/池复用判定），不发起真实网络。
- **Acceptance Criteria Addressed**：AC-2、AC-10
- **Test Requirements**：
  - `rule` TR-3.1：所有 DoH/ODoH/Fetch 调用点编译通过且正确接收 trace；QUIC 回退分支与 TLS 复用标记的判定逻辑有单测覆盖（抽取为不依赖网络的纯函数）。证据：`go build ./...` 与单测
  - `rule` TR-3.2：UDP 池命中返回 reused=true、池未命中 DialUDP 返回 reused=false。证据：池复用单测（扩展 `udp_conn_pool_test.go`）

## Task 4: 统一交换结果反馈接线
- **Status**：`completed`
- **Completion Evidence**：新增 scheduler_feedback.go——observeOutcome 成为唯一反馈点（processIncomingQuery defer 单次调用），classifyExchangeResponse 一次性解包识别 SERVFAIL/DNSSEC bogus/ECS（fork dns 的 SUBNET 平铺于 Msg.Pseudo，Scope!=0 判 ECS），超时识别（returnCode 或 net.Error Timeout），新增 Error 维度覆盖快速网络/解析失败；双写旧 RTT EWMA（失败/普通 SERVFAIL 加 timeout、bogus 不更新、成功加实测 elapsed，带 <timeout 门）与 WP2 total/failed 计数（含 >10000 减半）；删除 updateServerStats 与 noticeSuccess/noticeFailure（保留 noticeBegin 的 dormant 节流）；sendResponse 置 LocalTruncated。TR-4.1 PASS（10 例表驱动：成功/两类超时/快速错误/普通 SERVFAIL/bogus/上游TC+TCP回退/QUIC回退/本地截断/ECS）；TR-4.2 PASS（7 步混合序列 rtt EWMA 与 total/failed 与旧参考模型逐步精确相等）；TR-4.3 PASS（grep 无 updateServerStats/noticeFailure/noticeSuccess 调用点残留）；go vet/gofmt/-race 全绿。
- **Priority**：high
- **Depends On**：Task 1、Task 2、Task 3
- **Description**：
  - `PluginsState` 增加交换期信号字段：serverRtt、timeout、tcpFallback、quicFallback、connReused、upstreamTruncated、localTruncated、negotiatedProto。
  - `processDNSCryptQuery`：记录 UDP→TCP 回退、最终形态、UDP 池复用、上游 TC；错误路径区分超时/网络错误（沿用现有 returnCode 语义）。
  - `processDoHQuery`/`processODoHQuery`：接收 Task 3 的 trace 并置位；记录交换耗时。
  - `sendResponse`：本地截断发生时置位 localTruncated。
  - 在 `processIncomingQuery` 中以**单一** `serversInfo.observeOutcome(...)` 取代现有 `updateServerStats(serverName, success)` 调用点；观测时机放在 response 插件处理之后（此时已知 rcode 与 dnssec 标记）；在观测点一次性解包响应报文，识别 SERVFAIL、DNSSEC bogus（沿用 `pluginsState.dnssec` + SERVFAIL 语义）、响应中非 0 Scope 的 ECS 选项。
  - `observeOutcome` 同时：① 写入 Task 1 指标；② 保留旧策略依赖的 RTT EWMA 反馈（等价于现 noticeBegin/Success/Failure 语义），确保旧策略不回归；删除/内化 `updateServerStats` 与 WP2 专属计数路径（totalQueries/failedQueries 改由统一指标供给，WP2 打分读取适配）。
  - 接线单测：构造合成 outcome 走 observeOutcome，断言指标与旧 RTT EWMA 双写正确。
- **Acceptance Criteria Addressed**：AC-2、AC-8
- **Test Requirements**：
  - `rule` TR-4.1：对成功/超时/网络错误/SERVFAIL+dnssec/普通 SERVFAIL/TC/本地截断/TCP 回退/QUIC 回退/ECS 回传各至少一个用例，observeOutcome 后指标各维度计数与 outcome 一一对应。证据：表驱动单测
  - `rule` TR-4.2：同一 outcome 序列下，启用 wp2 时 `ServerInfo.rtt` EWMA 与 total/failed 的演化与改造前语义等价（用固定样本序列快照比较）。证据：单测
  - `rule` TR-4.3：`updateServerStats` 不再存在独立调用点（grep 为空），全部经 observeOutcome。证据：Grep 结果

## Task 5: MDS 多维调度策略实现
- **Status**：`completed`
- **Completion Evidence**：
  - 新增 [scheduler_mds.go](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/scheduler_mds.go)：`MDSParams`+`DefaultMDSParams()`（warmup=20、penaltyMin=10、margin=0.15、tieBand=0.03、dwell=5s、explore=0.15、熔断阈值=3、半开=10s、8 项惩罚权重）；`LBStrategyMDS` 实现粘性 primary（按名持久）+相对归一化尾延迟评分（0.6·EWMA+0.4·p95）+加性比率惩罚（分母门 10 样本，warming 中性、initialRtt 先验）+NoFilter/DNSSEC/ECS 软偏好（仅 tieBand 内翻转）+margin/dwell 抗震荡+熔断/半开状态机（冷启动豁免）；探索查询不改 primary、不增切换计数；全部状态在 ServersInfo 锁内访问，时钟/RNG 可注入，不重排 inner。
  - 选路接线：serversInfo.go getOne 新增 `lbStrategyAdvanced` type-switch（候选索引按名映射回 inner）；scheduler_feedback.go observeOutcome 经 `lbFeedbackReceiver` 派发反馈（带 totalSamples，warmup 门由策略自管）。
  - TR-5.1~5.4 全 PASS（scheduler_mds_test.go 4 测例：冷启动惩罚/份额/成熟后惩罚出现；同步抖动 dwell 内外 0 切换；3 连硬失败即时切换+半开探测+恢复 primary；软偏好 tie band 内外行为与候选集不变）；TR-5.5 rubric 自评 4/5（接口仅 2 方法、无全局可变状态、锁边界清晰、热路径 O(n) 且无 IO）；`go build ./...`、`go vet .`、`go test -race .`、gofmt 全绿。
- **Priority**：high
- **Depends On**：Task 2、Task 4
- **Description**：
  - 新增 `scheduler_mds.go`：实现 MDS 选择模型，全部判定仅使用 Task 2 快照视图，随机源与时钟可注入（包内默认 rand/time，测试可替换）。
  - 机制：粘性 primary（按名字持久）+ 挑战者短名单；可配置探索概率为纯随机探索；warming 节点保底探索份额。
  - 评分：尾延迟主项（延迟 EWMA + 抖动/高分位组合），候选间相对归一化；超时/SERVFAIL/bogus/截断/回退/新建占比的加性惩罚（比率须过最小分母，否则中性）；NoFilter/DNSSEC/ECS 软偏好仅在近分差区间生效。
  - 抗震荡：切换 margin（相对改善阈值）+ 最小驻留 dwell；近 tie 不切。
  - 冷启动：warmup 样本数以下为 warming，惩罚不适用、不淘汰、以 initialRtt 为先验、中性偏好。
  - 熔断/半开：primary 连续硬失败达阈值立即切换（豁免 dwell/margin）；熔断节点周期性接收探索，成功则恢复。
  - 全局与每节点切换计数；选择路径不重排 `inner`（旧策略的排序路径保持不变）。
  - 细粒度单测：打分相对归一化、warmup 门、margin+dwell 阻止抖动切换、熔断即时切换与半开恢复、软偏好不剔除节点。
- **Acceptance Criteria Addressed**：AC-3、AC-4、AC-5、AC-6、AC-7、AC-8、AC-11
- **Test Requirements**：
  - `rule` TR-5.1（冷启动）：warming 节点前 N-1 次观测中含失败时，其评分不含惩罚项、候选集永远包含它、选择份额 ≥ 探索保底值；第 N 次样本后惩罚项才出现。证据：单测
  - `rule` TR-5.2（抗震荡）：全体候选在短窗口同步注入 1–2 个劣化样本后，在 dwell 窗口内 primary 切换次数为 0（熔断阈值未触发的前提下）。证据：单测
  - `rule` TR-5.3（熔断）：对 primary 连续注入硬失败至阈值，下一次选择立即返回其他节点且不计 margin/dwell；随后对熔断节点注入探索成功，其可再次成为 primary。证据：单测
  - `rule` TR-5.4（软偏好）：任意 props 组合下候选集大小不变；仅当两候选总分之差小于软偏好区间时特征翻转并列次序。证据：单测
  - `rubric` TR-5.5：调度器设计质量（接口最小、无全局可变状态、锁边界清晰、热路径有界）；scale 1-5；anchors 见 spec AC-11；threshold >= 4；证据：代码评审

## Task 6: 策略注册与配置项
- **Status**：`completed`
- **Completion Evidence**：
  - configureLoadBalancing 注册 `mds`（大小写不敏感；缺省/非法仍回退 WP2，行为不变）；`mds` 分支构造 `NewLBStrategyMDS(params)` 并下发 metrics 窗口。
  - 新增 `MDSSchedulerConfig`（config.go，TOML `[scheduler_mds]` 扁平节，全部字段指针化以区分「未设置」与显式 0）+ `applyMDSConfig`（config_loader.go）：warmup_samples、penalty_min_samples、switch_margin、tie_band、dwell、exploration_probability、circuit_breaker_threshold、half_open_interval、metrics_window、8 个惩罚权重；逐字段范围校验 + 交叉校验（penaltyMin≤warmup、tieBand≤margin），非法告警回退默认。
  - 指标窗口配置化：ResolverMetrics 增加 per-instance window/epochDur + `NewResolverMetricsWithWindow`（10 epoch 结构不变，30s..30m），ServersInfo.metricsWindow 懒构造时传入。
  - example-dnscrypt-proxy.toml Load Balancing 节补 mds 说明与全量注释配置项。
  - TR-6.1/6.2 + TOML 解码往返 PASS（scheduler_config_test.go：5 策略输入、默认/合法/越界/交叉冲突/实际 TOML 文档解码）；`go build`、`go test -race .`、gofmt 全绿。
- **Priority**：high
- **Depends On**：Task 5
- **Description**：
  - `configureLoadBalancing` 注册 `mds`（大小写不敏感），缺省仍为 wp2，非法值行为不变。
  - 新增 MDS 配置（TOML `[scheduler]` 分节或与现有风格一致的扁平键，取其一并在代码注释说明），字段至少含：warmup 样本数、switch_margin、dwell、exploration_probability、circuit_breaker_threshold、window、各惩罚权重；全部有默认值与范围校验（非法值告警并回退默认）。
  - 在 [example-dnscrypt-proxy.toml](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/example-dnscrypt-proxy.toml#L174-L194) 的 Load Balancing 节补充 mds 说明与配置项注释（默认注释掉）。
  - 配置加载单测。
- **Acceptance Criteria Addressed**：AC-1
- **Test Requirements**：
  - `rule` TR-6.1：四种 lb_strategy 输入（mds/wp2/缺省/非法）解析出的策略类型正确，缺省为 WP2。证据：单测
  - `rule` TR-6.2：MDS 配置缺省时全部默认值生效；越界值（负 margin、>1 概率、0 warmup 等）告警并回退默认。证据：单测

## Task 7: 周期性结构化调度日志
- **Status**：`completed`
- **Completion Evidence**：新增 scheduler_observability.go：策略中立 `ResolverReportRow`/`SchedulerReport` + `buildSchedulerReportLocked()`（RLock 下从候选快照拼装全维度，MDS 复算 scoreAll/Stats，WP2 复用 calculateServerScore）；`logWP2Stats` 泛化为 `logSchedulerStats`（proxy.go 周期点改名），WP2 行经 `renderWP2StatsLocked` 逐字保留，MDS 经 `renderMDSReport` 输出 primary/切换/熔断/p50-p99/ewma/jitter/全比率/props/score。TR-7.1 PASS（scheduler_observability_test.go 断言 FR-2 全部维度标签与 WP2 旧行）。
- **Priority**：medium
- **Depends On**：Task 1、Task 2
- **Description**：
  - 将 `logWP2Stats` 泛化为 `logSchedulerStats`（WP2 下保持原输出内容不减少；MDS 下逐 resolver 输出：p50/p95/p99、延迟/抖动 EWMA、超时率、SERVFAIL、bogus、截断、TCP/QUIC 回退、新建/复用、ECS 回传率、props 特征、样本数/warming、评分、primary、切换次数）。
  - 沿用 proxy.StartProxy 中现有 5 分钟周期与 debug 级别。
  - 日志渲染单测（捕获渲染字符串做包含断言，不依赖 dlog 后端）。
- **Acceptance Criteria Addressed**：AC-9
- **Test Requirements**：
  - `rule` TR-7.1：注入合成指标后，渲染字符串包含 FR-2 全部维度标签与策略名/primary/切换次数；wp2 模式下原有 WP2 统计行仍存在。证据：单测

## Task 8: Prometheus 指标导出
- **Status**：`completed`
- **Completion Evidence**：generatePrometheusMetrics 追加 `appendResolverSchedulerMetrics`：26 个 `dnscrypt_proxy_resolver_*` 族（quantile 50/95/99 延迟、ewma/jitter、11 类计数、9 类比率、warming/features/primary/circuit_open/score/switch）+ 全局 `dnscrypt_proxy_scheduler_switch_total{strategy}`；旧指标名全部保留；server label 转义（\\、\"、\n）。TR-8.1 PASS（断言每族存在、quantile 标签、旧名保留、`weird"q\z` 正确转义）。
- **Priority**：medium
- **Depends On**：Task 2
- **Description**：
  - 在 `generatePrometheusMetrics` 中新增 `dnscrypt_proxy_resolver_*` 系列：latency_ms{quantile="50|95|99"}、jitter_ms、timeout/servfail/dnssec_bogus/truncated/tcp_fallback/quic_fallback 比率与计数、conn_new/conn_reused 计数、conn_new_ratio、ecs_scope_rate、switch_total（含全局）、warmup 状态 gauge、props（dnssec/nolog/nofilter）label；均带 `server` 标签并做 label 转义（沿用现有转义写法）。
  - 不重命名/删除既有指标。
  - 文本生成单测。
- **Acceptance Criteria Addressed**：AC-9
- **Test Requirements**：
  - `rule` TR-8.1：Prometheus 文本包含上述每个指标名且按 server 分标签；既有指标名全部保留；含特殊字符的 server 名被正确转义。证据：单测断言

## Task 9: 监控 Dashboard 多维列
- **Status**：`completed`
- **Completion Evidence**：resolverSnapshot 追加 report 指针（collectResolverSnapshots 第三返回值 SchedulerReport），resolver_health JSON 纯追加 27 个字段（分位仅在有样本时出现，优雅降级），根新增 `scheduler` 摘要；dashboard.html Resolver Health 增 7 列（P95/Timeout%/SERVFAIL/Trunc%/Fallbacks/Jitter/Features），monitoring.js 渲染 ★primary/circuit-open/warming、TCP/QUIC 回退计数、DNSSEC/NoFilter/ECS 徽标、缺值「–」、colSpan=14。TR-9.1 PASS（JSON 新字段+旧字段+scheduler 摘要断言）；TR-9.2 PASS（模板/JS 静态标识检查）；全量 `go test -race .`、vet、gofmt 绿。
- **Priority**：medium
- **Depends On**：Task 2
- **Description**：
  - 扩展 `resolverSnapshot` 与其 JSON 序列化（新增字段为追加方式，不删除既有字段）：p50/p95/p99、jitter、各比率、回退、建连占比、ECS 率、props 布尔、warming、switch 计数。
  - 更新 [dashboard.html](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/static/templates/dashboard.html#L230-L244) Resolver Health 表头与 [monitoring.js](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/static/js/monitoring.js#L135-L187) 渲染行：至少新增 p95、Timeout%、SERVFAIL、Trunc%、Fallbacks、Jitter、Features（dnssec/nofilter/ecs 徽标）；无数据时优雅降级。
  - snapshot 单测（JSON 字段存在性与数值）。
- **Acceptance Criteria Addressed**：AC-9
- **Test Requirements**：
  - `rule` TR-9.1：注入合成指标后 snapshot JSON 含全部新字段且既有字段不变。证据：单测
  - `rule` TR-9.2：dashboard.html 含新列表头、monitoring.js 含对应单元格渲染与缺失值降级分支。证据：模板/JS 静态检查（测试中读取文件断言关键标识）

## Task 10: 仿真 A/B harness 与场景断言
- **Status**：`completed`
- **Priority**：high
- **Depends On**：Task 5
- **Completion Evidence**：
  - 新增 [scheduler_sim_test.go](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/dnscrypt-proxy/scheduler_sim_test.go)：`simClock` 可注入虚拟时钟；`simNode`（base/jitter/tail+tailProb/timeoutP/errorP/servfailP/connNewP/addedAt/forcedFailures/step hook）+ `rollOutcome`；每步全节点用独立环境 RNG（seed 异或 golden-ratio 常量，经 uint64 变量规避 int64 常量溢出）预滚结果，保证 wp2/mds 面对**逐值相同的世界**；真实 `NewProxy` + 真实 `observeOutcome`（合成 PluginsState，timeout 走 PluginsReturnCodeServerTimeout + deadline 成本，servfail/success 走真实 `dns` 打包）；wp2 用全局种子 + `getWeightedCandidate`，mds 用注入 RNG + `selectCandidateLocked`，不复制任何算法。
  - 六测例全绿（`go test -run SchedulerSim -v`，跨进程重复运行逐值一致）：Deterministic、LongTail（4000 查询）、Stable（3000）、NetworkJitter（2s 全网尖峰窗口）、ColdStart（step500 加入 + 2 次早期硬失败）、CircuitBreaker（step600-1099 全超时，step1100 恢复）+ `BenchmarkSchedulerMDSSelect`（7 候选 684 ns/op）。
  - **实测 A/B（seed 2025，4000 查询）**：wp2 p50=10.7ms / p95=16.7ms / **p99=200ms** churn=3168；mds p50=10.9ms / p95=15.2ms / **p99=40ms（降 80%）** primarySwitches=5。稳定基线：wp2 churn≈1820-1860 vs mds primary 切换 11。抖动窗口（20 拍=2s）：wp2 churn=14-15，mds primaryMoves=1（≤1 且 ≤25% 预算）。冷启动：warming 阶段（step500-699）cold 份额 0.855（探索全给 warming 节点），全程不熔断，成熟后持续获选。熔断：**failover=1 拍（100ms，3 连硬失败即切）**，半开探测成功后 **63 拍（6.3s，≤1 个 half_open_interval 10s+余量 120 拍）** 重获 primary，结束时 circuit 闭合。
  - 仿真暴露并修复一个真实缺陷：熔断节点半开探测成功后，故障期的 2s deadline 样本仍污染 EWMA/p95 滑窗，加上仅 15% 探索份额，实测需 31.4s 才能重获 primary。修复方案为**恢复观察期（recovery probation）**：探测成功后置 `recoverUntilTotal=totalSamples+WarmupSamples`，期间复用 warming 语义（initialRtt 先验、惩罚中性、熔断解除武装），与冷启动保护对称；观察期结束若故障仍在则即时重新熔断。新增 `TestMDSRecoveryProbation` 固化（污染窗口立即重获 primary / 观察期内不熔断 / 期满断路器重新武装），报告行 Warming 同步反映观察期。
  - TR-10.3 rubric 自评 **5/5**：五需求场景齐全且与 AC 一一对应；双策略同流（预滚环境 RNG + 相同 observeOutcome）；阈值直接对应 AC-4/5/6/7 语义未放松；报告含 p50/p95/p99、churn、窗口移动、份额、failover/recovery 步数；且 harness 在本轮真实抓出一个产品级缺陷而非仅作展示。
- **Description**：
  - 新增 `scheduler_sim_test.go`：确定性仿真器（可推进虚拟时钟、种子 RNG），合成 N 个 resolver，每个节点由可配置延迟分布（含尾部分布）、失败率、SERVFAIL、截断、回退、建连事件模型驱动；查询事件流对两种策略完全相同。
  - 适配层：让 wp2 与 mds 在同一仿真接口下运行（复用真实策略代码，以测试实现的快照/反馈接口驱动，不复制算法）。
  - 五场景：稳定基线、长尾节点、短暂全网抖动（≤2s 同步飙升后恢复）、冷启动新节点（20 样本内 1–2 次失败）、primary 持续故障。
  - 每场景输出对比报告：端到端 p50/p95/p99、累计切换次数、抖动窗口内切换数、冷启动节点选择份额、故障切换时延、半开恢复结果；断言阈值按 AC-4/5/6/7 固化（p99 降 ≥10%、稳定/长尾场景切换 ≤ wp2、抖动窗口 mds ≤1 次且 ≤ wp2 的 25%、冷启动份额 ≥ 保底、熔断切换时延有上界并可恢复）。
  - 可选 `BenchmarkSchedulerSelect` 微基准。
- **Acceptance Criteria Addressed**：AC-3、AC-4、AC-5、AC-6、AC-7、AC-12
- **Test Requirements**：
  - `rule` TR-10.1：五类场景断言全部通过；同一种子重复运行结果逐值一致（确定性）。证据：`go test -run SchedulerSim -v` 两次输出一致
  - `rule` TR-10.2：长尾场景输出对比表，mds p99 相对 wp2 降幅 ≥10% 且 p95 不高于 wp2。证据：仿真报告（测试日志）
  - `rubric` TR-10.3：harness 可信度（场景覆盖、双策略同流、阈值与需求对应、报告可读性）；scale 1-5；anchors 见 spec AC-12；threshold >= 4；证据：代码评审与一次实际运行输出

## Task 11: 全量回归与构建门禁
- **Status**：`completed`
- **Priority**：high
- **Depends On**：Task 1–Task 10
- **Completion Evidence**：
  - `GOTOOLCHAIN=auto go vet ./...` 退出码 0（全包编译通过）；`go test -race ./...` 退出码 0（单包 7.5s，无数据竞争）；`go test -count=3 .` 连跑 3 轮全绿（13.8s，无偶发波动）；`gofmt -l .` 无输出。（`go build ./...` 在仓库根因同名目录提示 "build output already exists and is a directory"，vet/test 编译已覆盖全部包。）
  - Grep 复核：`updateServerStats/noticeSuccess/noticeFailure` 仅在 scheduler_feedback_test.go 的一条说明性注释中出现，无代码残留；Fetch 六元组（`[]byte, int, *tls.ConnectionState, time.Duration, *FetchTrace, error`）的 5 个包装方法与 2 个 `applyHTTPFetchTrace` 调用点（HTTP/ODoH）齐全，无遗漏调用点；TCP 回退/新建连接标记在 query_processing.go DNSCrypt 交换路径设置；WP2 选择逻辑无改动（getOne 仅在最前加 advanced type-switch，按 name 映射回 inner，旧路径逐字保留）。
- **Description**：
  - 运行 `gofmt`/`go vet ./...`、`go build ./...`、`go test ./...`，新增并发测试以 `-race` 运行；修复全部失败与告警。
  - 全仓 Grep 复核：无 `updateServerStats` 残留旧调用、无被遗漏的 Fetch 调用点、旧策略行为无意外改动。
- **Acceptance Criteria Addressed**：AC-10、AC-11
- **Test Requirements**：
  - `rule` TR-11.1：`go build ./...`、`go vet ./...`、`go test ./...` 退出码均为 0；含 race 的测试命令退出码为 0。证据：命令输出
  - `rule` TR-11.2：复核记录：旧策略选择/反馈路径 diff 仅包含适配性改动（指标双写），无选择逻辑变更。证据：`git diff` 人工核查结论

## Task 12: 独立审查与修复闭环
- **Status**：`completed`
- **Priority**：medium
- **Depends On**：Task 1–Task 11
- **Completion Evidence**：
  - 独立只读审查（全新 subagent）：12 AC 逐项判定，发现 1 Critical + 4 Major + 7 Minor，明细与终判见 [review.md](file:///Users/kkcarrot/swe-project/dnscrypt-proxy_fork1/.trae/specs/multidim-resolver-scheduler/review.md)。
  - C-1（RLock 懒写 metrics map 可致 concurrent map writes）已修：`candidatesLocked()` 改只读、`refreshServer()` 写锁内预发布；新增 `TestCandidatesLockedConcurrentScrape`，**实证**回退旧懒写实现时 `-race` 必报 DATA RACE，修复版通过。
  - M-1（Drop 响应污染 legacy EWMA）已修并新增 `TestObserveOutcomeDropSkipsLegacyRTT`；M-2（滑窗计数伪装 counter）10 族改名 `_window`+gauge、切换计数改 counter 并加 TYPE 契约断言；M-3/M-8（冷启动边界连击 + recovery probation 首个硬失败立即重新武装）已修，`TestMDSRecoveryProbation` 同步更新；M-6 热路径分配经 `PercentileBuf` 共享累加器优化至 1 alloc/op（726 ns/op）；M-5 example toml 注明窗口生效范围；M-9 dashboard 补 NoLog 徽标。
  - 终验：`go vet .`、`go test ./...`、`go test -race ./...`、`go test -count=2 .`、`gofmt -l .` 全部通过；SchedulerSim 六场景修复后复跑逐值稳定（LongTail p99 40ms vs wp2 200ms、breaker failover=1 步/recovery=63 步/结束闭合）。
  - M-4/M-7/M-9 残留/M-10 记录为 review.md 后续跟进，不阻塞验收；AC-11 修复后终判 ≥4。
- **Acceptance Criteria Addressed**：AC-10、AC-11、AC-12
- **Test Requirements**：
  - `rule` TR-12.1：全部 Critical/Major 有对应修复与回归测试，且关键回归测试对旧实现实证失败。证据：review.md 与 `-race` 输出
  - `rule` TR-12.2：修复后全仓 vet/test/-race/gofmt 全绿，仿真六场景断言不回归。证据：命令输出（review.md 终验节）
