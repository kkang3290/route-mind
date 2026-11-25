# 架构与扩展

## 分层

```text
Kratos raw HTTP routes
  └─ service：认证 / admission / 请求规范化 / 原生协议 JSON + SSE
      └─ biz：主体策略、direct/auto 分派、恢复权限检查
          ├─ pkg/llm.Gateway：只接受具体模型
          │   └─ OpenAI / Anthropic Provider → 统一 Response / Event
          └─ pkg/agent.Engine：Decider → work/tool/observation → Decider → respond
              └─ RunStore checkpoints → PostgreSQL CAS + journals
```

- `cmd/route-mind`、`internal/app` 装配 Kratos、pgx pool、配置、Gateway、LLMDecider、纯函数工具、生命周期。
- `pkg/llm` 和 `pkg/agent` 不依赖应用配置、Kratos、pgx 或环境变量，可由其他 Go 程序导入。
- `pkg/llm/protocol` 共享南北向转换，避免两个方向有不同的字段语义。
- `cmd/migrate` 独立部署，不在业务启动时执行 DDL；SQL/清单嵌入二进制。

## 决策不是一次路由

Decider 的输入 View 只包含原始任务、此前观察、有权限且可满足当前预算/能力的候选、工具契约及累计消耗，不带凭据。输出严格为：

```json
{"action":"work","model_id":"small","instruction":"提取必要事实"}
```

动作仅 work/respond。Engine 校验选择、上下文、权限和硬预算后才调用，instruction 是有界低信任提示，不能扩大权限。work 完整缓冲并记录为观察；下一次决策实际看到它。respond 为独立、禁用工具的最终调用，只有这里允许流出答案。LLMDecider 也通过 Gateway，计入调用、用量与检查点。坏 JSON/非法候选只做确定性 RuleDecider fallback，不无限“修复 JSON”。

控制器持有工具选择、允许模型/Provider、最终 stop/采样/输出限制；模型不能改变这些值。没有可行模型就错误，不把历史静默截掉。上下文大小采用保守字节限额，不宣称精确 token 计数。

## Go 扩展点

- **替换决策模型**：配置 `agent.decision_model` 为另一个具体注册模型，重启；不能用 auto。配置文件不做热重载。
- **替换决策算法**：实现 `agent.Decider` 或用 `agent.DeciderFunc`，传给 `agent.NewEngine`。可执行公共包示例：`go test ./pkg/agent -run ExampleEngine -v`，不依赖 internal 类型、不访问付费 Provider。
- **增加 Provider**：实现 llm.Provider 的 Generate/Stream；后者同步调用 Sink，Sink 返回错误必须及时关闭流，最终返回前验证 terminal event。禁止偷偷 buffering 整个回答再拆分伪装流式。
- **Invoker / 核算契约**：Engine 的严格逐 attempt 预算和持久化预留依赖 `llm.WithObserver`。内置 Gateway 保证每个真实 attempt（含重试）Before/After 成对；自定义 Invoker 必须遵守相同契约，不得内部隐式重试或绕过 observer。自定义 Decider 若调用模型，也应使用同一 Gateway/context；任意外部 I/O 不会神奇地被框架发现或核算。
- **增加服务端工具**：用显式版本和 JSON Schema 注册 `agent.ServerTool`，应用层另加主体允许列表。工具是受信任的 Go 代码，必须协作响应 context；不是沙箱。Schema 禁止远程引用且限制深度/节点/大小。整个调用批次先校验，再执行；首版串行执行，不做工具重试。
- **其他持久化**：实现 agent.RunStore 的原子 Create/Resume、owner/revision/operation fencing Save。生产应用只装配 PostgreSQL，没有内存降级。

## 状态、并发与崩溃

Run 在 running/responding 时有唯一 operation token；waiting/terminal 释放 token。PostgreSQL 的原子条件更新以 principal + public UUID + revision + operation 取得推进权。外部调用期间没有长事务或行锁。

每次真实模型 attempt：持久化预算/step running → 发起调用 → 原子记录 step/usage。工具：保存 planned batch → 保存 running → Execute → 保存结果。提交确认丢失时按幂等 operation 的 checkpoint_revision 核对，不盲目重做。它解决的是数据库提交不确定，不提供与外部副作用的原子性。

waiting Run 在重启后可恢复，继续原绝对 deadline 和累计预算；执行中进程崩溃不自动接管。5 秒维护循环分批收敛到 deadline 的任务，运行工具标记 outcome_unknown，模型 usage 未知。存储失败不降级，不用数据库结果不确定来触发模型重试。终态迟到写通过 CAS 拦截。

当前公开 Snapshot 是字段白名单，不是 Run 的完整 JSON；输入/观察/服务端工具结果不暴露。Snapshot 与 Advance 不允许并发读取同一 Run 指针；HTTP GET 从存储加载独立副本。

## 可靠性与资源

入口 active request 槽（默认 32、每主体 4）与 Gateway 上游槽（全局 32、每 Provider 8）独立。等待客户端工具不占活跃槽。主体 token-bucket 默认 60 请求/分钟、突发 10；默认 `coordination.mode: local` 仅进程内有效。可选 `redis` 将这些业务额度改为 namespace 下的集群共享额度，每进程只保留有限硬保护。默认 fail-fast，不排无界队列，详见[多实例协调](multi-instance.md)。

`pkg/llm.AttemptController` / `Permit` 是框架窄接口，不依赖 Redis。应用通过 `internal/reliability.Admission` 和 `internal/data/redis` 注入入口/上游两个独立资源池；每 attempt 均在 Before 前获取、Before 后再验证，完成/协调失败仍配对 After 并保留用量和预算。协调/容量/熔断错误显式标记为控制错误，不进入 RuleDecider fallback。Redis 不存 Run 内容或执行权，不改变 PostgreSQL 的 CAS 和禁止自动接管语义。

上游有连接/TLS/响应头/流空闲超时；请求和 Run 有总 deadline；SSE 写有重置的 write deadline。Kratos 显式 Timeout(0)，避免默认 1 秒截断；原生 HTTP 路由也经过共同认证、恢复、限额和安全头，ResponseWriter 保留 Unwrap/Flush 能力。输出后不再重试或切换 Provider。

重试仅限明确 transient、无任何流事件的失败；应用默认最多一次重试，受 Retry-After、退避/jitter 和请求 deadline 约束。连续上游服务端错误触发 Provider 级熔断，默认五次/15 秒冷却。local 为进程内状态；Redis 模式共享状态及唯一半开探针，以不可复用 epoch 拦截迟到旧结果，下游取消/写失败和上游认证/限流不计健康失败。直连不跨模型自动降级。

应用关闭时停止接收并取消在途请求，最多等待 Kratos 10 秒；关闭维护协程、idle HTTP 连接及数据库 pool。挂起任务不绑定已结束请求 context。

## 数据与信任边界

操作员配置可信 Provider URL 和环境变量名，客户端不能传任意 URL/凭据。Provider 上下文流向是显式服务端/主体允许列表交集，包含决策模型所在 Provider。密钥不进入模型上下文/持久化/默认日志。所有 SQL 参数化，表标识来自受约束 schema。远程 TLS 验证，重定向不转发密钥。

模型输出及工具参数不可信，权限由代码实现而不是“系统提示”。内部观察可能包含敏感内容；最终模型可基于它生成答案，因此部署方仍需数据分类及输出安全策略。公开快照不直接泄漏观察不等于 LLM 永远不能复述信息。

原生 Run 数据默认保留 24 小时，有总量/主体容量，定时级联清理；可按 owner + ID + revision 删除静止任务。数据库备份、日志保留、加密与组织授权由部署方配置。兼容请求默认不写完整 prompt 到数据库。
