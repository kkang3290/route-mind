# API 兼容范围与故障语义

## 兼容不是全字段代理

支持 `POST /v1/chat/completions`（OpenAI）和 `POST /v1/messages`（Anthropic）的**文本、工具、单候选**子集。接入格式不决定上游，模型别名配置才决定 Provider。错误不套 `data`；兼容 SSE 使用各自原生事件。

| 语义 | OpenAI | Anthropic |
|---|---|---|
| 模型 | model，具体别名或 auto | 同左 |
| 对话 | system/user/assistant/tool；字符串或 text 内容块 | system 字符串/text 块；user/assistant text/tool_use/tool_result |
| 输出限制 | max_tokens 或 max_completion_tokens（不能冲突） | 必填 max_tokens |
| 采样 | temperature、top_p | 同左 |
| 停止 | stop 字符串/最多四项数组 | stop_sequences，最多四项 |
| 工具 | tools[type=function] | tools[input_schema] |
| 选择 | auto / none / required / 指定 function | auto / none / any / 指定 tool |
| 流式 | stream，stream_options.include_usage | stream，原生 Messages SSE |
| 候选数 | n=1 | 单一 message |

OpenAI 未提供输出限制时默认 1024。auto 的最终输出还受 Agent final_tokens 限制；内部 decision/work 使用各自额度和采样设置，不使用用户最终 stop 来截断决策 JSON。

未知请求字段（例如 thinking、reasoning_effort、response_format、logprobs、metadata 等未实现项）、图片/音频、Responses API、多候选和私有推理块均明确拒绝，不静默丢弃。上游响应允许新增无关 envelope 字段，但不支持的内容块或流增量会失败。

### 不能承诺无损的部分

- OpenAI 将 content 与 tool_calls 分离，不能表达 Anthropic 所有 text/tool 块交错顺序；转换保留可表达的文本、工具数组顺序和 ID/结果关联，不承诺原始交错位置不变。
- 相邻 Anthropic 同角色消息会合并；多个系统文本会合并。OpenAI 工具错误编码成带错误前缀的工具文本。
- OpenAI 并行工具增量可交错；转成 Anthropic 时按有界缓冲依次输出合法、连续的 content_block。文本仍实时输出，工具参数不一定具有相同分片/时序。
- OpenAI 上游可能只在末尾给 usage。转成 Anthropic 时 message_start 的 input_tokens 可能暂为 0，最终 message_delta 在已知时提供 input_tokens 和 output_tokens 更新。部分严格只读取起始 input_tokens 的客户端会看到不完整输入计数；要用完整非流式响应或原生 Run total_usage 核算，不把起始 0 当作免费。
- 缺失 usage 在内部标记 `known:false`。OpenAI 流式只在已知且 include_usage 时输出 usage 帧，不伪造完整总量。Anthropic 起始 usage 为协议结构要求的暂定值，未知时不宣称其准确。
- finish_reason `length` / max_tokens 表示截断；原生 Run 标记 incomplete。工具交付为 tool_calls/tool_use；其他未支持停止原因视为上游协议错误。

## 模型及工具约束

具体模型绝不悄悄换模型。`auto` 每步通过 Decider 选择具体候选，不能递归选择 auto。候选需满足主体/服务端 Provider 数据流允许列表、上下文、输出、工具与 streaming 能力、累计预算。

兼容接口声明的工具**一律由客户端执行**，即使与服务端工具同名。无状态 auto 返回工具后，下一次请求应携带工具调用及完整结果对话；若需保留内部观察与累计预算，改用原生 Run。

- none：任何 work 都禁用工具。
- auto：允许直接 respond，允许模型提工具。
- required/指定工具：在完整验证的工具批次满足前，不能通过 respond 去掉工具来绕过；无可行模型则拒绝。
- 原生强制选择只约束下一次有效工具轮次，满足后不重复强制。

## 原生 Run DTO

```json
{
  "input": [{"role":"user","content":[{"type":"text","text":"任务"}]}],
  "tools": [
    {"executor":"server","name":"sum"},
    {"executor":"client","name":"local_lookup","parameters":{"type":"object"}}
  ],
  "tool_choice":{"mode":"required"},
  "max_tokens":512,
  "allowed_models":["small","strong"],
  "allowed_providers":["mock-openai","mock-anthropic"],
  "limits":{"max_steps":4},
  "stream":false
}
```

`input` 是统一消息结构，不是 OpenAI DTO。工具 choice mode 为 auto/none/required/named，named 另带 name。server 引用当前注册契约，不允许覆盖参数 Schema/description，可选 version 必须匹配；client 提供 parameters。请求只能缩小允许列表和限额，不能传 URL、密钥或扩大权限。未提供 lists/limits 使用服务端与主体权限交集。max_tokens 不超过 final_tokens。

创建和恢复要求 `Idempotency-Key`（1–128 字符）。幂等摘要绑定**精确请求体字节**，含 stream、revision 等；重试时保留原请求体。不同空格/键顺序也可能得到 409，避免客户端重建请求导致不一致。

- 创建首次成功 `201`，Location 指向 Run；执行直到终态或等待客户端工具。
- 非流式重复请求 `200` 返回当前快照，不重复调用。
- 相同流式请求的重复调用 `409`，Location 可查询，不重放 token。
- GET `200` 返回状态、revision、步骤摘要、用量、最终 output 或 required_actions；不返回原始输入/内部观察/工具服务端结果/operation ID。
- 外部模型失败/存储失败返回对应 HTTP 错误，不用 200 假装执行成功。GET 或幂等查询可正常返回 status=failed 的资源快照。
- 恢复为 `POST .../tool-results`：`{"revision":42,"results":[{"call_id":"call_...","content":"结果","is_error":false}],"stream":false}`。必须提交整个 pending 客户端批次，拒绝未知、重复、服务端 ID、缺项和旧版本。
- 重启恢复重新验证现有主体允许列表、具体模型和服务端工具版本/Schema。权限或契约不兼容明确拒绝，不替换工具或重置预算。

原生 SSE：`run.output_delta`（id、delta），`run.requires_action`、`run.completed`、`run.incomplete`（run 快照），错误为 `error`。不提前发送 work/决策进度，只有最终 respond 文本可实时输出。正常终态事件需在最终 PostgreSQL 检查点确认后发送。

### 删除数据

`DELETE /v1/agent/runs/{id}` 要求认证及 `If-Match: "42"` 的精确 revision。只有当前主体的终态或等待客户端工具、无活动 operation 的 Run 可删；成功 204，旧版本/执行中 409，非本人或不存在 404。关联工具、步骤与幂等记录级联删除。删除后旧幂等键不再保留，重新提交创建可能启动新任务；先停止客户端自动重试。此接口不会删除离线备份，备份保留/销毁由部署方管理。

## 流式失败、取消与核算

- 真正读取上游 SSE 并同步写出，慢客户端形成背压；请求体、事件、工具参数和总缓冲均有界。
- 首帧前可按明确可重试错误做有限重试。已有输出后不重播、不换模型；中途失败输出流错误，不发送 `[DONE]` / message_stop / 正常 Run 终态。
- 客户端断开会取消上游并释放并发槽。超时、无终止帧 EOF、格式错误也失败，不切成假成功。
- 工作步骤在服务端完整缓冲，不作为流答案透传。最终模型收到必要观察，仍属于不可信模型输出；“未直接透传 work”不是对模型永不复述信息的安全保证。
- 兼容 usage 仅表示当前对外交付的模型调用。auto 的决策/中间工作消耗见原生 Run total_usage、steps，不等于兼容 usage。
- 原生 total_usage 包含决策、重试和执行。缺失 usage 不当作零费用；调用次数、输出预留、deadline 是硬边界，费用不是精确保证。

## 认证与错误

OpenAI/原生 Bearer，Anthropic 支持 x-api-key；不允许混合冲突凭据。身份来自固定密钥配置而非请求体或转发 IP。模型列表按权限筛选，Run 访问带 owner 条件，非本人 404。

400 输入/不支持语义；401 凭据；403 权限；404 资源；409 幂等/版本/契约冲突；413 体积；415 Content-Type；429 主体速率/预算；502 上游/工具；503 入口容量/存储/熔断；504 deadline。上游错误体与凭据不返回。请求返回服务端生成 X-Request-ID。未启用浏览器 CORS。
