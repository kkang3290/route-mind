# Route Mind

基于 **Go Kratos** 的 LLM Gateway + 可复用多步 Agent 框架。

- **协议与 Provider 解耦**：OpenAI Chat Completions / Anthropic Messages，支持文本、工具和真正的 SSE 增量；两种北向格式均可调用任一种上游。
- **显式模型直连 / `auto` Agent**：决策器每步重新选择 `work` 或 `respond` 及具体模型，后一步看到前一步观察，不只是请求入口的一次分类。
- **可替换决策器**：`pkg/agent.Decider`，内置 LLMDecider 和受约束 RuleDecider fallback；可从外部 Go 程序直接使用 Provider、Gateway、Engine。
- **两类工具**：客户端工具暂停/恢复，服务端显式注册工具，以及混合批次。仅内置纯函数 `sum` 和固定本地词典 `lookup`，没有 shell/任意 URL 抓取。
- **PostgreSQL**：原生 Run、预算、步骤用量、工具 journal、幂等记录；等待工具时可跨重启恢复，执行中的外部调用不自动重跑。
- **可选 Redis 多实例协调**：共享主体 token、入口/上游并发和 Provider 熔断；独立租约续期、单半开探针、故障 fail closed，默认 local 无需 Redis。
- **独立迁移管理**：嵌入式版本 SQL、SHA-256 清单与 Git 历史校验、advisory lock、dirty 阻断、只读启动检查、独立 DDL 身份。

> 当前是有明确边界的核心 MVP，不是计费平台、代码沙箱或完整 HA 系统。默认演示只访问本机确定性 mock，**不会调用真实付费模型**；mock 的路由脚本只证明控制流，不证明模型质量或成本收益。

## 快速开始：完全本地演示

需要 Go **1.26.11+**、Docker Compose、curl；示例中的可选 JSON 提取使用 Python 3。`go.mod` 固定修复过安全问题的工具链。无需 kratos CLI、protoc、psql 或外部 Provider 密钥。

### 1. 配置开发凭据并启动 PostgreSQL

```sh
cp -n .env.example .env
# 编辑 .env，替换四个 replace-* 密码/API Key；建议用 openssl rand -hex 24。
# 不要提交 .env；密码须 URL-safe，或正确进行 URL 编码。
set -a; . ./.env; set +a

docker compose up -d --wait postgres
make migrate-up
# 迁移与业务角色分离：用 migration 身份给 app 角色授予明确表权限。
docker compose run --rm db-grant
make migrate-status
```

`DATABASE_URL` 是仅 DML 的 app 身份，`MIGRATION_DATABASE_URL` 是独立 DDL 身份。数据库卷已有数据时 entrypoint 不会重跑初始化脚本；不要为修复凭据误删现有卷，见[迁移运维](docs/database-migrations.md)。

### 2. 两个终端启动 mock 和 Gateway

```sh
# 终端 A（mock 仅监听 127.0.0.1:18081）
make mock

# 终端 B
set -a; . ./.env; set +a
make run
```

默认配置 `configs/config.yaml`：小模型和决策走 mock OpenAI，强模型走 mock Anthropic，访问密钥来自环境变量。正常服务监听 `127.0.0.1:8000`，仍要求认证。

```sh
curl -fsS http://127.0.0.1:8000/readyz
curl -fsS http://127.0.0.1:8000/v1/models \
  -H "Authorization: Bearer $GATEWAY_API_KEY"

# 明确模型：不进入 Agent；OpenAI 北向 → Anthropic 上游。
curl -fsS http://127.0.0.1:8000/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" -H 'Content-Type: application/json' \
  -d '{"model":"strong","messages":[{"role":"user","content":"hello"}],"max_tokens":256}'

# auto：本地脚本依次 small.work → strong.work → small.respond。
curl -fsSN http://127.0.0.1:8000/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" -H 'Content-Type: application/json' \
  -d '{"model":"auto","messages":[{"role":"user","content":"提取、分析、整理"}],"max_tokens":256,"stream":true,"stream_options":{"include_usage":true}}'

# Anthropic 北向 → OpenAI 上游，原生 Anthropic SSE。
curl -fsSN http://127.0.0.1:8000/v1/messages \
  -H "x-api-key: $GATEWAY_API_KEY" -H 'anthropic-version: 2023-06-01' \
  -H 'Content-Type: application/json' \
  -d '{"model":"small","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"hello"}]}'
```

### 3. 持久化混合工具与跨重启恢复

```sh
DEMO_DIR=$(mktemp -d "${TMPDIR:-/tmp}/route-mind-demo.XXXXXX")
curl -fsS http://127.0.0.1:8000/v1/agent/runs \
  -H "Authorization: Bearer $GATEWAY_API_KEY" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-create-1' \
  -d '{"input":[{"role":"user","content":[{"type":"text","text":"计算并请求本地查询"}]}],"tools":[{"executor":"server","name":"sum"},{"executor":"client","name":"client_lookup","parameters":{"type":"object"}}],"tool_choice":{"mode":"required"}}' \
  > "$DEMO_DIR/run.json"
```

返回 `201` 与 `Location`，状态 `awaiting_tool_results`，服务端 sum 已完成。`required_actions` 只包含客户端动作；步骤摘要和累计预算在响应中，内部 work 文本不在公开快照中。

可以在此时停止并重新启动 Gateway。随后查询同一 Run 并提交**完整客户端批次**：

```sh
RUN_ID=$(python3 -I -c 'import json,sys;print(json.load(open(sys.argv[1]))["id"])' "$DEMO_DIR/run.json")
curl -fsS "http://127.0.0.1:8000/v1/agent/runs/$RUN_ID" \
  -H "Authorization: Bearer $GATEWAY_API_KEY" > "$DEMO_DIR/current.json"
python3 -I -c 'import json,sys; r=json.load(open(sys.argv[1])); print(json.dumps({"revision":r["revision"],"results":[{"call_id":a["id"],"content":"client-local-result"} for a in r["required_actions"]]}))' "$DEMO_DIR/current.json" \
  > "$DEMO_DIR/results.json"
curl -fsS "http://127.0.0.1:8000/v1/agent/runs/$RUN_ID/tool-results" \
  -H "Authorization: Bearer $GATEWAY_API_KEY" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-resume-1' --data-binary @"$DEMO_DIR/results.json"
```

必须在原 deadline 内恢复（默认 180 秒，重启不会延长）。同键同一请求体返回当前快照，不重复执行；同键不同请求体 `409`。示例在独立私有临时目录保存结果；验证完请核对目录归属后自行清理，生产任务不要写入共享固定路径。

## API

| 路由 | 用途 |
|---|---|
| `POST /v1/chat/completions` | OpenAI 原生 JSON/SSE |
| `POST /v1/messages` | Anthropic 原生 JSON/SSE |
| `GET /v1/models` | 当前主体有权限的具体别名及 `auto` |
| `POST /v1/agent/runs` | 持久化创建并同步执行，要求 Idempotency-Key |
| `GET /v1/agent/runs/{id}` | 主体隔离的白名单快照 |
| `DELETE /v1/agent/runs/{id}` | 精确 If-Match revision，删除本人无活动 operation 的任务及关联数据 |
| `POST /v1/agent/runs/{id}/tool-results` | 版本校验、完整批次提交及继续执行 |
| `GET /healthz`, `GET /readyz` | 本地存活 / 数据库 Schema 与可选 Redis 策略就绪 |
| `GET /metrics` | 认证后的进程内 Prometheus 文本指标 |

[协议支持与差异](docs/api-compatibility.md) · [框架架构及扩展](docs/architecture.md) · [多实例协调](docs/multi-instance.md) · [迁移运维](docs/database-migrations.md) · [测试指南](docs/verification.md)

## 真实 Provider / 容器

仅在明确授权发送数据及承担调用费用后，复制并修改 `configs/providers.example.yaml`，填入当前可用的上游模型 ID、真实能力/保守上下文字节上限，配置密钥环境变量，再运行：

```sh
go run ./cmd/route-mind -conf /your/operator-owned/config.yaml
```

远程 Provider 使用 HTTPS，禁止跟随重定向。远程 PostgreSQL 要求 `sslmode=verify-full`；生产设置 `APP_ENV=production`。前置 TLS 代理需关闭 SSE 缓冲、设置合适的长请求超时，且不能把入口端口绕过代理暴露公网。

```sh
docker build --target runtime -t route-mind:local .
docker build --target migration -t route-mind-migrate:local .
# 迁移镜像只带迁移二进制和嵌入 SQL，不带 Provider 或运行角色凭据。
docker compose run --rm migrate up
```

应用镜像默认配置是**必须修改**的真实 Provider 模板，故意不内置可用密钥。Compose 的 `app` profile 也要求先调整挂载配置；开发 PostgreSQL 明文连接需显式设置 allow_insecure，生产不得开启。推荐先使用上面的宿主机离线快速开始。

## 测试与构建

```sh
make check                 # unit + race + vet + build + migration integrity
make vuln                  # pinned govulncheck; requires advisory database access
# 使用独立可销毁数据库，库名必须以 _test 结尾，绝不读取 DATABASE_URL：
export TEST_DATABASE_URL='postgres://test_admin:YOUR_TEST_PASSWORD@127.0.0.1:5432/route_mind_test?sslmode=disable'
make integration           # PostgreSQL + HTTP + restart/idempotency + race
```

集成测试需要测试库管理员能创建临时 Schema/角色；每个测试仅清理自己创建的资源。没有 TEST_DATABASE_URL 时命令明确失败，不把跳过当作通过。可选 Redis 验证：

```sh
export TEST_REDIS_URL='redis://127.0.0.1:6379/0' # 专用可销毁服务，不回退 REDIS_URL
make coordination-integration # 真实 Redis + race，无需 PostgreSQL
make integration-all          # 两种依赖的组合集成测试
# 专用测试库先由独立 migration 身份 up；烟测不接收迁移凭据：
make coordination-smoke       # 双进程 + curl + 断连/强杀/TTL，仅本地 mock
```

生产配置及本机双副本演示见[多实例协调](docs/multi-instance.md)。CI 提供隔离 PostgreSQL 和 Redis，默认 `make check` 仍不需要 Redis。

## 边界

不支持 Responses API、多模态、私有 thinking/reasoning 块、全字段无损兼容；不提供后台自动接管、Exactly-once 外部副作用、SSE 重放或自动计费。默认限流/熔断为单进程策略；可选 Redis 提供共享分布式配额，但不构成完整 HA 调度系统，也不保证所有分区/故障切换下远端物理在途数绝不超额。未知上游 usage 明确为未知，输出额度预留不是精确费用上限。原生 Run 存储任务内容，应按数据分类配置保留期、限制数据库访问并制定备份删除策略。

Apache-2.0，见 [LICENSE](LICENSE)。
