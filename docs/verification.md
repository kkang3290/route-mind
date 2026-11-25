# 测试与验证

## 本地检查

需要 Go 1.26.11+。默认单元测试只使用本地假上游，不需要 Provider 密钥，也不会调用付费模型。

```sh
make check       # unit、race、vet、迁移完整性、三个二进制构建
make vuln        # 固定版本 govulncheck，需要访问漏洞数据库
go mod verify
```

`make migration-verify BASE_REF=<commit>` 同时检查 SHA-256 清单和指定 Git 基线中的历史 SQL。CI 以 PR base 或 push before 为基线，防止修改旧迁移后重算清单绕过检查。

## PostgreSQL 集成测试

使用独立、可销毁且名称以 `_test` 结尾的数据库。管理员需要创建临时 schema 和测试角色的权限：

```sh
export TEST_DATABASE_URL='postgres://test_admin:YOUR_TEST_PASSWORD@127.0.0.1:5432/route_mind_test?sslmode=disable'
make integration
```

测试不会回退到业务 `DATABASE_URL`。缺少变量、库名不合要求或数据库不可用时明确失败；每个测试只清理自己创建的资源。

覆盖范围包括：

- 双协议 JSON/SSE、工具调用、逐步模型选择、上下文与预算约束。
- owner 隔离、幂等、并发恢复、revision/operation fencing、提交确认丢失后的核对。
- 混合工具暂停/恢复、执行取消、结果未知、deadline 收敛和级联清理。
- 调用前后检查点失败不重放外部调用，最终持久化失败不发送 SSE 成功结束事件。
- fresh/repeated up、down/up、dirty 阻断、事务回滚、迁移锁、人工恢复、样本数据升级和只读启动检查。

初版升级测试使用 fixture；后续版本应增加从上一发布 schema 升级的数据保留测试。

## Redis 多实例集成测试

```sh
export TEST_REDIS_URL='redis://127.0.0.1:6379/0' # 专用可销毁服务
make coordination-integration # 真实 Redis + race，不要求 PostgreSQL
make integration-all          # 同时要求 TEST_DATABASE_URL 和 TEST_REDIS_URL
```

测试不回退 `REDIS_URL`，随机 namespace 带归属 marker，清理只枚举并删除自己的精确 key，不 FLUSHDB。覆盖共享 token/两级并发、主体与 namespace 隔离、超 lease TTL 的 token 保留、续租、ACK 丢失与卡住、独立 watchdog、策略/endpoint 一致性、单半开探针/中性探针、迟到旧成功、过期 renew/release、池等待与关闭、元数据回收。真实 Redis 执行写入后的 Lua 运行时错误测试证明保守占额会过期，而不是假设脚本失败回滚。HTTP 测试覆盖慢正文、阻塞 SSE、取消 hook 与复用连接；真实流丢租约后没有成功 terminator 或重试；工具丢租约后为 outcome_unknown、无下一次执行。

## 真实进程烟测

先对专用测试库执行迁移并授权，建议使用仅 DML 的 app 角色运行烟测：

```sh
export TEST_DATABASE_URL='postgres://test_app:YOUR_TEST_PASSWORD@127.0.0.1:5432/route_mind_test?sslmode=disable'
python3 -I scripts/smoke.py
```

脚本构建并启动 Gateway 和本地 mock，通过 curl 验证 12 组协议请求、3 步/6 次调用的 Agent 路由、混合工具、真实进程重启恢复及重复请求去重。它拒绝占用已有的 8000/18081 listener，仅停止自己启动的进程；业务子进程不接收迁移凭据。

每轮烟测使用独立幂等命名空间，可重复运行而不删除已有测试数据。批量请求使用临时限流配置，不改变服务默认限额。CI 的烟测使用其隔离测试库管理员，运行身份权限另由集成测试检查。

## 容器与 CI

CI 的 PostgreSQL + Redis 7.4.8 隔离服务运行组合集成 race，再运行原烟测和双进程烟测：

```sh
# 专用测试库已经迁移并授权；仅使用本地确定性 mock：
make coordination-smoke
```

双进程脚本选择空闲端口和随机 namespace，启动两个真实 Kratos 二进制及自有 Redis TCP 代理。验证共享 rate、真实上游 peak=1、决策拒绝无 fallback、原生 6 次模型调用、唯一半开探针、强杀持有者后 TTL 回收、超 TTL 长调用续租、断连 fail-closed、就绪/指标与无重放恢复。它不停止调用者提供的 Redis，只操作自己的 Popen 进程和有 marker 的精确 namespace。脚本仅接受 literal-loopback 的明文测试 Redis；生产 TLS 语义由配置/传输验证覆盖。

输出包含 mock 真实调用数/峰值、协调逻辑操作及耗时均值、测试期间 Redis 服务端命令增量/观察 QPS。服务端数包含脚本子命令、就绪、测试管理操作及同服务其他客户端；不是纯业务 QPS或生产吞吐基准，故障耗时也包含在观察均值中。真实 Provider 调用为 0。

```sh
docker build --target runtime -t route-mind:local .
docker build --target migration -t route-mind-migrate:local .
```

镜像验证需要访问基础镜像仓库。本机二进制测试不能替代镜像构建/启动验证；CI 结果以对应提交的 GitHub Actions 为准。

## 已知依赖告警

2026-11-25 对含 go-redis/v9 v9.24.0 的锁定依赖重新运行 `go mod verify`、govulncheck 及 verbose 扫描：0 个可达漏洞，仍有 1 个 package 级和 5 个 module 级未调用告警；这不等于所有依赖零告警，也不保证不存在未知漏洞。新增 Redis 依赖和传递依赖通过模块 checksum 校验；x/sys 已升级 v0.50.0，原 GO-2026-5024 不再出现。

| Advisory | 模块 | 修复版本 | 暂缓原因 |
|---|---|---|---|
| [GO-2026-5471](https://pkg.go.dev/vuln/GO-2026-5471) | Kratos v2.9.2 | 未提供 | package 级 Confused Deputy 告警，静态分析未发现受影响符号调用；继续跟踪上游，不作绝对安全结论。 |
| [GO-2026-6443](https://pkg.go.dev/vuln/GO-2026-6443) | grpc v1.74.2 | v1.82.2 | module 级，当前未启用 gRPC server。 |
| [GO-2026-6441](https://pkg.go.dev/vuln/GO-2026-6441) | grpc v1.74.2 | v1.83.1 | module 级，当前未启用 xDS。 |
| [GO-2026-6348](https://pkg.go.dev/vuln/GO-2026-6348) | grpc v1.74.2 | v1.83.1 | module 级，当前未启用 gRPC transport server。 |
| [GO-2026-6061](https://pkg.go.dev/vuln/GO-2026-6061) | grpc v1.74.2 | v1.82.1 | module 级，当前未启用相应 xDS/HTTP2 server。 |
| [GO-2026-4762](https://pkg.go.dev/vuln/GO-2026-4762) | grpc v1.74.2 | v1.79.3 | module 级，当前无 gRPC 授权入口。 |

在 **2026-12-23 前**复核；引入 gRPC/xDS/Windows 功能、变更相关调用路径或上游提供修复时提前复核。CI 不抑制这些告警。详细结果：

```sh
go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 -show verbose ./...
```

真实厂商鉴权、线上兼容范围、模型质量和成本收益，需要明确授权发送数据及付费后另行验证。本地 mock 只验证协议与控制流。
