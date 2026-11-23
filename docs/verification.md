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

## 真实进程烟测

先对专用测试库执行迁移并授权，建议使用仅 DML 的 app 角色运行烟测：

```sh
export TEST_DATABASE_URL='postgres://test_app:YOUR_TEST_PASSWORD@127.0.0.1:5432/route_mind_test?sslmode=disable'
python3 -I scripts/smoke.py
```

脚本构建并启动 Gateway 和本地 mock，通过 curl 验证 12 组协议请求、3 步/6 次调用的 Agent 路由、混合工具、真实进程重启恢复及重复请求去重。它拒绝占用已有的 8000/18081 listener，仅停止自己启动的进程；业务子进程不接收迁移凭据。

每轮烟测使用独立幂等命名空间，可重复运行而不删除已有测试数据。批量请求使用临时限流配置，不改变服务默认限额。CI 的烟测使用其隔离测试库管理员，运行身份权限另由集成测试检查。

## 容器与 CI

```sh
docker build --target runtime -t route-mind:local .
docker build --target migration -t route-mind-migrate:local .
```

镜像验证需要访问基础镜像仓库。本机二进制测试不能替代镜像构建/启动验证；CI 结果以对应提交的 GitHub Actions 为准。

## 已知依赖告警

2026-11-23 的 govulncheck 符号级扫描未发现可达漏洞，但仍有以下未调用告警；这不等于所有依赖零告警，也不保证不存在未知漏洞。

| Advisory | 模块 | 修复版本 | 暂缓原因 |
|---|---|---|---|
| [GO-2026-5471](https://pkg.go.dev/vuln/GO-2026-5471) | Kratos v2.9.2 | 未提供 | package 级 Confused Deputy 告警，静态分析未发现受影响符号调用；继续跟踪上游，不作绝对安全结论。 |
| [GO-2026-6443](https://pkg.go.dev/vuln/GO-2026-6443) | grpc v1.74.2 | v1.82.2 | module 级，当前未启用 gRPC server。 |
| [GO-2026-6441](https://pkg.go.dev/vuln/GO-2026-6441) | grpc v1.74.2 | v1.83.1 | module 级，当前未启用 xDS。 |
| [GO-2026-6348](https://pkg.go.dev/vuln/GO-2026-6348) | grpc v1.74.2 | v1.83.1 | module 级，当前未启用 gRPC transport server。 |
| [GO-2026-6061](https://pkg.go.dev/vuln/GO-2026-6061) | grpc v1.74.2 | v1.82.1 | module 级，当前未启用相应 xDS/HTTP2 server。 |
| [GO-2026-4762](https://pkg.go.dev/vuln/GO-2026-4762) | grpc v1.74.2 | v1.79.3 | module 级，当前无 gRPC 授权入口。 |
| [GO-2026-5024](https://pkg.go.dev/vuln/GO-2026-5024) | x/sys v0.38.0 | v0.44.0 | module 级，当前不调用受影响的 Windows API。 |

在 **2026-12-23 前**复核；引入 gRPC/xDS/Windows 功能、变更相关调用路径或上游提供修复时提前复核。CI 不抑制这些告警。详细结果：

```sh
go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 -show verbose ./...
```

真实厂商鉴权、线上兼容范围、模型质量和成本收益，需要明确授权发送数据及付费后另行验证。本地 mock 只验证协议与控制流。
