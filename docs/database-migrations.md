# PostgreSQL 迁移管理与运维

## 身份和部署顺序

业务运行角色只需目标 schema 的 USAGE、四张应用表的 SELECT/INSERT/UPDATE/DELETE、identity sequence 的 USAGE，以及 schema_migrations 的 SELECT。不能 CREATE/ALTER/DROP，不能修改迁移元数据。DDL 角色是单独 job 的身份，业务容器**不要注入** MIGRATION_DATABASE_URL。

开发环境 `deploy/dev-init.sql` 创建两个 NO SUPERUSER/CREATEDB/CREATEROLE 身份并收紧 public schema CREATE；`deploy/dev-grants.sql` 是迁移后的显式授权。生产由 DBA 用审核过的角色/schema 名执行等价 SQL，不能直接复制开发密码。app 用户不能设置成数据库 owner 或 schema owner；别使用超级用户运行服务。

部署顺序：

1. 备份，验证恢复能力，固定本次应用/迁移镜像版本。
2. 独立 migration job 注入 DDL 凭据、正确 DATABASE_SCHEMA，执行 up；失败阻断发布。
3. 以 DDL 身份执行本版显式 DML/metadata 只读授权；新增表须审核后纳入授权。
4. 启动业务副本，仅注入运行凭据；只读 CheckSchema 通过才就绪。
5. 观察错误、延迟、锁等待及 Run 清理；确认旧版本退出后才做 contract。

```sh
make migration-verify BASE_REF=HEAD
make migrate-status           # DATABASE_URL；只读，不创建迁移元数据
make migrate-up               # MIGRATION_DATABASE_URL；嵌入 SQL
# 无 psql 时的开发授权：
docker compose run --rm db-grant
```

生产 DATABASE_URL 使用 `sslmode=verify-full`、可信 CA；禁止把 DSN 放日志、命令行参数、工单或镜像。应用池默认 16 连接、5 秒连接/SQL 超时、1 秒锁等待；根据副本数协调 PostgreSQL 连接上限。迁移有独立专用连接，不占业务池。

## SQL 历史不可变

- 文件配对：`YYYYMMDDHHMMSS_description.up.sql` / `.down.sql`，UTC 时间、lower_snake_case、版本严格递增。
- SQL 通过 migrations.Files embed.FS 进入 cmd/migrate，发布不依赖工作目录或外部 SQL 挂载。
- 同秒版本即使名字不同也拒绝生成，O_EXCL 防覆盖，创建锁防并发碰撞；已有残留锁须检查后人工处理，不能无脑删除。
- 已合入/发布的 SQL 不能改/删/重命名；修复用更新版本。不要用“重新生成 checksum”掩盖改旧文件。
- SHA-256 清单校验当前内容，CI 再与 PR base/push before commit 的实际 Git SQL 比较，拒绝历史改写和低于基线的新版本。首版基线只有 LICENSE，因此如实是初始迁移验收。
- checksum 是**构建源码完整性**，不是 PostgreSQL 实际 schema 漂移检测，也不是 golang-migrate 的 SQL checksum ledger；数据库元数据只记录 version/dirty。

```sh
make migration-new NAME=add_optional_field
# 编辑新 up/down，移除 TODO；不要编辑历史文件。
make migration-checksum
make migration-verify BASE_REF=HEAD
make integration
```

生成和清单操作拒绝源文件/manifest 符号链接；manifest 临时文件写完、sync 后原子替换。源码目录是操作员拥有的可信仓库，不是允许用户上传迁移脚本的接口。

## 事务与在线变更

普通多语句迁移显式 `BEGIN; ... COMMIT;`。不要以为 migrate 自动把所有 SQL 包在一个事务。失败后封装会回滚遗留失败事务，但保留独立写入的 dirty 标记，避免“SQL 部分回滚却假装干净”。

`CREATE INDEX CONCURRENTLY` 等不能在事务中执行，必须单独非事务迁移；失败要检查 `pg_index.indisvalid`、实际索引定义和使用情况，再有计划清理 invalid index/前向修复。不要给所有 CREATE 都加 IF NOT EXISTS 来掩盖漂移。

采用 **expand → 兼容应用发布/分批可重入 backfill → contract**：先新增可空列/表，等旧应用退出、数据回填验证后，才收紧/删除。大回填不要塞进持锁很久的 DDL 事务。应用启动要求最低版本、非 dirty、必需列类型/唯一键契约，不要求“恰好等于二进制最后版本”；已测试旧应用可在额外可空列的未来 schema 上运行。

## 互斥与超时

复用 migrate 官方 pgx/v5 driver 的 PostgreSQL advisory session lock。同数据库/schema/metadata 的两个 runner 会互斥。默认锁超时 5 秒、SQL 30 秒、CLI 总期限两分钟；锁等待/语句超时非零退出，专用连接关闭释放会话锁。命令取消请求停止，但当前 SQL 只在其 timeout/cancel 边界结束，不能承诺瞬时杀死。

业务启动**不调用** migration driver 的构造器（它可能创建 metadata），仅执行 SELECT 检查。缺表、dirty、不兼容则明确启动失败；没有自动建库、自动 force 或内存降级。

## Dirty 与人工恢复

发生 dirty：停止后续迁移和应用发布，保留日志、迁移版本及备份证据。

1. 用只读 status 确认 version/dirty。
2. 检查实际 schema、事务是否全回滚、非事务操作是否部分完成、数据/索引状态。不要把 dirty version 当作“SQL 已全部执行”。
3. 根据真实状态选择人工修复或前向修复；记录审核和恢复验证。已经发布的 SQL 不改写。
4. 只有已确认实际 schema 对应目标版本后，执行显式 force；再 status、验 schema、继续 up/发布。

```sh
# 以下仅为流程形状；VERSION 必须由实际检查确定，绝不能照抄猜测。
go run ./cmd/migrate force --version="$VERIFIED_VERSION" \
  --ack-schema-verified --reason='经审核的故障单和实际 schema 核对结论'
```

force 只改 metadata，**不执行 SQL、不修表、不恢复数据**。CLI 要求非负显式版本、确认和单行原因；输出用于留存，凭据不能写在 reason 中。Runner.Force 是内部运维 API，不是公开 HTTP 端点。

## 回滚和备份

生产默认前向修复，或回退到兼容的旧应用，不自动 down。down 仅开发/测试，且同时要求：

```sh
APP_ENV=development ALLOW_DESTRUCTIVE_MIGRATIONS=1 make migrate-down STEPS=1
```

CLI 还要求 `--ack-data-loss`（Makefile 已显式传入）。生产 APP_ENV 下禁止 down，没有 drop 命令。初始 down 会删除四张业务表和数据，仅用于空库/可销毁测试；up/down 成对不代表业务数据可逆。未来不可逆 down 应明确报错，不能空脚本假成功。

破坏性 contract 前需备份和隔离库恢复演练；备份包含原生任务内容，应用 TTL/DELETE 不会自动擦除备份。为备份制定访问控制、加密、生命周期与法律保留规则。不要用 `docker compose down -v` 当作修复已有数据库的手段，它会销毁开发数据；任何真正删卷动作先核对归属并人工确认。

## Run 持久化不是 Exactly-once

模型/工具调用前持久化预留与 running，调用后保存结果，提交确认丢失时核对 operation checkpoint。进程崩溃/外部结果未知时不自动重跑；超 deadline 后收敛到 failed/outcome_unknown。原生终态响应必须确认保存成功，流已输出但数据库失败时错误终止。

绝对 deadline 与 retention 分离（默认 180 秒/24 小时），5 秒维护循环按批次 FOR UPDATE SKIP LOCKED 收敛超时任务并删除过期终态，FK 级联清理 steps/tools/idempotency。不删除仍有效挂起任务。手工清理可通过 owner 隔离的 DELETE Run API（精确 revision、只能无活动 operation）进行。保留期至少覆盖执行 deadline。

## 验收与升级测试

```sh
# 必须是独立 *_test 数据库的管理员，不读取 DATABASE_URL。
TEST_DATABASE_URL='postgres://test_admin:YOUR_PASSWORD@127.0.0.1:5432/route_mind_test?sslmode=disable' make integration
```

当前测试覆盖：fresh up、重复 up、初始可逆 down/up、带样本 Run 的 additive upgrade/down 保留数据、未来兼容 schema、两个 runner 互斥、失败事务无部分 DDL且 dirty 阻断、人工修复后 force/继续升级、不能 DDL 的运行角色可检查 schema，以及 Run 唯一性/CAS/事务回滚/owner 隔离/恢复/模糊 commit/过期删除。

后续发布须在测试中增加上一发布 schema → 本版的数据保留升级路径及本次 contract 的回退边界。首版没有旧发布基线，不把 fixture 测试冒充真实生产升级历史。
