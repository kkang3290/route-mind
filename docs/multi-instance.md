# 多实例网关协调（Redis 协议 v1）

## 解决什么，不解决什么

`coordination.mode: local` 是默认值，无需 Redis；额度与熔断只在当前进程内有效。
`mode: redis` 将以下状态放到一个 namespace 下共享：

| 配置 | Redis 模式含义 |
|---|---|
| `server.rate_per_minute` / `burst` | 每个已认证 principal 的集群 token bucket |
| `server.max_concurrent` | 集群活跃 HTTP 请求上限 |
| `server.per_principal_concurrent` | 同 principal 的集群活跃请求上限 |
| `gateway.max_concurrent` / `provider_concurrent` | 集群全局 / Provider 上游 attempt 上限 |
| `gateway.circuit_failures` / `circuit_cooldown_seconds` | 共享 Provider 熔断阈值 / 冷却时间 |

每进程仍有有限硬容量保护，但不会再扣另一份业务 token 或运行第二套 Provider 熔断器。入口与上游是**独立资源池**：Agent 每个 decision/work/respond/retry 获取上游许可，整个 HTTP 请求只扣一次入口 token；等待客户端工具时 HTTP 返回并释放入口许可。

Redis 只存配额、租约、摘要和健康状态；principal 使用已认证 ID 的 SHA-256 摘要，不使用 API Key、IP 或任意客户端 header。Redis 不存 prompt、response、工具参数或 Run。PostgreSQL 继续负责 Run、幂等、revision/operation CAS；不新增数据库迁移。

**这不是完整 HA 调度系统**：没有后台队列、Agent Worker、执行中 Run 自动接管、SSE 重放或 Exactly-once。Redis 许可不是 Run 推进权，也不能 fence 外部 Provider：网络分区、进程暂停、取消不及时、Redis 异步复制/故障切换丢状态时，不能保证真实远端物理在途数绝不超过额度。

## 本机双实例演示

先完成 README 的 PostgreSQL 迁移、DML 授权与环境变量配置，启动 `make mock`。然后：

```sh
docker compose --profile redis up -d --wait redis
export REDIS_URL='redis://127.0.0.1:56379/0'
make build
bin/route-mind -conf configs/redis.example.yaml
# 另一个终端：复制示例到自己的配置文件，只把 server.address 改为 127.0.0.1:8001，
# 保持 namespace、限额、Provider 映射和租约参数相同，再启动第二个进程。
```

该示例仅访问本地确定性 mock，无付费调用。可选 Compose Redis 是无持久卷、限制内存、`noeviction` 的开发服务，不是生产 HA/持久化配置；不得把其端口发布到公网。默认 local 演示不需要启动它。

## 配置与安全

完整示例见 `configs/redis.example.yaml`。默认连接池上限 32、dial 1000ms、operation 500ms、TTL 30000ms、renew 10000ms。必须满足 operation < renew ≤ TTL/3；连接、读写、池等待和业务操作都有界，SDK 副作用命令自动重试关闭。只有明确 NOSCRIPT 才安全转 EVAL，不把响应丢失当作可以重试 acquire。

`gateway` 省略字段使用 32/8 并发、1 次重试、5 次失败/15 秒冷却；显式 `retries: 0` / `circuit_failures: 0` 分别关闭重试/健康熔断，不关闭并发控制。负值拒绝。主体 429、全局/上游容量 503，均 fail-fast，不排无界队列。

Redis URL 仅从 `coordination.url_env` 指定的环境变量读取，排除配置运行态序列化和错误日志。所有 URL query 参数均拒绝，不能通过 query 覆写 TLS、超时和重试。远程只允许验证证书的 `rediss://`，使用系统信任根和至少 TLS 1.2；私有 CA 应安装到受信任根，不关闭证书验证。`APP_ENV=production` 强制认证且禁止明文 Redis。`redis://` 仅在显式 `allow_insecure_local: true` + **literal loopback** 开发地址下允许，`localhost` 或容器 DNS 不构成例外。

容器间不要使用 `redis://redis:6379` 绕过限制；应部署可信 TLS Redis，配置 `rediss://` 和 ACL，或使用宿主机二进制进行上述本地演示。密钥不写入 YAML、命令输出、指标或 git。

首版支持单逻辑主节点 URL（可使用托管故障切换端点），不实现 Sentinel/Cluster 发现。所有脚本 key 在 `route-mind:{namespace}:` 同一 hash slot，但这不等于已经支持 Redis Cluster。服务端至少使用测试过的 Redis 7.4.8；开发和 CI 固定 `redis:7.4.8-alpine`，升级需重新验证协议和故障用例。

### ACL 与容量

生产使用专用 ACL 身份，key pattern 限定为 `~route-mind:{YOUR_NAMESPACE}:*`。运行身份所需命令为：

- 连接：`HELLO`、认证所需 `AUTH`、非零 DB 时的 `SELECT`。
- 初始化/就绪：`SETNX`、`GET`。
- 脚本：`EVALSHA`、`EVAL`，及脚本内部 `TYPE TIME ZREMRANGEBYSCORE ZREVRANGE ZRANGE ZSCORE ZCARD ZADD ZREM PEXPIREAT PEXPIRE HGET HMGET HSET HDEL EXISTS DEL`。

ACL 必须同时允许脚本调用及其中的数据命令；不需要 SCRIPT FLUSH、FLUSHDB、FLUSHALL、CONFIG、ACL 管理、KEYS 或 SCAN。测试清理和策略变更用独立运维身份。不要给业务身份 `+@all`。部署前在专用环境验证最终 ACL 与客户端版本；认证/ACL/脚本错误都按协调不可用拒绝，不回退本地。

配置 `maxmemory-policy noeviction`，预留内存、连接和 CPU，监测 Redis 延迟/容量；不要通过驱逐策略 key 伪装恢复。Lua 提供原子隔离，**不是失败回滚**：写后出错可能保守占额，未确认的许可不用于分发上游，过期 score/TTL 负责后续回收。不要宣称 Redis OOM 有事务回滚保证。

## 策略一致性与变更

同 namespace 的摘要绑定协议版本、共享限额、熔断、租约/续租/操作超时，以及按 ID 排序的 Provider ID/format/endpoint 映射。不含凭据、监听地址、连接池大小和无关模型元数据。同 ID 必须代表相同上游配额资源；多个凭据是否属于同一账号额度，仍由部署方正确分组。增加 Provider、改 endpoint、限额或租约参数都是策略变更。

首次初始化仅 SETNX；不一致拒绝启动/就绪/新许可。策略 key 无自动 TTL，运行时丢失不会由活跃副本自动补建。普通密钥轮换且映射/策略不变无需修改摘要。

**不支持热变更或混合协议版本滚动升级。** 同协议策略变更流程：

1. 从负载均衡移除所有旧副本，排空请求并停止它们；确认没有旧副本、重连或在途协调命令。等待旧 TTL 加操作/网络余量，必要时等待更长的外部调用取消。
2. 在隔离预演环境用目标版本和相同策略生成新摘要，读取其精确 `policy` key。不要猜测/手改 JSON 摘要或借启动失败覆盖线上值。
3. 使用独立运维身份，读取并记录线上**精确**策略 key 的旧摘要，检查 request/upstream 两个 global ZSET 无有效 score。以下 CAS 同时再次检查；`OLD_DIGEST`/`NEW_DIGEST` 是 64 位十六进制摘要，不是密码：

```sh
# redis-cli 连接使用已验证 TLS、运维 ACL 和安全的认证输入；此处省略连接参数。
# 所有旧副本必须已停止，脚本检查不能代替排空。
redis-cli EVAL '
  if #ARGV[2] ~= 64 or not string.match(ARGV[2], "^%x+$") then return 0 end
  if redis.call("GET", KEYS[1]) ~= ARGV[1] then return 0 end
  local t = redis.call("TIME")
  local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)
  for i=2,3 do
    if redis.call("ZCOUNT", KEYS[i], "("..now, "+inf") ~= 0 then return 0 end
  end
  redis.call("SET", KEYS[1], ARGV[2]); return 1
' 3 'route-mind:{YOUR_NAMESPACE}:policy' \
    'route-mind:{YOUR_NAMESPACE}:request:global' \
    'route-mind:{YOUR_NAMESPACE}:upstream:global' "$OLD_DIGEST" "$NEW_DIGEST"
```

4. 只有返回 1 才启动新版本并检查 readyz。返回 0 或错误时停止操作、查明原因，不能删整个 namespace、清空 token 桶/熔断或 FLUSHDB。旧策略下的 token 数量不会被重置为新的 burst；降低 burst 时下次补充会按新上限截断。

协议不兼容升级改用新 namespace，但仍必须先把旧流量完全排空，不能让新旧池同时服务来放大总额度。策略 key 丢失/Redis 数据丢失需要人工确认并恢复；重启实例可能初始化缺失策略，因此事故期间先停流量，不能用不断重启代替一致性恢复。

## 租约与失败语义

- acquire/renew 使用 Redis TIME；许可有随机、不复用 ID，续租同时验证全局和 scope 成员存在且未过期，不能复活过期许可。释放仅删除自身 ID、有限时间内幂等。
- 本地 monotonic watchdog 从 RPC **开始**计时并预留 operation 余量；晚 ACK 不延长旧有效窗。独立 watchdog 不等续租协程返回才失效。拿到许可后执行 Before 检查点，发送 Provider 前再次检查；Before 成功后失效仍配对 After，不把已落库预算免费回滚。
- 正常结束停止续租，以独立短超时完成/释放；确认不明不重放模型。释放失败可能保守占额到 TTL，已发出的响应无法撤回。
- Redis 模式固定 **fail closed**。协调不可用、策略不一致、许可失效与容量/熔断拒绝不会触发 Agent RuleDecider fallback。用户取消与协调故障使用不同 cause；租约丢失时 Run 失败，执行中的工具结果仍保守标 outcome_unknown，后续步骤不再启动。
- Provider 5xx、网络和协议故障计健康失败；下游写失败、客户端取消、上游认证/限流、存储/协调故障为中性。冷却后全 namespace 只发一个有租约的半开探针；未分发归还探测权，已分发中性结果按冷却/Retry-After 再探测，不冒充成功。旧 epoch 的迟到成功不关闭新一轮熔断。
- 正文阻塞读与 SSE 阻塞写会被取消期限中断；后续 Write 不覆盖已取消 deadline。已经输出的流只错误结束或断开，无成功 terminator、无重播。旧 HTTP 连接的取消 hook 在返回前停止/等待，避免污染复用连接。
- token 桶保留至自然补满，而非随短 lease TTL 重置；活跃集合带 score 和最晚成员后的 key TTL，permit tombstone 有限保留。仅固定策略元数据和需要保留的未健康复位 Provider 状态长期存在。

## 观测与成本

`/healthz` 是本地存活；`/readyz` 同时检查 PostgreSQL schema、Redis 可用性及摘要；错误体不泄漏依赖详情。认证 `/metrics` 使用本地 admission，即使 Redis 故障也可读取，但仍受本进程硬容量/认证限额保护。

新增 `route_mind_coordination_*` 进程计数、操作耗时 sum、固定 reason 的拒绝数、lease lost 和 probe 数；没有 principal/lease ID/prompt 标签，也没有每副本重复的“集群总量”gauge。各副本 counter 可聚合。逻辑操作计数不等于精确 Redis 命令 QPS：每个成功 attempt 通常 acquire+complete 两个逻辑操作，外加每个活跃入口/上游许可每 renew interval 一次续租、就绪 GET、首次 SETNX 和 NOSCRIPT 的额外 EVAL。用 Redis 服务端监控测真实 QPS、延迟、脚本 CPU 和内存，不据本地 mock 虚构生产吞吐 SLA。

## 验证

```sh
# 显式专用依赖；不会回退 REDIS_URL / DATABASE_URL。
export TEST_REDIS_URL='redis://127.0.0.1:6379/0'
export TEST_DATABASE_URL='postgres://test_admin:YOUR_TEST_PASSWORD@127.0.0.1:5432/route_mind_test?sslmode=disable'
make coordination-integration  # Redis，无需 PostgreSQL
make integration-all           # PostgreSQL + Redis + HTTP 组合，race
# 先通过独立迁移身份对专用测试库 up；再使用运行身份：
make coordination-smoke
```

Go 集成测试创建随机 namespace/归属 marker，仅清理验证归属后的精确 key，禁止 FLUSHDB。双进程烟测仅接受 literal-loopback 明文 **测试 Redis**，用自有 TCP 代理模拟断连，不停止调用者提供的 Redis；仅终止自己启动的进程。测试包含跨副本 token、真实 mock peak、禁止决策绕过、原生 6 次调用、单半开探针、强杀网关 TTL 回收、长调用续租、断网 fail-closed/就绪/指标和恢复无重放。恢复就绪不等于孤儿许可立即消失，必须等待其真实过期。
