-- Coordination protocol v1. All keys share an operator-owned hash tag.
-- Atomic isolation is NOT rollback: check types/policy before any write and
-- grant only after every required write has succeeded. Unknown errors fail shut.
local types = {'string', 'zset', 'zset', 'hash', 'hash'}
for i, wanted in ipairs(types) do
  local actual = redis.call('TYPE', KEYS[i]).ok
  if actual ~= 'none' and actual ~= wanted then return {'invalid', 0} end
end
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return {'policy', 0} end
local action, kind, id = ARGV[2], ARGV[3], ARGV[4]
local ttl, global_limit, scope_limit = tonumber(ARGV[5]), tonumber(ARGV[6]), tonumber(ARGV[7])
local rate, burst, threshold, cooldown = tonumber(ARGV[8]), tonumber(ARGV[9]), tonumber(ARGV[10]), tonumber(ARGV[11])
local outcome, dispatched, retry, new_epoch = ARGV[12], ARGV[13], tonumber(ARGV[14]), ARGV[15]
if (action ~= 'acquire' and action ~= 'renew' and action ~= 'complete') or
   (kind ~= 'request' and kind ~= 'upstream') or not ttl or ttl < 1 or
   not global_limit or not scope_limit or not rate or rate < 1 or not burst or
   not threshold or not cooldown or not retry or #id ~= 32 or #new_epoch ~= 32 or
   (outcome ~= 'success' and outcome ~= 'provider_failure' and outcome ~= 'neutral') then
  return {'invalid', 0}
end
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local expires = now + ttl

local function trim(key)
  redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
end
local function touch(key)
  local last = redis.call('ZREVRANGE', key, 0, 0, 'WITHSCORES')
  if #last > 0 then redis.call('PEXPIREAT', key, math.ceil(tonumber(last[2]) + ttl)) end
end
local function remaining(key)
  local first = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  if #first > 0 then return math.max(1, tonumber(first[2]) - now) end
  return 1
end
trim(KEYS[2]); trim(KEYS[3])
local state = redis.call('HGET', KEYS[5], 'state')
local function active()
  if state ~= 'active' or redis.call('HGET', KEYS[5], 'scope') ~= KEYS[3] then return false end
  local a = tonumber(redis.call('ZSCORE', KEYS[2], id) or '0')
  local b = tonumber(redis.call('ZSCORE', KEYS[3], id) or '0')
  return a > now and b > now
end
local function tombstone()
  redis.call('HSET', KEYS[5], 'state', 'done')
  redis.call('PEXPIRE', KEYS[5], ttl * 2)
end
local function release()
  redis.call('ZREM', KEYS[2], id); redis.call('ZREM', KEYS[3], id)
  tombstone(); touch(KEYS[2]); touch(KEYS[3])
end
local function clean_circuit()
  -- Never discard unhealthy history or reset epochs underneath active calls.
  if redis.call('ZCARD', KEYS[3]) == 0 and
     redis.call('HGET', KEYS[4], 'state') == 'closed' and
     tonumber(redis.call('HGET', KEYS[4], 'failures') or '0') == 0 then
    redis.call('DEL', KEYS[4])
  end
end

if action == 'acquire' then
  if state then
    if active() then return {'ok', redis.call('HGET', KEYS[5], 'expires'), 'existing'} end
    return {'lost', 0}
  end
  local circuit, epoch, failures, probe = 'closed', '', 0, false
  if kind == 'upstream' and threshold > 0 then
    circuit = redis.call('HGET', KEYS[4], 'state') or 'closed'
    epoch = redis.call('HGET', KEYS[4], 'epoch') or new_epoch
    failures = tonumber(redis.call('HGET', KEYS[4], 'failures') or '0')
    if circuit == 'open' then
      local until_at = tonumber(redis.call('HGET', KEYS[4], 'until') or '0')
      if until_at > now then return {'open', until_at - now} end
      probe = true
    elseif circuit == 'half_open' then
      local probe_id = redis.call('HGET', KEYS[4], 'probe') or ''
      local probe_until = tonumber(redis.call('ZSCORE', KEYS[3], probe_id) or '0')
      if probe_until > now then return {'open', probe_until - now} end
      probe = true
    elseif circuit ~= 'closed' then return {'invalid', 0} end
  end
  local tokens = burst
  if kind == 'request' then
    local old = redis.call('HMGET', KEYS[4], 'tokens', 'at')
    if old[1] then
      if not tonumber(old[1]) or not tonumber(old[2]) then return {'invalid', 0} end
      tokens = math.min(burst, tonumber(old[1]) + math.max(0, now - tonumber(old[2])) * rate / 60000)
    end
    if tokens < 1 then return {'rate', math.ceil((1 - tokens) * 60000 / rate)} end
  end
  if redis.call('ZCARD', KEYS[3]) >= scope_limit then
    if kind == 'request' then return {'rate', remaining(KEYS[3])} end
    return {'capacity', remaining(KEYS[3])}
  end
  if redis.call('ZCARD', KEYS[2]) >= global_limit then return {'capacity', remaining(KEYS[2])} end
  -- All business rejection paths precede allocation and token consumption.
  redis.call('ZADD', KEYS[2], expires, id)
  redis.call('ZADD', KEYS[3], expires, id)
  touch(KEYS[2]); touch(KEYS[3])
  if kind == 'request' then
    redis.call('HSET', KEYS[4], 'tokens', tokens - 1, 'at', now)
    -- A short lease TTL must never replenish a long-lived token bucket.
    redis.call('PEXPIRE', KEYS[4], math.ceil((burst - tokens + 1) * 60000 / rate) + 1000)
  elseif threshold > 0 then
    if probe then circuit = 'half_open'; epoch = new_epoch end
    redis.call('HSET', KEYS[4], 'state', circuit, 'epoch', epoch, 'failures', failures)
    if probe then redis.call('HSET', KEYS[4], 'probe', id) end
  end
  redis.call('HSET', KEYS[5], 'state', 'active', 'expires', expires, 'scope', KEYS[3], 'epoch', epoch)
  redis.call('PEXPIRE', KEYS[5], ttl * 2)
  if probe then return {'ok', expires, 'probe'} end
  return {'ok', expires, 'normal'}
end

if action == 'renew' then
  if not active() then return {'lost', 0} end
  if kind == 'upstream' and threshold > 0 and redis.call('EXISTS', KEYS[4]) == 0 then return {'lost', 0} end
  redis.call('ZADD', KEYS[2], expires, id); redis.call('ZADD', KEYS[3], expires, id)
  touch(KEYS[2]); touch(KEYS[3])
  redis.call('HSET', KEYS[5], 'expires', expires)
  redis.call('PEXPIRE', KEYS[5], ttl * 2)
  return {'ok', expires}
end

if action == 'complete' then
  if state == 'done' then return {'ok', 0} end
  if not active() then
    if state == 'active' then release() end
    return {'lost', 0}
  end
  if kind == 'upstream' and threshold > 0 then
    local epoch = redis.call('HGET', KEYS[5], 'epoch')
    if epoch == redis.call('HGET', KEYS[4], 'epoch') then
      local circuit = redis.call('HGET', KEYS[4], 'state')
      if outcome == 'success' and dispatched == '1' then
        -- In closed state concurrent successes share an epoch until a transition.
        local next_epoch = new_epoch
        if circuit == 'closed' then next_epoch = epoch end
        redis.call('HSET', KEYS[4], 'state', 'closed', 'failures', 0, 'epoch', next_epoch)
        redis.call('HDEL', KEYS[4], 'probe', 'until')
      elseif outcome == 'provider_failure' and dispatched == '1' then
        local failures = tonumber(redis.call('HGET', KEYS[4], 'failures') or '0') + 1
        redis.call('HSET', KEYS[4], 'failures', failures)
        if circuit == 'half_open' or failures >= threshold then
          redis.call('HSET', KEYS[4], 'state', 'open', 'until', now + cooldown, 'epoch', new_epoch)
          redis.call('HDEL', KEYS[4], 'probe')
        end
      elseif circuit == 'half_open' then
        -- No successful health observation: release the probe without declaring
        -- recovery. An undispatched probe can be replaced immediately.
        local delay = 0
        if dispatched == '1' then delay = math.max(cooldown, retry) end
        redis.call('HSET', KEYS[4], 'state', 'open', 'until', now + delay, 'epoch', new_epoch)
        redis.call('HDEL', KEYS[4], 'probe')
      end
    end
  end
  release()
  if kind == 'upstream' and threshold > 0 then clean_circuit() end
  return {'ok', 0}
end
return {'invalid', 0}
