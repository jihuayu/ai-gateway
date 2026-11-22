-- Verify the actual server identity atomically with admission (not only a cached client INFO).
local actualRunId = string.match(redis.call('INFO','server'),'run_id:([%w]+)')
if ARGV[12] and actualRunId ~= ARGV[12] then return {2} end
-- Atomic admission: generation fence, project+key token buckets, concurrency, budget reservation.
-- KEYS: gen, paused, project bucket, key bucket, concurrency zset, reservations hash, budget hash, request hash, request set, period set
-- ARGV: generation, requestId, keyId, owner, reserveMicros, rpm, keyRpm, concurrency, budgetMicros, leaseMs, period
local gen = redis.call('GET', KEYS[1])
if not gen then return {2} end
if gen ~= ARGV[1] then return {2} end
if redis.call('EXISTS', KEYS[2]) == 1 then return {3} end

local existing = redis.call('HMGET', KEYS[8], 'state', 'amount', 'period')
if existing[1] then
  if existing[1] == 'reserved' or existing[1] == 'dispatched' then return {1} end
  return {7, existing[1]}
end

local requestId = ARGV[2]
local reserve = tonumber(ARGV[5])
local rpm = tonumber(ARGV[6])
local keyRpm = tonumber(ARGV[7])
local concurrency = tonumber(ARGV[8])
local budget = tonumber(ARGV[9])
local leaseMs = tonumber(ARGV[10])
local period = ARGV[11]
if not reserve or not rpm or not keyRpm or not concurrency or not budget or not leaseMs then return {8} end
if reserve < 0 or rpm < 1 or keyRpm < 1 or concurrency < 1 or leaseMs < 1 then return {8} end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local function bucket(key, rate)
  local d = redis.call('HMGET', key, 'tokens', 'ts')
  local cap = rate * 1000
  local tokens = tonumber(d[1])
  local ts = tonumber(d[2])
  if not tokens or not ts then tokens = cap; ts = now end
  local elapsed = now - ts
  if elapsed < 0 then elapsed = 0 end
  if elapsed > 86400000 then elapsed = 86400000 end
  tokens = tokens + math.floor(elapsed * rate / 60)
  if tokens > cap then tokens = cap end
  if tokens < 0 then tokens = 0 end
  return tokens, ts
end

local pTokens, pTs = bucket(KEYS[3], rpm)
local kTokens, kTs = bucket(KEYS[4], keyRpm)
if pTokens < 1000 or kTokens < 1000 then
  redis.call('HSET', KEYS[3], 'tokens', pTokens, 'ts', now)
  redis.call('HSET', KEYS[4], 'tokens', kTokens, 'ts', now)
  local retry = 0
  if pTokens < 1000 then
    local ms = math.ceil((1000 - pTokens) * 60 / rpm)
    if ms > retry then retry = ms end
  end
  if kTokens < 1000 then
    local ms = math.ceil((1000 - kTokens) * 60 / keyRpm)
    if ms > retry then retry = ms end
  end
  return {4, retry}
end

redis.call('ZREMRANGEBYSCORE', KEYS[5], '-inf', now)
if redis.call('ZCARD', KEYS[5]) >= concurrency then return {5} end

if redis.call('SISMEMBER',KEYS[10],period)==1 and redis.call('EXISTS',KEYS[7])==0 then return {2} end
local b = redis.call('HMGET', KEYS[7], 'spent', 'reserved', 'limit')
local spent = tonumber(b[1]) or 0
local reserved = tonumber(b[2]) or 0
local limit = tonumber(b[3]) or -1
if budget >= 0 then
  if spent + reserved + reserve > budget then
    local available = budget - spent - reserved
    if available < 0 then available = 0 end
    return {6, available}
  end
  limit = budget
end
if spent + reserved + reserve > 9007199254740991 then return {8} end

redis.call('HSET', KEYS[3], 'tokens', pTokens - 1000, 'ts', now)
redis.call('HSET', KEYS[4], 'tokens', kTokens - 1000, 'ts', now)
redis.call('ZADD', KEYS[5], now + leaseMs, requestId)
redis.call('HSET', KEYS[6], requestId, reserve)
redis.call('HSET', KEYS[7], 'spent', spent, 'reserved', reserved + reserve, 'limit', limit)
redis.call('HSET', KEYS[8], 'state', 'reserved', 'amount', reserve, 'period', period, 'key', ARGV[3],
  'owner', ARGV[4], 'generation', ARGV[1], 'dispatched', '0', 'concurrency', concurrency, 'created', now)
redis.call('PEXPIRE', KEYS[8], 604800000)
redis.call('SADD', KEYS[9], requestId)
redis.call('SADD', KEYS[10], period)
return {0, 'new'}
