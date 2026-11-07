-- Mark durable dispatch intent in Redis; idempotent and generation-fenced.
-- KEYS: gen, paused, request, concurrency
-- ARGV: generation, requestId, leaseMs
local gen = redis.call('GET', KEYS[1])
if not gen or gen ~= ARGV[1] then return {1} end
if redis.call('EXISTS', KEYS[2]) == 1 then return {2} end
local state = redis.call('HGET', KEYS[3], 'state')
if not state then return {4} end
if state ~= 'reserved' and state ~= 'dispatched' then return {3} end
local leaseMs = tonumber(ARGV[3])
if not leaseMs or leaseMs < 1 then return {5} end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
if state == 'reserved' then
  redis.call('HSET', KEYS[3], 'state', 'dispatched', 'dispatched', '1', 'dispatched_at', now)
end
redis.call('ZADD', KEYS[4], now + leaseMs, ARGV[2])
return {0}
