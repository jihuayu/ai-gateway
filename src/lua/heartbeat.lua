-- Renew a concurrency lease. Owner and generation are fenced.
-- KEYS: gen, request, concurrency
-- ARGV: generation, requestId, owner, leaseMs, concurrency
local gen = redis.call('GET', KEYS[1])
if not gen or gen ~= ARGV[1] then return {1} end
local state = redis.call('HGET', KEYS[2], 'state')
if not state then return {4} end
if state ~= 'reserved' and state ~= 'dispatched' then return {3} end
local owner = redis.call('HGET', KEYS[2], 'owner')
if not owner or owner ~= ARGV[3] then return {5} end
local leaseMs = tonumber(ARGV[4])
local concurrency = tonumber(ARGV[5])
if not concurrency or concurrency < 1 then concurrency = tonumber(redis.call('HGET', KEYS[2], 'concurrency')) end
if not leaseMs or not concurrency or leaseMs < 1 or concurrency < 1 then return {6} end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', now)
local score = redis.call('ZSCORE', KEYS[3], ARGV[2])
if not score and redis.call('ZCARD', KEYS[3]) >= concurrency then return {6} end
redis.call('ZADD', KEYS[3], now + leaseMs, ARGV[2])
return {0}
