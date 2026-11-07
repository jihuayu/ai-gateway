-- Finalize a reservation atomically. Idempotent per request; generation fenced.
-- KEYS: gen, request, concurrency, reservations, budget, request set
-- ARGV: generation, requestId, chargeMicros, usageState
local gen = redis.call('GET', KEYS[1])
if not gen or gen ~= ARGV[1] then return {1} end
local state = redis.call('HGET', KEYS[2], 'state')
if not state then return {4} end
if state == 'settled' or state == 'unknown' or state == 'cancelled' then return {2} end
local charge = tonumber(ARGV[3])
if not charge or charge < 0 or charge > 9007199254740991 then return {8} end
local amount = tonumber(redis.call('HGET', KEYS[2], 'amount')) or 0
local b = redis.call('HMGET', KEYS[5], 'spent', 'reserved', 'limit')
local spent = tonumber(b[1]) or 0
local reserved = tonumber(b[2]) or 0
local limit = tonumber(b[3]) or -1
if spent + charge > 9007199254740991 then return {8} end
reserved = reserved - amount
if reserved < 0 then reserved = 0 end
local overage = charge - amount
if overage < 0 then overage = 0 end
redis.call('HSET', KEYS[5], 'spent', spent + charge, 'reserved', reserved)
if limit >= 0 and spent + charge > limit then
  redis.call('HSET', KEYS[5], 'over_limit', '1', 'limit', limit)
end
redis.call('ZREM', KEYS[3], ARGV[2])
redis.call('HDEL', KEYS[4], ARGV[2])
local finalState = 'settled'
if ARGV[4] == 'unknown' then finalState = 'unknown' end
redis.call('HSET', KEYS[2], 'state', finalState, 'settled', charge, 'usage', ARGV[4],
  'settled_at', redis.call('TIME')[1])
redis.call('SREM', KEYS[6], ARGV[2])
return {0, amount, spent + charge, reserved, overage}
