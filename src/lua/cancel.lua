-- Release an un-dispatched reservation. Never refunds consumed rate tokens.
-- KEYS: gen, request, concurrency, reservations, budget, request set
-- ARGV: generation, requestId
local gen = redis.call('GET', KEYS[1])
if not gen or gen ~= ARGV[1] then return {1} end
local state = redis.call('HGET', KEYS[2], 'state')
if not state then return {4} end
if state == 'settled' or state == 'unknown' or state == 'cancelled' then return {2} end
if state == 'dispatched' or redis.call('HGET', KEYS[2], 'dispatched') == '1' then return {3} end
local amount = tonumber(redis.call('HGET', KEYS[2], 'amount')) or 0
local reserved = tonumber(redis.call('HGET', KEYS[5], 'reserved')) or 0
reserved = reserved - amount
if reserved < 0 then reserved = 0 end
redis.call('HSET', KEYS[5], 'reserved', reserved)
redis.call('ZREM', KEYS[3], ARGV[2])
redis.call('HDEL', KEYS[4], ARGV[2])
redis.call('HSET', KEYS[2], 'state', 'cancelled', 'cancelled_at', redis.call('TIME')[1])
redis.call('SREM', KEYS[6], ARGV[2])
return {0}
