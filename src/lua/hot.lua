-- Redis-only state machine. All keys share one deployment hash slot.
-- Admission reserves journal capacity for its future dispatch/finalization events.
local function encode(v,field)
  local t=type(v)
  if t=='number' then
    if v==math.floor(v) then return string.format('%.0f',v) end
    return string.format('%.17g',v)
  end
  if t~='table' then return cjson.encode(v) end
  local array=#v>0 or field=='attempts' or field=='targets' or field=='capabilities'
  local out={}
  if array then for _,value in ipairs(v) do table.insert(out,encode(value)) end;return '['..table.concat(out,',')..']' end
  local names={};for key,_ in pairs(v) do table.insert(names,key) end;table.sort(names)
  for _,key in ipairs(names) do table.insert(out,cjson.encode(key)..':'..encode(v[key],key)) end
  return '{'..table.concat(out,',')..'}'
end
local types={'hash','hash','stream','string','zset','string','string','hash','hash'}
for i,key in ipairs(KEYS) do
  local typ=redis.call('TYPE',key).ok
  if typ~='none' and typ~=types[i] then return redis.error_reply('HOT_WRONGTYPE') end
end
local op=ARGV[1]
local epoch=redis.call('HGET',KEYS[1],'epoch')
local run=string.match(redis.call('INFO','server'),'run_id:([%w]+)')
if not epoch or epoch~=ARGV[2] or redis.call('HGET',KEYS[1],'run_id')~=run then return {1} end
if redis.call('TYPE',KEYS[3]).ok~='stream' then return {3} end
local streamInfo=redis.call('XINFO','STREAM',KEYS[3]);local tail=nil
for i=1,#streamInfo,2 do if streamInfo[i]=='last-generated-id' then tail=streamInfo[i+1] end end
if not tail or tail~=redis.call('HGET',KEYS[1],'journal_tail') then return {3} end
local slots=tonumber(redis.call('GET',KEYS[4]))
local rawProject=redis.call('GET',KEYS[6])
if not slots or not rawProject then return {3} end
local project=cjson.decode(rawProject)
if project.epoch~=epoch or type(project.budgets)~='table' or type(project.leases)~='table' then return {3} end
local raw=redis.call('GET',KEYS[7])
local data=raw and cjson.decode(raw) or nil
local input=cjson.decode(ARGV[4])
local time=redis.call('TIME');local now=tonumber(time[1])*1000+math.floor(tonumber(time[2])/1000)
local maxSafe=9007199254740991
local lease=tonumber(ARGV[6])
if data and data.epoch~=epoch then return {1} end
local pTokens,kTokens,pRate,kRate
if op=='admit' then
  if data then
    if data.projectId~=input.projectId or data.keyId~=input.keyId or data.period~=input.period or data.reserveMicros~=input.reserveMicros then return {11} end
    if data.state=='reserved' or data.state=='dispatched' then return {0,data.owner} end
    return {7}
  end
  local rev=redis.call('HGET',KEYS[2],'revision')
  if redis.call('HGET',KEYS[1],'projection_paused')=='1' then return {10} end
  if rev~=input.routeVersion.runtime_revision then return {2} end
  local auth=redis.call('HGET',KEYS[2],'key:'..input.routeVersion.runtime_key_hash)
  if not auth then return {2} end
  local key=cjson.decode(auth)
  if key.key_id~=input.keyId or (key.revoked_at and key.revoked_at~=cjson.null) then return {2} end
  local expires=input.routeVersion.runtime_expires_at
  if expires and expires~=cjson.null and now>=expires then return {15} end
  if redis.call('XLEN',KEYS[3])+slots+7>tonumber(ARGV[5]) then return {10} end
  local function bucket(key,rate)
    local b=redis.call('HMGET',key,'tokens','ts');local tokens=tonumber(b[1]) or rate*1000;local ts=tonumber(b[2]) or now
    return math.min(rate*1000,tokens+math.floor(math.min(86400000,math.max(0,now-ts))*rate/60))
  end
  pRate=input.rpm;kRate=input.keyRpm;pTokens=bucket(KEYS[8],pRate);kTokens=bucket(KEYS[9],kRate)
  if pTokens<1000 or kTokens<1000 then return {4,math.max(math.ceil(math.max(0,1000-pTokens)*60/pRate),math.ceil(math.max(0,1000-kTokens)*60/kRate))} end
  local count=0
  for id,untilTime in pairs(project.leases) do if untilTime>now then count=count+1 else project.leases[id]=nil end end
  if count>=input.concurrency then return {5} end
  local budget=project.budgets[input.period] or {spent=0,reserved=0}
  if budget.spent+budget.reserved+input.reserveMicros>maxSafe then return {8} end
  if input.budgetMicros>=0 and budget.spent+budget.reserved+input.reserveMicros>input.budgetMicros then return {6,math.max(0,input.budgetMicros-budget.spent-budget.reserved)} end
  budget.reserved=budget.reserved+input.reserveMicros;project.budgets[input.period]=budget
  data={version=1,epoch=epoch,revision=1,requestId=ARGV[3],projectId=input.projectId,keyId=input.keyId,
    period=input.period,reserveMicros=input.reserveMicros,routeVersion=input.routeVersion,priceSnapshot=input.priceSnapshot,
    state='reserved',status='reserved',actualMicros=cjson.null,chargedMicros=cjson.null,inputTokens=cjson.null,outputTokens=cjson.null,
    aliasName=input.routeVersion.alias_name or cjson.null,errorCode=cjson.null,attempts={},createdAt=input.at,updatedAt=input.at,
    owner=input.owner,leaseUntil=now+lease,remainingEvents=6}
  slots=slots+6;project.leases[ARGV[3]]=now+lease
else
  if not data then return {9} end
  local budget=project.budgets[data.period]
  if not budget then return {3} end
  if op=='heartbeat' then
    if data.state~='reserved' and data.state~='dispatched' then return {13} end
    if data.owner~=input.owner then return {13} end
    if data.leaseUntil<=now then return {13} end
    data.leaseUntil=now+lease;project.leases[ARGV[3]]=now+lease
    redis.call('SET',KEYS[6],encode(project));redis.call('SET',KEYS[7],encode(data))
    redis.call('ZADD',KEYS[5],data.leaseUntil,ARGV[3]);return {0}
  end
  if op=='expire' and (data.state~='reserved' and data.state~='dispatched' or data.leaseUntil>now) then return {0} end
  if op=='dispatch' then
    if data.state=='dispatched' then return {0} end
    if data.state~='reserved' then return {12} end
    if data.leaseUntil<=now then return {13} end
    data.state='dispatched';data.status='dispatched';data.leaseUntil=now+lease;project.leases[ARGV[3]]=now+lease
  elseif op=='metadata' then
    if data.state=='cancelled' or data.state=='settled' then return {0} end
    -- Keep the last ticket of an unknown charge for its eventual actual correction.
    if data.state=='unknown' and data.remainingEvents<=1 then return {0} end
    local prior=encode({attempts=data.attempts,aliasName=data.aliasName,errorCode=data.errorCode})
    local attempts={};for _,a in ipairs(data.attempts) do attempts[a.attempt_no]=a end
    for _,a in ipairs(input.attempts) do attempts[a.attempt_no]=a end
    local merged={};for _,a in pairs(attempts) do table.insert(merged,a) end;table.sort(merged,function(a,b)return a.attempt_no<b.attempt_no end)
    data.attempts=merged;data.aliasName=input.aliasName;data.errorCode=input.errorCode
    if prior==encode({attempts=data.attempts,aliasName=data.aliasName,errorCode=data.errorCode}) then return {0} end
  elseif op=='settle' or op=='cancel' or op=='expire' then
    local expireDispatched=op=='expire' and data.state=='dispatched'
    local cancel=op=='cancel' or op=='expire' and not expireDispatched
    if cancel then
      if data.state=='cancelled' then return {0} end
      if data.state=='dispatched' then return {14} end
      if data.state~='reserved' then return {7} end
      data.state='cancelled';data.status='cancelled';data.chargedMicros=0
      budget.reserved=budget.reserved-data.reserveMicros
    else
      if data.state=='cancelled' then return {7} end
      local actual=expireDispatched and cjson.null or input.actualMicros
      if data.state=='settled' then
        if actual==cjson.null or actual==data.actualMicros then return {0} end
        return {11}
      end
      if data.state=='unknown' and actual==cjson.null then return {0} end
      local charge=actual==cjson.null and data.reserveMicros or actual
      local previous=data.state=='unknown' and data.chargedMicros or 0
      -- Subtract first: spent + charge can exceed 2^53 even when the net
      -- correction is valid. Bound the final addition before performing it.
      local adjustment=charge-previous
      if adjustment>0 and budget.spent>maxSafe-adjustment or adjustment<0 and budget.spent < -adjustment then return {8} end
      local spent=budget.spent+adjustment
      if data.state~='unknown' then budget.reserved=budget.reserved-data.reserveMicros end
      budget.spent=spent
      data.actualMicros=actual;data.chargedMicros=charge;data.state=actual==cjson.null and 'unknown' or 'settled'
      data.status=expireDispatched and 'unknown' or input.status
      if not expireDispatched then
        data.inputTokens=input.inputTokens;data.outputTokens=input.outputTokens;data.errorCode=input.errorCode
        if #input.attempts>0 then data.attempts=input.attempts end
      end
    end
    if budget.reserved<0 then return {3} end
    project.leases[ARGV[3]]=nil
  else return {8} end
  if data.remainingEvents<1 or slots<1 then return {10} end
  slots=slots-1;data.remainingEvents=data.remainingEvents-1
  if data.state=='settled' or data.state=='cancelled' then slots=slots-data.remainingEvents;data.remainingEvents=0
  elseif data.state=='unknown' and data.remainingEvents>1 then slots=slots-(data.remainingEvents-1);data.remainingEvents=1 end
  data.revision=data.revision+1;data.updatedAt=input.at
end
if slots<0 then return {3} end
-- Types, bounds and capacity have been checked before the first mutation.
-- The journal and the state transition are a single Redis command, never fire-and-forget.
local json=encode(data)
local appended=redis.call('XADD',KEYS[3],'*','data',json)
redis.call('HSET',KEYS[1],'journal_tail',appended)
redis.call('SET',KEYS[6],encode(project))
redis.call('SET',KEYS[7],json)
redis.call('SET',KEYS[4],string.format('%.0f',slots))
if data.state=='reserved' or data.state=='dispatched' then redis.call('ZADD',KEYS[5],data.leaseUntil,ARGV[3])
else redis.call('ZREM',KEYS[5],ARGV[3]);if data.state~='unknown' then redis.call('EXPIRE',KEYS[7],604800) end end
if op=='admit' then
  redis.call('HSET',KEYS[8],'tokens',pTokens-1000,'ts',now)
  redis.call('HSET',KEYS[9],'tokens',kTokens-1000,'ts',now)
end
return {0,data.owner}
