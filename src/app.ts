import Fastify from 'fastify';
import cookie from '@fastify/cookie';
import staticFiles from '@fastify/static';
import { Pool } from 'pg';
import { Redis } from 'ioredis';
import { fileURLToPath } from 'node:url';
import { migrate } from './db.js';
import { HotBilling } from './hot-billing.js';
import { HotRuntime } from './hot-runtime.js';
import { HotProjector } from './hot-projector.js';
import { hotKeys } from './hot-contract.js';
import { registerControl } from './control.js';
import { registerGateway } from './gateway.js';
import { registerQueries } from './queries.js';
import { registerProviders } from './providers.js';
import { registerCatalog } from './catalog.js';
import { registerIdentity } from './identity.js';
import { EventWorker } from './events.js';
import { authenticateSession, requireProject } from './control.js';
import { hashKey } from './security.js';
import { randomBytes, randomUUID } from 'node:crypto';
import { loadConfig } from './config.js';

export async function buildApp(config: ReturnType<typeof loadConfig>, logging = true, options:{backgroundWorkers?:boolean}={}) {
  const pool = new Pool({ connectionString: config.DATABASE_URL, max: 12, connectionTimeoutMillis: 5000, statement_timeout: 10000 });
  const backgroundPool=new Pool({connectionString:config.DATABASE_URL,max:4,connectionTimeoutMillis:2000,statement_timeout:10000});
  // An idle PG socket can fail independently of a query. It must not crash the
  // Redis data plane; background operations report failures and retain the journal.
  pool.on('error',()=>{});backgroundPool.on('error',()=>{});
  const redis = new Redis(config.REDIS_URL, {maxRetriesPerRequest: 1, connectTimeout: 5000, commandTimeout:5000, enableOfflineQueue: false});
  redis.on('error', () => {});
  try {
  if (redis.status !== 'ready') await new Promise<void>((resolve, reject) => {
    redis.once('ready', resolve); redis.once('error', reject);
  });
  await migrate(pool);
  const keys=hotKeys(config.DATA_PLANE_NAMESPACE);
  const runtime=new HotRuntime(backgroundPool,redis,keys,config.DATA_PLANE_NAMESPACE,config.HOT_DURABILITY);
  await runtime.initialize();
  const billing = new HotBilling(redis,keys,runtime.epoch,config.HOT_JOURNAL_CAPACITY);
  await billing.initialize();
  const app = Fastify({logger: logging ? {redact:['req.headers.authorization', 'req.headers.cookie', 'res.headers.set-cookie']} : false, bodyLimit: 1024 * 1024, requestTimeout: 130000});
  app.setErrorHandler((error: any, _req, reply) => {
    const status = Number(error.statusCode) || 500;
    if (status >= 500) app.log.error({code: error.code, name: error.name}, 'Request failed');
    reply.code(status).send({error:{code: error.code || (status >= 500 ? 'internal_error' : 'invalid_request'), message:status >= 500 ? '服务暂时不可用，请稍后重试。' : error.message}});
  });
  app.addHook('onSend', async (_req, reply, payload) => {
    reply.header('X-Content-Type-Options','nosniff');
    reply.header('Referrer-Policy','same-origin');
    reply.header('X-Frame-Options','DENY');
    reply.header('Cache-Control','no-store');
    reply.header('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'");
    return payload;
  });
  await app.register(cookie);
  app.addHook('onRequest',async(req,reply)=>{
    const path=req.url.split('?')[0];
    if(req.method==='POST'&&['/api/login','/api/bootstrap','/api/invitations/accept'].includes(path)) {
      const n=Number(await redis.eval("local n=redis.call('INCR',KEYS[1]);if n==1 then redis.call('EXPIRE',KEYS[1],60) end;return n",1,`auth:${hashKey(req.ip)}:${path}`));
      if(n>20){reply.header('Retry-After','60');throw Object.assign(Error('登录尝试过多，请稍后重试。'),{statusCode:429,code:'auth_rate_limit'});}
    }
  });
  // A successful control mutation is not acknowledged until its Redis publication succeeds.
  app.addHook('onSend',async(req,reply,payload)=>{
    const path=req.url.split('?')[0];
    if(!['GET','HEAD','OPTIONS'].includes(req.method)&&path.startsWith('/api/')&&path!=='/api/test'&&reply.statusCode<400)
      await runtime.publish();
    return payload;
  });
  const dataPlane={authenticateKey:runtime.authenticateKey.bind(runtime),loadRoute:runtime.loadRoute.bind(runtime),models:runtime.models.bind(runtime),persistMetadata:billing.persistMetadata.bind(billing)};
  const deps = {pool,billing,dataPlane,encryptionKey:config.ENCRYPTION_KEY,allowPrivateUpstream:config.ALLOW_PRIVATE_UPSTREAM==='true'};
  await registerControl(app,deps);
  await registerProviders(app,deps);
  await registerCatalog(app,deps);
  await registerIdentity(app,deps);
  await registerGateway(app,deps);
  await registerQueries(app,{...deps,hotState:{redis,keys}});
  app.post('/api/test',async(req,reply)=>{
    const body=req.body as any;
    const user=await authenticateSession(req,pool);
    const projectId=body?.project_id||body?.projectId;
    await requireProject(req,pool,projectId,true);
    if(typeof body.model!=='string')throw Object.assign(Error('请选择模型。'),{statusCode:400});
    const secret='gw_test_'+randomBytes(32).toString('base64url'), id=randomUUID();
    await pool.query(`INSERT INTO api_keys(id,project_id,name,key_hash,prefix,created_by,allowed_models,expires_at) VALUES($1,$2,'Console test',$3,'gw_test',$4,$5,now()+interval '1 minute')`,[id,projectId,hashKey(secret),user.userId,JSON.stringify([body.model])]);
    try {
      await runtime.publish();
      const response=await app.inject({method:'POST',url:'/v1/chat/completions',headers:{authorization:`Bearer ${secret}`},payload:{model:body.model,messages:body.messages||[{role:'user',content:body.prompt||'你好'}],max_tokens:body.max_tokens||64,stream:false}});
      if(response.headers['x-request-id']) reply.header('x-request-id',response.headers['x-request-id']);
      let result:unknown;try{result={...JSON.parse(response.body),request_id:response.headers['x-request-id']};}catch{result={error:{message:'上游返回格式错误'}};}
      return reply.code(response.statusCode).type('application/json').send(result);
    }finally{await pool.query('UPDATE api_keys SET revoked_at=now() WHERE id=$1',[id]);await runtime.publish();}
  });
  app.get('/health/live', async () => ({status:'ok'}));
  app.get('/health/ready', async (_req, reply) => {
    try { if (!await billing.ready()) throw Error('Billing unavailable'); return {status:'ready'}; }
    catch { return reply.code(503).send({status:'unavailable'}); }
  });
  await app.register(staticFiles,{root:fileURLToPath(new URL('../web/',import.meta.url)),prefix:'/'});
  const events=new EventWorker(backgroundPool,config.KAFKA_BROKERS.split(',').filter(Boolean));await events.start();
  const projector=new HotProjector(backgroundPool,redis,keys,runtime.epoch);
  let processing:Promise<void>|null=null,publishing:Promise<void>|null=null;
  const eventTimer=options.backgroundWorkers===false?undefined:setInterval(()=>{
    if(processing)return;
    processing=(async()=>{
      const full=await backgroundPool.query("/*background:outbox-capacity*/ SELECT 1 FROM outbox WHERE status!='published' OFFSET 9999 LIMIT 1");
      await redis.hset(keys.runtime,'projection_paused',full.rowCount?'1':'0');
      if(!full.rowCount)await projector.tick();
      await events.tick();
      // A changed Redis identity fences new work, but retained events can still
      // be committed while an operator verifies the recovery source.
      await billing.reap();
    })().catch(()=>app.log.error('Background projection failed; uncommitted journal retained')).finally(()=>{processing=null;});
  },100);eventTimer?.unref();
  const publishTimer=options.backgroundWorkers===false?undefined:setInterval(()=>{
    if(publishing)return;publishing=runtime.publish().catch(()=>app.log.error('Runtime publication failed')).finally(()=>{publishing=null;});
  },10000);publishTimer?.unref();
  app.addHook('onClose', async () => {
    clearInterval(eventTimer);clearInterval(publishTimer);
    await Promise.allSettled([processing,publishing].filter(Boolean));
    await billing.close();await projector.close();await events.close();
    await redis.quit().catch(()=>redis.disconnect());await backgroundPool.end();await pool.end();
  });
  return {app,pool,redis,billing,runtime,projector,backgroundPool};
  }catch(error){await redis.quit().catch(()=>redis.disconnect());await Promise.allSettled([pool.end(),backgroundPool.end()]);throw error;}
}
