import Fastify from 'fastify';
import cookie from '@fastify/cookie';
import staticFiles from '@fastify/static';
import { Pool } from 'pg';
import { Redis } from 'ioredis';
import { fileURLToPath } from 'node:url';
import { migrate } from './db.js';
import { Billing } from './billing.js';
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

export async function buildApp(config: ReturnType<typeof loadConfig>, logging = true) {
  const pool = new Pool({ connectionString: config.DATABASE_URL, max: 12, connectionTimeoutMillis: 5000, statement_timeout: 10000 });
  const redis = new Redis(config.REDIS_URL, {maxRetriesPerRequest: 1, connectTimeout: 5000, enableOfflineQueue: false});
  redis.on('error', () => {});
  if (redis.status !== 'ready') await new Promise<void>((resolve, reject) => {
    redis.once('ready', resolve); redis.once('error', reject);
  });
  await migrate(pool);
  const billing = new Billing(pool, redis);
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
    if(req.method==='POST'&&path==='/v1/chat/completions'){
      const full=await pool.query("SELECT 1 FROM outbox WHERE status!='published' OFFSET 9999 LIMIT 1");
      if(full.rowCount)throw Object.assign(Error('计量事件积压，请稍后重试。'),{statusCode:503,code:'event_backlog'});
    }
  });
  const deps = {pool,billing,encryptionKey:config.ENCRYPTION_KEY,allowPrivateUpstream:config.ALLOW_PRIVATE_UPSTREAM==='true'};
  await registerControl(app,deps);
  await registerProviders(app,deps);
  await registerCatalog(app,deps);
  await registerIdentity(app,deps);
  await registerGateway(app,deps);
  await registerQueries(app,deps);
  app.post('/api/test',async(req,reply)=>{
    const body=req.body as any;
    const user=await authenticateSession(req,pool);
    const projectId=body?.project_id||body?.projectId;
    await requireProject(req,pool,projectId,true);
    if(typeof body.model!=='string')throw Object.assign(Error('请选择模型。'),{statusCode:400});
    const secret='gw_test_'+randomBytes(32).toString('base64url'), id=randomUUID();
    await pool.query(`INSERT INTO api_keys(id,project_id,name,key_hash,prefix,created_by,allowed_models,expires_at) VALUES($1,$2,'Console test',$3,'gw_test',$4,$5,now()+interval '1 minute')`,[id,projectId,hashKey(secret),user.userId,JSON.stringify([body.model])]);
    try {
      const response=await app.inject({method:'POST',url:'/v1/chat/completions',headers:{authorization:`Bearer ${secret}`},payload:{model:body.model,messages:body.messages||[{role:'user',content:body.prompt||'你好'}],max_tokens:body.max_tokens||64,stream:false}});
      if(response.headers['x-request-id']) reply.header('x-request-id',response.headers['x-request-id']);
      let result:unknown;try{result={...JSON.parse(response.body),request_id:response.headers['x-request-id']};}catch{result={error:{message:'上游返回格式错误'}};}
      return reply.code(response.statusCode).type('application/json').send(result);
    }finally{await pool.query('UPDATE api_keys SET revoked_at=now() WHERE id=$1',[id]);}
  });
  app.get('/health/live', async () => ({status:'ok'}));
  app.get('/health/ready', async (_req, reply) => {
    try { await pool.query('SELECT 1'); if (!await billing.ready()) throw Error('Billing unavailable'); return {status:'ready'}; }
    catch { return reply.code(503).send({status:'unavailable'}); }
  });
  await app.register(staticFiles,{root:fileURLToPath(new URL('../web/',import.meta.url)),prefix:'/'});
  const events=new EventWorker(pool,config.KAFKA_BROKERS.split(',').filter(Boolean));await events.start();
  let processing=false;
  const eventTimer=setInterval(()=>{if(processing)return;processing=true;void events.tick().catch(()=>app.log.error('Event delivery failed')).finally(()=>{processing=false;});},500);eventTimer.unref();
  app.addHook('onClose', async () => {clearInterval(eventTimer);await billing.close();await events.close();while(processing)await new Promise(r=>setTimeout(r,20));await redis.quit().catch(()=>redis.disconnect()); await pool.end();});
  return {app,pool,redis,billing};
}
