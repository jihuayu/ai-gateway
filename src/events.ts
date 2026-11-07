import type { Pool } from 'pg';
import { Kafka, type Producer, type Consumer } from 'kafkajs';

export async function setupEvents(pool:Pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS usage_hourly (
    project_id uuid NOT NULL REFERENCES projects(id), bucket timestamptz NOT NULL,
    actual_micros bigint NOT NULL DEFAULT 0, estimated_micros bigint NOT NULL DEFAULT 0,
    input_tokens bigint NOT NULL DEFAULT 0, output_tokens bigint NOT NULL DEFAULT 0,
    event_count bigint NOT NULL DEFAULT 0, updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(project_id,bucket));
    CREATE TABLE IF NOT EXISTS dead_letters(event_id text PRIMARY KEY, envelope jsonb NOT NULL, reason text NOT NULL, created_at timestamptz DEFAULT now());`);
}

export interface Envelope {event_id:string;schema_version:number;occurred_at:string;project_id:string|null;event_type:string;payload:unknown}
/** A claimed batch projects each affected project once, in the same inbox transaction. */
export async function consumeEvents(pool:Pool,events:Envelope[]) {
  if(!events.length)return;
  for(const event of events)if(event.schema_version!==1||typeof event.event_id!=='string')throw Error('Unsupported event schema');
  const entries=[...new Map(events.map(e=>[e.event_id,{event_id:e.event_id,project_id:e.project_id}])).values()].sort((a,b)=>a.event_id.localeCompare(b.event_id));
  const c=await pool.connect();
  try{
    await c.query('BEGIN');
    const inserted=await c.query(`INSERT INTO consumer_inbox(consumer_name,event_id,project_id)
      SELECT 'usage-v1',event_id,project_id FROM jsonb_to_recordset($1::jsonb) AS e(event_id text,project_id uuid)
      ORDER BY event_id ON CONFLICT DO NOTHING RETURNING project_id`,[JSON.stringify(entries)]);
    const projects=[...new Set<string>(inserted.rows.map(r=>r.project_id).filter(Boolean))].sort();
    for(const project of projects){
      await c.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))',[project]);
      await c.query(`INSERT INTO usage_hourly(project_id,bucket,actual_micros,estimated_micros,input_tokens,output_tokens,event_count)
        SELECT project_id,date_trunc('hour',first_at),COALESCE(sum(amount) FILTER(WHERE NOT estimated),0),
        COALESCE(sum(amount) FILTER(WHERE estimated),0),COALESCE(sum(input_tokens),0),COALESCE(sum(output_tokens),0),count(*)
        FROM (SELECT project_id,COALESCE(request_id::text,NULLIF(split_part(event_id,':',2),''),event_id) AS identity,
          min(created_at) AS first_at,sum(amount_micros) AS amount,
          (array_agg(estimated ORDER BY created_at DESC,event_id DESC))[1] AS estimated,
          (array_agg(input_tokens ORDER BY created_at DESC,event_id DESC))[1] AS input_tokens,
          (array_agg(output_tokens ORDER BY created_at DESC,event_id DESC))[1] AS output_tokens
          FROM ledger WHERE project_id=$1 GROUP BY project_id,identity) entries
        GROUP BY project_id,date_trunc('hour',first_at)
        ON CONFLICT(project_id,bucket) DO UPDATE SET actual_micros=excluded.actual_micros,estimated_micros=excluded.estimated_micros,
        input_tokens=excluded.input_tokens,output_tokens=excluded.output_tokens,event_count=excluded.event_count,updated_at=now()`,[project]);
    }
    await c.query('COMMIT');
  }catch(e){await c.query('ROLLBACK');throw e;}finally{c.release();}
}
export async function consumeEvent(pool:Pool,event:Envelope){await consumeEvents(pool,[event]);}

export class EventWorker {
  private producer?:Producer;
  private consumer?:Consumer;
  private stopped=false;
  constructor(private pool:Pool,private brokers:string[]=[]){ }
  async start() {
    await setupEvents(this.pool);
    if(this.brokers.length) {
      const kafka=new Kafka({clientId:'ai-gateway',brokers:this.brokers,retry:{retries:3},logLevel:0});
      const admin=kafka.admin();await admin.connect();
      try {await admin.createTopics({topics:['gateway.usage.v1','gateway.request.v1','gateway.audit.v1'].map(topic=>({topic,numPartitions:3,replicationFactor:Number(process.env.KAFKA_REPLICATION_FACTOR||1)}))});} finally{await admin.disconnect();}
      this.producer=kafka.producer({idempotent:true,maxInFlightRequests:1});await this.producer.connect();
      this.consumer=kafka.consumer({groupId:'gateway-usage-v1'});await this.consumer.connect();
      await this.consumer.subscribe({topics:['gateway.usage.v1','gateway.request.v1','gateway.audit.v1'],fromBeginning:true});
      await this.consumer.run({eachMessage:async ({topic,partition,message})=>{
        if(!message.value)return;
        let event:Envelope;
        try {
          event=JSON.parse(message.value.toString());
          if(event.schema_version!==1||typeof event.event_id!=='string')throw Error('schema');
        }catch{
          // Invalid schemas are permanent; persist a dead letter before committing this offset.
          await this.pool.query(`INSERT INTO dead_letters(event_id,envelope,reason) VALUES($1,$2,'invalid_schema') ON CONFLICT DO NOTHING`,
            [`${topic}:${partition}:${message.offset}`,JSON.stringify({topic,partition,offset:message.offset})]);
          return;
        }
        await consumeEvent(this.pool,event);
      }});
    }
  }
  async tick() {
    const c=await this.pool.connect();let rows:any[];
    try {
      await c.query('BEGIN');
      rows=(await c.query(`WITH candidate AS (SELECT id FROM outbox WHERE status IN('pending','leased') AND available_at<=now()
        ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 50)
        UPDATE outbox o SET status='leased',available_at=now()+interval '30 seconds' FROM candidate c WHERE o.id=c.id RETURNING o.*`)).rows;
      await c.query('COMMIT');
    }catch(e){await c.query('ROLLBACK');throw e;}finally{c.release();}
    if(!this.producer&&rows!.length){
      try{
        await consumeEvents(this.pool,rows!.map(row=>({event_id:row.event_id,schema_version:1,occurred_at:new Date(row.created_at).toISOString(),project_id:row.project_id,event_type:row.event_type,payload:row.payload})));
        await this.pool.query("UPDATE outbox SET status='published',published_at=now() WHERE id=ANY($1::uuid[])",[rows!.map(r=>r.id)]);
        return rows!.length;
      }catch{
        // Fall back to individual retry/DLQ handling so one poison event cannot block peers.
      }
    }
    for(const row of rows!) {
      const envelope:Envelope={event_id:row.event_id,schema_version:1,occurred_at:new Date(row.created_at).toISOString(),project_id:row.project_id,event_type:row.event_type,payload:row.payload};
      try {
        if(this.producer) {
          const kind=/audit/.test(row.event_type)?'audit':/usage|settle|billing/.test(row.event_type)?'usage':'request';
          await this.producer.send({topic:`gateway.${kind}.v1`,acks:-1,messages:[{key:row.project_id||row.event_id,value:JSON.stringify(envelope)}]});
        } else await consumeEvent(this.pool,envelope);
        await this.pool.query(`UPDATE outbox SET status='published',published_at=now() WHERE id=$1`,[row.id]);
      } catch {
        const attempt=Number(row.attempts)+1;
        const c=await this.pool.connect();try{
          await c.query('BEGIN');
          if(attempt>=5) await c.query(`INSERT INTO dead_letters(event_id,envelope,reason) VALUES($1,$2,'delivery_failed') ON CONFLICT DO NOTHING`,[row.event_id,JSON.stringify(envelope)]);
          await c.query(`UPDATE outbox SET status=$2,attempts=$3,available_at=now()+($4*interval '1 second') WHERE id=$1`,[row.id,attempt>=5?'failed':'pending',attempt,Math.min(300,2**attempt)]);
          await c.query('COMMIT');
        }catch(e){await c.query('ROLLBACK');throw e;}finally{c.release();}
      }
    }
    return rows!.length;
  }
  async replay(eventId:string) {
    await this.pool.query(`UPDATE outbox SET status='pending',attempts=0,available_at=now() WHERE event_id=$1 AND status='failed'`,[eventId]);
  }
  async run(){while(!this.stopped){await this.tick();await new Promise(r=>setTimeout(r,500));}}
  async close(){this.stopped=true;await this.consumer?.disconnect();await this.producer?.disconnect();}
}
