import { createServer } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
export async function mockUpstream() {
  let calls = 0;
  const server=createServer(async (req,res) => {
    if(req.url==='/v1/models'){res.setHeader('content-type','application/json');res.end(JSON.stringify({object:'list',data:[{id:'mock-model'}]}));return;}
    let raw='';for await(const chunk of req)raw+=chunk;
    calls++;
    const body=JSON.parse(raw||'{}');
    if(body.model==='limited'){res.writeHead(429,{'content-type':'application/json'});res.end('{"error":{"message":"limited"}}');return;}
    if(body.model==='broken'){res.writeHead(500);res.end('not a JSON response');return;}
    const usage=body.model==='unknown'?undefined:{prompt_tokens:10,completion_tokens:5,total_tokens:15};
    if(body.stream){
      res.writeHead(200,{'content-type':'text/event-stream'});
      res.write('data: '+JSON.stringify({id:'mock',object:'chat.completion.chunk',choices:[{index:0,delta:{content:'你好'},finish_reason:null}]})+'\n\n');
      if(body.model==='partial'){await delay(20);res.destroy();return;}
      if(body.model==='slow') await delay(500);
      res.write('data: '+JSON.stringify({id:'mock',object:'chat.completion.chunk',choices:[{index:0,delta:{},finish_reason:'stop'}],usage})+'\n\n');
      res.end('data: [DONE]\n\n');
    }else{
      res.setHeader('content-type','application/json');
      res.end(JSON.stringify({id:'mock',object:'chat.completion',model:body.model,choices:[{index:0,message:{role:'assistant',content:'你好，网关已连接。'},finish_reason:'stop'}],usage}));
    }
  });
  await new Promise<void>(resolve=>server.listen(0,'127.0.0.1',resolve));
  const address=server.address();if(!address||typeof address==='string') throw Error('Invalid address');
  return {url:`http://127.0.0.1:${address.port}/v1`,get calls(){return calls;},close:()=>new Promise<void>((resolve,reject)=>server.close(e=>e?reject(e):resolve()))};
}
