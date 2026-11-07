import { loadConfig } from './config.js';
import { buildApp } from './app.js';
const config = loadConfig();
const { app } = await buildApp(config);
await app.listen({host:config.HOST,port:config.PORT});
let closing = false;
for (const signal of ['SIGINT','SIGTERM']) process.on(signal, () => {
  if (closing) return;
  closing = true;
  const timer=setTimeout(()=>process.exit(1),135000);timer.unref();
  void app.close().then(()=>{clearTimeout(timer);process.exit(0);});
});
