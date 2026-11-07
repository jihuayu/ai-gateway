/** Compatibility fixture for historical SQL/billing contract tests, never used by buildApp. */
import type { Pool } from 'pg';
import { authenticateKey,loadRoute,MODELS_SQL,persistRequestMetadata,type GatewayDataPlane } from '../src/gateway.js';
export function pgGatewayDataPlane(pool:Pool):GatewayDataPlane{
  return {authenticateKey:req=>authenticateKey(req,pool),loadRoute:(key,model)=>loadRoute(pool,key.projectId,key.workspaceId,model),
    models:async key=>(await pool.query(MODELS_SQL,[key.projectId])).rows,
    persistMetadata:(id,alias,status,attempts,rows)=>persistRequestMetadata(pool,id,alias,status,attempts,rows)};
}
