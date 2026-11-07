import { test } from 'node:test';
import assert from 'node:assert/strict';
import { encrypt,decrypt,hashKey,validateProviderUrl,isBlockedAddress } from '../src/security.js';
test('credential encryption authenticates ciphertext and uses a new nonce',()=>{
  const key='ab'.repeat(32),secret='test-provider-credential';const a=encrypt(secret,key),b=encrypt(secret,key);
  assert.notEqual(a,b);assert.equal(decrypt(a,key),secret);
  assert.throws(()=>decrypt(a,'cd'.repeat(32)));
  const parts=a.split('.');parts[3]=(parts[3][0]==='A'?'B':'A')+parts[3].slice(1);
  assert.throws(()=>decrypt(parts.join('.'),key));
  assert.ok(!a.includes(secret));assert.equal(hashKey('same'),hashKey('same'));
});
test('provider policy blocks reserved IPv4/IPv6 and credential-bearing URLs',async()=>{
  for(const ip of ['127.0.0.1','169.254.169.254','10.0.0.1','::1','::ffff:127.0.0.1','::ffff:7f00:1','fc00::1','fe80::1'])assert.equal(isBlockedAddress(ip),true,ip);
  assert.equal(isBlockedAddress('8.8.8.8'),false);
  for(const url of ['https://127.0.0.1/v1','https://2130706433/v1','https://[::1]/v1','https://[::ffff:7f00:1]/v1','https://169.254.169.254/latest','http://8.8.8.8/v1','https://user:pass@example.com/v1'])await assert.rejects(()=>validateProviderUrl(url));
  await validateProviderUrl('http://127.0.0.1:9876/v1',true);
});
