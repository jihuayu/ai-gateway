import { chromium } from 'playwright';
import { mkdir } from 'node:fs/promises';
import assert from 'node:assert/strict';
import { mockUpstream } from '../test/mock-upstream.js';
const upstream=await mockUpstream();
const browser=await chromium.launch({headless:true});
const page=await browser.newPage({viewport:{width:1440,height:1000}});
const errors:string[]=[];page.on('pageerror',e=>errors.push(e.message));
const base=process.env.BROWSER_TEST_URL||'http://127.0.0.1:3000';
await mkdir('artifacts',{recursive:true});
try{
  await page.goto(base);
  await page.locator('#auth-view').waitFor({state:'visible'});
  await page.locator('#tab-login').click();
  await page.locator('#login-email').fill('browser@example.com');
  await page.locator('#login-password').fill('browser-test-password');
  const loginResponse=page.waitForResponse(r=>r.url().endsWith('/api/login'));
  await page.locator('#login-form button[type=submit]').click();
  if((await loginResponse).status()!==200){
    await page.locator('#tab-bootstrap').click();
    await page.locator('#bootstrap-email').fill('browser@example.com');
    await page.locator('#bootstrap-password').fill('browser-test-password');
    await page.locator('#bootstrap-project').fill('浏览器验证项目');
    await page.locator('#bootstrap-form button[type=submit]').click();
  }
  const suffix=Date.now().toString();const providerName='验证上游-'+suffix, aliasName='browser-'+suffix;
  await page.locator('#app-shell').waitFor({state:'visible'});
  await page.locator('[data-nav=providers]').click();
  await page.locator('[data-action=provider-new]').first().click();
  await page.locator('#provider-name').fill(providerName);
  await page.locator('#provider-url').fill(upstream.url);
  await page.locator('#provider-secret').fill('mock-secret');
  await page.locator('form[data-form=provider] button[type=submit]').click();
  await page.locator('#provider-name').waitFor({state:'hidden'});
  await page.locator('[data-action=target-new]').first().click();
  await page.locator('#target-provider').selectOption({label:providerName});
  await page.locator('#target-model').fill('mock-model');
  await page.locator('#target-input').fill('1');await page.locator('#target-output').fill('2');
  await page.locator('#target-caps').fill('chat,stream');
  const targetResponse=page.waitForResponse(r=>r.url().endsWith('/targets')&&r.request().method()==='POST');
  await page.locator('form[data-form=target] button[type=submit]').click();
  const targetId=(await (await targetResponse).json()).id;
  await page.locator('#target-model').waitFor({state:'hidden'});
  await page.locator('[data-nav=aliases]').click();
  await page.locator('[data-action=alias-new]').first().click();
  await page.locator('#alias-name').fill(aliasName);
  await page.locator(`input[name=targetIds][value="${targetId}"]`).check();
  await page.locator('form[data-form=alias] button[type=submit]').click();
  await page.locator('#alias-name').waitFor({state:'hidden'});
  await page.locator('tr').filter({hasText:aliasName}).locator('[data-action=alias-publish]').click();
  await page.getByRole('button',{name:'发布',exact:true}).last().click();
  await page.locator('[data-nav=keys]').click();
  await page.locator('[data-action=key-new]').first().click();
  await page.locator('#key-name').fill('browser-key-'+suffix);
  const scope=page.locator(`input[type=checkbox][value="${aliasName}"]`);if(await scope.count())await scope.check();
  await page.locator('form[data-form=key] button[type=submit]').click();
  await page.locator('#new-key-value').waitFor();
  assert.ok((await page.locator('#new-key-value').innerText()).length>20);
  await page.locator('[data-action=modal-cancel]').last().click();
  assert.equal(await page.locator('#new-key-value').count(),0,'full key removed after one-time display');
  await page.locator('[data-nav=guide]').click();
  await page.locator('#test-model').selectOption(aliasName);
  await page.locator('#test-prompt').fill('浏览器完整链路验证');
  await page.locator('form[data-form=test] button[type=submit]').click();
  await page.locator('#test-result .response-box').waitFor();
  assert.match(await page.locator('#test-result').innerText(),/网关已连接/);
  for(const section of ['keys','requests','usage','settings','overview']){
    await page.locator(`[data-nav=${section}]`).click();
    await page.locator('.page-header:visible').first().waitFor();
    assert.equal(await page.locator('.error-state:visible').count(),0,section+' has an API error');
  }
  await page.screenshot({path:'artifacts/console-desktop.png',fullPage:true});
  await page.setViewportSize({width:390,height:844});
  await page.waitForTimeout(250);
  await page.screenshot({path:'artifacts/console-mobile.png',fullPage:true});
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>window.innerWidth),false,'mobile horizontal overflow');
  assert.deepEqual(errors,[]);console.log(JSON.stringify({browser:'Chromium',setup:true,request:true,pages:8,mobile:true,errors}));
}catch(e){await page.screenshot({path:'artifacts/browser-failure.png',fullPage:true});console.error('Browser failed at',page.url(),errors);throw e;}
finally{await browser.close();await upstream.close();}
