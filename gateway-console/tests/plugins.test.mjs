import assert from 'node:assert/strict';
import { afterEach, describe, it } from 'node:test';
import { setCsrfToken } from '../.test-dist/api/client.js';
import {
  abiModeLabel,
  buildLoadNativePayload,
  createPlugin,
  deletePlugin,
  dryRunPlugin,
  listLoadedNativeLibraries,
  listPlugins,
  loadNativePlugin,
  parseJsonObject,
  pluginTypeLabel,
  pluginTypeTone,
  unloadJarPlugin,
  unloadNativePlugin,
  updatePlugin,
  validateNativeLoadInput,
} from '../.test-dist/api/plugins.js';

const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
});

function mockFetchJson(expectedUrl, expectedMethod, responseBody, onCall) {
  globalThis.fetch = async (input, init) => {
    assert.equal(String(input), expectedUrl);
    assert.equal(init?.method, expectedMethod);
    onCall?.(init);
    return new Response(JSON.stringify(responseBody), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };
}

describe('plugins helper utilities', () => {
  it('validates shared library and jar input paths', () => {
    assert.deepEqual(validateNativeLoadInput('lib', ' /opt/plugins/libdemo.so '), {
      ok: true,
      path: '/opt/plugins/libdemo.so',
    });
    assert.deepEqual(validateNativeLoadInput('lib', '/opt/plugins/libdemo.dylib'), {
      ok: true,
      path: '/opt/plugins/libdemo.dylib',
    });
    assert.deepEqual(validateNativeLoadInput('lib', 'C:\\plugins\\demo.dll'), {
      ok: true,
      path: 'C:\\plugins\\demo.dll',
    });
    assert.equal(validateNativeLoadInput('lib', '').ok, false);
    assert.equal(validateNativeLoadInput('lib', '/opt/plugins/demo.jar').ok, false);

    assert.deepEqual(validateNativeLoadInput('jar', '/opt/plugins/demo.jar'), {
      ok: true,
      path: '/opt/plugins/demo.jar',
    });
    assert.equal(validateNativeLoadInput('jar', '/opt/plugins/libdemo.so').ok, false);
  });

  it('builds load-native request payloads for lib and jar modes', () => {
    assert.deepEqual(buildLoadNativePayload('lib', ' /tmp/liba.so '), {
      libPath: '/tmp/liba.so',
    });
    assert.deepEqual(buildLoadNativePayload('jar', '/tmp/a.jar', ''), {
      jarPath: '/tmp/a.jar',
    });
    assert.deepEqual(buildLoadNativePayload('jar', '/tmp/a.jar', '/tmp/out/liba.so'), {
      jarPath: '/tmp/a.jar',
      outputLibPath: '/tmp/out/liba.so',
    });
  });

  it('parses JSON object inputs and rejects invalid payloads', () => {
    assert.deepEqual(parseJsonObject('', '配置'), { ok: true, value: {} });
    assert.deepEqual(parseJsonObject('{"limit":5}', '配置'), { ok: true, value: { limit: 5 } });
    assert.equal(parseJsonObject('[1,2]', '配置').ok, false);
    assert.equal(parseJsonObject('{broken', '配置').ok, false);
  });

  it('formats plugin types and ABI modes', () => {
    assert.equal(pluginTypeLabel('NATIVE_FFM'), 'Native FFM');
    assert.equal(pluginTypeTone('NATIVE_FFM'), 'info');
    assert.equal(pluginTypeLabel('JAVA_SPI'), 'Java SPI (.jar)');
    assert.equal(abiModeLabel('GRAAL_ISOLATE'), 'GraalVM Isolate');
    assert.equal(abiModeLabel('PLAIN_C'), 'Plain C ABI');
  });
});

describe('plugins API client', () => {
  it('fetches mounted native libraries and runtime capability flags', async () => {
    mockFetchJson('/api/plugins/native-loaded', 'GET', {
      nativeImageRuntime: true,
      nativeImageCompilerAvailable: true,
      items: [
        {
          libraryPath: '/opt/plugins/libext_quota.so',
          abiMode: 'GRAAL_ISOLATE',
          lastModifiedMillis: 1700000000000,
          loadedAtMillis: 1700000001000,
          inFlightCalls: 0,
          pluginIds: ['ext-quota-header'],
        },
      ],
    });

    const res = await listLoadedNativeLibraries();
    assert.equal(res.nativeImageRuntime, true);
    assert.equal(res.nativeImageCompilerAvailable, true);
    assert.equal(res.items.length, 1);
    assert.equal(res.items[0].pluginIds[0], 'ext-quota-header');
  });

  it('loads and unloads native shared libraries with CSRF header', async () => {
    setCsrfToken('csrf-test-token');
    mockFetchJson(
      '/api/plugins/load-native',
      'POST',
      {
        loadedCount: 1,
        libraryPath: '/opt/plugins/libext_quota.so',
        plugins: [
          {
            id: 'ext-quota-header',
            name: 'External Quota',
            type: 'NATIVE_FFM',
            priority: 20,
            className: 'com.ext.QuotaPlugin',
          },
        ],
      },
      (init) => {
        const headers = init?.headers;
        assert.equal(headers['X-CSRF-Token'], 'csrf-test-token');
        assert.deepEqual(JSON.parse(String(init?.body)), {
          jarPath: '/opt/plugins/ext-quota.jar',
        });
      },
    );

    const loaded = await loadNativePlugin({ jarPath: '/opt/plugins/ext-quota.jar' });
    assert.equal(loaded.loadedCount, 1);

    mockFetchJson('/api/plugins/unload-native', 'POST', { unloaded: true }, (init) => {
      assert.deepEqual(JSON.parse(String(init?.body)), {
        libPath: '/opt/plugins/libext_quota.so',
      });
    });
    const unloaded = await unloadNativePlugin({ libPath: '/opt/plugins/libext_quota.so' });
    assert.equal(unloaded.unloaded, true);

    mockFetchJson('/api/plugins/unload-jar', 'POST', { unloaded: true });
    const jarUnloaded = await unloadJarPlugin({ pluginId: 'ext-quota-header' });
    assert.equal(jarUnloaded.unloaded, true);
  });

  it('lists, creates, updates, deletes, and dry-runs plugins', async () => {
    mockFetchJson('/api/plugins', 'GET', {
      items: [{ id: 'p1', name: 'ext-quota', type: 'NATIVE_FFM', enabled: true }],
    });
    const list = await listPlugins();
    assert.equal(list.length, 1);

    mockFetchJson('/api/plugins', 'POST', { id: 'p2', name: 'new-native', type: 'NATIVE_FFM' });
    const created = await createPlugin({
      name: 'new-native',
      type: 'NATIVE_FFM',
      jarPath: '/opt/plugins/libnew.so',
    });
    assert.equal(created.id, 'p2');

    mockFetchJson('/api/plugins/p2', 'PATCH', { id: 'p2', enabled: false });
    const updated = await updatePlugin('p2', { enabled: false });
    assert.equal(updated.enabled, false);

    mockFetchJson('/api/plugins/p2', 'DELETE', { deleted: true, id: 'p2', runtimeUnloaded: true });
    const deleted = await deletePlugin('p2');
    assert.equal(deleted.runtimeUnloaded, true);

    mockFetchJson('/api/plugins/dry-run', 'POST', {
      stage: 'PRE_ADMIT',
      action: 'CONTINUE',
      httpStatus: 200,
    });
    const dry = await dryRunPlugin({ pluginId: 'ext-quota', stage: 'PRE_ADMIT' });
    assert.equal(dry.action, 'CONTINUE');
  });
});
