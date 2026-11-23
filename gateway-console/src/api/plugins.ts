import { api, arr, type BadgeTone } from './client.js';

export type PluginType = 'BUILTIN' | 'JAVA_SPI' | 'NATIVE_FFM' | 'JS' | 'WEBHOOK';
export type FailMode = 'FAIL_OPEN' | 'FAIL_CLOSED';

export const PLUGIN_STAGES = [
  'PRE_ADMIT',
  'POST_ADMIT',
  'PRE_ROUTE',
  'PRE_UPSTREAM',
  'STREAM_CHUNK',
  'POST_UPSTREAM',
  'POST_SETTLE',
  'ON_ERROR',
] as const;

export type PluginStageName = (typeof PLUGIN_STAGES)[number];

export interface PluginRecord {
  id: string;
  workspaceId: string | null;
  name: string;
  type: PluginType;
  version: number;
  priority: number;
  stages: string[];
  sourceCode?: string | null;
  codeSha256?: string | null;
  jarPath?: string | null;
  className?: string | null;
  config: Record<string, unknown>;
  failMode: FailMode;
  timeoutMs: number;
  maxStatements: number;
  enabled: boolean;
  createdAt?: string;
  updatedAt?: string;
}

export interface LoadedNativeLibSummary {
  libraryPath: string;
  abiMode: 'GRAAL_ISOLATE' | 'PLAIN_C' | string;
  lastModifiedMillis: number;
  loadedAtMillis: number;
  inFlightCalls: number;
  pluginIds: string[];
}

export interface NativeLoadedResponse {
  nativeImageRuntime: boolean;
  nativeImageCompilerAvailable: boolean;
  items: LoadedNativeLibSummary[];
}

export interface SummarizedPlugin {
  id: string;
  name: string;
  type: string;
  priority: number;
  className: string;
}

export interface LoadNativeResponse {
  loadedCount: number;
  libraryPath: string;
  compiledFromJar?: string;
  plugins: SummarizedPlugin[];
}

export interface UnloadResponse {
  unloaded: boolean;
}

export interface DeletePluginResponse {
  deleted: boolean;
  id: string;
  runtimeUnloaded?: boolean;
}

export interface CreatePluginInput {
  name: string;
  type: PluginType;
  priority?: number;
  stages?: string[];
  failMode?: FailMode;
  timeoutMs?: number;
  maxStatements?: number;
  enabled?: boolean;
  sourceCode?: string;
  jarPath?: string;
  libPath?: string;
  className?: string;
  compileNative?: boolean;
  config?: Record<string, unknown>;
}

export interface DryRunInput {
  pluginId?: string;
  name?: string;
  type?: PluginType;
  stage: string;
  failMode?: FailMode;
  sourceCode?: string;
  headers?: Record<string, string>;
  requestBody?: Record<string, unknown>;
  config?: Record<string, unknown>;
}

export interface DryRunResponse {
  stage: string;
  action: 'CONTINUE' | 'REJECT' | 'SHORT_CIRCUIT' | string;
  httpStatus: number;
  errorCode?: string;
  message?: string;
  shortCircuitBody?: string;
  executedStatements?: number;
  mutatedModelAlias?: string;
  mutatedRequestHeaders?: Record<string, string>;
  mutatedRequestBody?: Record<string, unknown>;
  mutatedResponseHeaders?: Record<string, string>;
  mutatedResponseBody?: string;
  attributionTags?: Record<string, string>;
  trace?: unknown[];
}

export function pluginTypeLabel(type: string): string {
  switch (String(type || '').toUpperCase()) {
    case 'NATIVE_FFM':
      return 'Native FFM';
    case 'JAVA_SPI':
      return 'Java SPI (.jar)';
    case 'JS':
      return 'JS 沙箱';
    case 'WEBHOOK':
      return 'Webhook';
    case 'BUILTIN':
      return '内置插件';
    default:
      return type || '未知';
  }
}

export function pluginTypeTone(type: string): BadgeTone {
  switch (String(type || '').toUpperCase()) {
    case 'NATIVE_FFM':
      return 'info';
    case 'JAVA_SPI':
      return 'warning';
    case 'JS':
      return 'success';
    case 'WEBHOOK':
      return 'neutral';
    default:
      return 'neutral';
  }
}

export function abiModeLabel(abiMode: string): string {
  switch (String(abiMode || '').toUpperCase()) {
    case 'GRAAL_ISOLATE':
      return 'GraalVM Isolate';
    case 'PLAIN_C':
      return 'Plain C ABI';
    default:
      return abiMode || '未知 ABI';
  }
}

export function validateNativeLoadInput(
  mode: 'lib' | 'jar',
  rawPath: string,
): { ok: true; path: string } | { ok: false; error: string } {
  const trimmed = rawPath.trim();
  if (!trimmed) {
    return {
      ok: false,
      error: mode === 'lib' ? '请输入共享库路径（.so / .dylib / .dll）。' : '请输入插件 JAR 路径（.jar）。',
    };
  }
  const lower = trimmed.toLowerCase();
  if (mode === 'lib') {
    if (!lower.endsWith('.so') && !lower.endsWith('.dylib') && !lower.endsWith('.dll')) {
      return { ok: false, error: '共享库路径必须以 .so、.dylib 或 .dll 结尾。' };
    }
  } else if (!lower.endsWith('.jar')) {
    return { ok: false, error: 'JAR 编译模式路径必须以 .jar 结尾。' };
  }
  return { ok: true, path: trimmed };
}

export function buildLoadNativePayload(
  mode: 'lib' | 'jar',
  rawPath: string,
  rawOutputLibPath?: string,
): { libPath?: string; jarPath?: string; outputLibPath?: string } {
  const path = rawPath.trim();
  if (mode === 'lib') {
    return { libPath: path };
  }
  const out = (rawOutputLibPath || '').trim();
  return out ? { jarPath: path, outputLibPath: out } : { jarPath: path };
}

export function parseJsonObject(
  raw: string,
  fieldLabel: string,
): { ok: true; value: Record<string, unknown> } | { ok: false; error: string } {
  const trimmed = raw.trim();
  if (!trimmed) return { ok: true, value: {} };
  try {
    const parsed = JSON.parse(trimmed);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return { ok: false, error: `${fieldLabel} 必须是 JSON 对象。` };
    }
    return { ok: true, value: parsed as Record<string, unknown> };
  } catch {
    return { ok: false, error: `${fieldLabel} 不是合法的 JSON 格式。` };
  }
}

export async function listPlugins(): Promise<PluginRecord[]> {
  const res = await api<{ items?: PluginRecord[] }>('/api/plugins');
  return arr<PluginRecord>(res?.items);
}

export async function createPlugin(input: CreatePluginInput): Promise<PluginRecord> {
  return api<PluginRecord>('/api/plugins', { method: 'POST', body: input });
}

export async function updatePlugin(
  pluginId: string,
  patch: Partial<Pick<PluginRecord, 'enabled' | 'priority' | 'failMode' | 'timeoutMs' | 'stages' | 'config' | 'sourceCode'>>,
): Promise<PluginRecord> {
  return api<PluginRecord>(`/api/plugins/${encodeURIComponent(pluginId)}`, {
    method: 'PATCH',
    body: patch,
  });
}

export async function deletePlugin(pluginId: string): Promise<DeletePluginResponse> {
  return api<DeletePluginResponse>(`/api/plugins/${encodeURIComponent(pluginId)}`, {
    method: 'DELETE',
  });
}

export async function listLoadedNativeLibraries(): Promise<NativeLoadedResponse> {
  const res = await api<NativeLoadedResponse>('/api/plugins/native-loaded');
  return {
    nativeImageRuntime: Boolean(res?.nativeImageRuntime),
    nativeImageCompilerAvailable: Boolean(res?.nativeImageCompilerAvailable),
    items: arr<LoadedNativeLibSummary>(res?.items),
  };
}

export async function loadNativePlugin(payload: {
  libPath?: string;
  jarPath?: string;
  outputLibPath?: string;
}): Promise<LoadNativeResponse> {
  return api<LoadNativeResponse>('/api/plugins/load-native', {
    method: 'POST',
    body: payload,
  });
}

export async function unloadNativePlugin(payload: {
  libPath?: string;
  pluginId?: string;
}): Promise<UnloadResponse> {
  return api<UnloadResponse>('/api/plugins/unload-native', {
    method: 'POST',
    body: payload,
  });
}

export async function unloadJarPlugin(payload: {
  jarPath?: string;
  pluginId?: string;
}): Promise<UnloadResponse> {
  return api<UnloadResponse>('/api/plugins/unload-jar', {
    method: 'POST',
    body: payload,
  });
}

export async function dryRunPlugin(input: DryRunInput): Promise<DryRunResponse> {
  return api<DryRunResponse>('/api/plugins/dry-run', {
    method: 'POST',
    body: input,
  });
}
