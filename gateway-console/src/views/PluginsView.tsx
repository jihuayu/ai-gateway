import React, { useEffect, useState } from 'react';
import * as stylex from '@stylexjs/stylex';
import { Tabs, TabsList, TabsPanel, TabsTab } from '@ui-sdk/react/tabs';
import { Field, FieldDescription, FieldLabel } from '@ui-sdk/react/field';
import { Input } from '@ui-sdk/react/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@ui-sdk/react/select';
import { ui } from '../styles/ui';
import { fmtDate, fmtInt } from '../api/client';
import {
  PLUGIN_STAGES,
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
  type DryRunResponse,
  type FailMode,
  type LoadedNativeLibSummary,
  type NativeLoadedResponse,
  type PluginRecord,
  type PluginType,
} from '../api/plugins';
import {
  EmptyBox,
  MetricCard,
  PageHeader,
  UiBadge,
  UiButton,
  UiCheckboxRow,
  UiConfirmDialog,
  UiDialog,
  UiSwitch,
  copyToClipboard,
  notify,
} from '../components/Primitives';

export function PluginsView({
  canWrite,
  refreshTick,
}: {
  canWrite: boolean;
  refreshTick: number;
}) {
  const [tab, setTab] = useState<'native' | 'registry' | 'dryrun'>('native');
  const [plugins, setPlugins] = useState<PluginRecord[]>([]);
  const [nativeState, setNativeState] = useState<NativeLoadedResponse>({
    nativeImageRuntime: false,
    nativeImageCompilerAvailable: false,
    items: [],
  });
  const [loading, setLoading] = useState(false);

  // Load Native / JAR dialog state
  const [loadOpen, setLoadOpen] = useState(false);
  const [loadMode, setLoadMode] = useState<'lib' | 'jar'>('lib');
  const [loadPath, setLoadPath] = useState('');
  const [outputLibPath, setOutputLibPath] = useState('');
  const [loadError, setLoadError] = useState('');
  const [loadBusy, setLoadBusy] = useState(false);

  // Register plugin dialog state
  const [createOpen, setCreateOpen] = useState(false);
  const [createName, setCreateName] = useState('');
  const [createType, setCreateType] = useState<PluginType>('NATIVE_FFM');
  const [createPriority, setCreatePriority] = useState('100');
  const [createFailMode, setCreateFailMode] = useState<FailMode>('FAIL_CLOSED');
  const [createTimeoutMs, setCreateTimeoutMs] = useState('50');
  const [createStages, setCreateStages] = useState<string[]>(['PRE_ADMIT']);
  const [createJarOrLibPath, setCreateJarOrLibPath] = useState('');
  const [createClassName, setCreateClassName] = useState('');
  const [createCompileNative, setCreateCompileNative] = useState(true);
  const [createSourceCode, setCreateSourceCode] = useState('');
  const [createConfigText, setCreateConfigText] = useState('{}');
  const [createError, setCreateError] = useState('');

  // Confirm dialogs
  const [unloadTarget, setUnloadTarget] = useState<LoadedNativeLibSummary | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<PluginRecord | null>(null);

  // Dry-run state
  const [dryPluginId, setDryPluginId] = useState('');
  const [dryStage, setDryStage] = useState<string>('PRE_ADMIT');
  const [dryHeadersText, setDryHeadersText] = useState('{"x-tenant":"demo"}');
  const [dryBodyText, setDryBodyText] = useState(
    '{"model":"support-chat","messages":[{"role":"user","content":"hello"}]}',
  );
  const [dryConfigText, setDryConfigText] = useState('{}');
  const [dryResult, setDryResult] = useState<DryRunResponse | null>(null);
  const [dryError, setDryError] = useState('');

  async function refreshAll() {
    setLoading(true);
    try {
      const [pluginList, nativeLoaded] = await Promise.all([
        listPlugins(),
        listLoadedNativeLibraries(),
      ]);
      setPlugins(pluginList);
      setNativeState(nativeLoaded);
      if (!dryPluginId && nativeLoaded.items[0]?.pluginIds[0]) {
        setDryPluginId(nativeLoaded.items[0].pluginIds[0]);
      }
    } catch (e: any) {
      notify(e?.message || '读取插件与 Native FFM 状态失败', 'error');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refreshAll();
  }, [refreshTick]);

  async function handleLoadNativeSubmit(ev: React.FormEvent) {
    ev.preventDefault();
    const check = validateNativeLoadInput(loadMode, loadPath);
    if (!check.ok) {
      setLoadError(check.error);
      return;
    }
    setLoadError('');
    setLoadBusy(true);
    try {
      const payload = buildLoadNativePayload(loadMode, check.path, outputLibPath);
      const res = await loadNativePlugin(payload);
      const ids = res.plugins.map((p) => p.id).join(', ');
      notify(
        `已挂载 ${res.loadedCount} 个 Native FFM 插件`,
        'success',
        ids ? `插件 ID：${ids}` : res.libraryPath,
      );
      setLoadOpen(false);
      setLoadPath('');
      setOutputLibPath('');
      await refreshAll();
    } catch (e: any) {
      setLoadError(e?.message || '加载共享库失败');
    } finally {
      setLoadBusy(false);
    }
  }

  async function handleHotReloadLib(libPath: string) {
    try {
      const res = await loadNativePlugin({ libPath });
      notify(`已热重载共享库（${res.loadedCount} 个插件）`, 'success', libPath);
      await refreshAll();
    } catch (e: any) {
      notify(e?.message || '热重载失败', 'error');
    }
  }

  async function handleConfirmUnloadNative() {
    if (!unloadTarget) return;
    try {
      const res = await unloadNativePlugin({ libPath: unloadTarget.libraryPath });
      if (res.unloaded) {
        notify('已卸载 Native 共享库并释放 Isolate', 'success', unloadTarget.libraryPath);
      } else {
        notify('目标共享库当前未处于挂载状态', 'info', unloadTarget.libraryPath);
      }
      await refreshAll();
    } catch (e: any) {
      notify(e?.message || '卸载共享库失败', 'error');
    }
  }

  async function handleToggleEnabled(plugin: PluginRecord, enabled: boolean) {
    try {
      const updated = await updatePlugin(plugin.id, { enabled });
      setPlugins((prev) => prev.map((p) => (p.id === plugin.id ? updated : p)));
      notify(`已${enabled ? '启用' : '停用'}插件 ${plugin.name}`, 'success');
    } catch (e: any) {
      notify(e?.message || '更新插件状态失败', 'error');
    }
  }

  async function handleUnloadRegisteredRuntime(plugin: PluginRecord) {
    try {
      const res =
        plugin.type === 'NATIVE_FFM'
          ? await unloadNativePlugin(
              plugin.jarPath ? { libPath: plugin.jarPath } : { pluginId: plugin.name },
            )
          : await unloadJarPlugin(
              plugin.jarPath ? { jarPath: plugin.jarPath } : { pluginId: plugin.name },
            );
      notify(
        res.unloaded ? `已从运行时卸载 ${plugin.name}` : `运行时未找到 ${plugin.name} 的活跃句柄`,
        res.unloaded ? 'success' : 'info',
      );
      await refreshAll();
    } catch (e: any) {
      notify(e?.message || '运行时卸载失败', 'error');
    }
  }

  async function handleConfirmDeletePlugin() {
    if (!deleteTarget) return;
    try {
      const res = await deletePlugin(deleteTarget.id);
      notify(
        `已删除插件 ${deleteTarget.name}`,
        'success',
        res.runtimeUnloaded ? '同时已自动卸载关联的运行时动态库。' : undefined,
      );
      await refreshAll();
    } catch (e: any) {
      notify(e?.message || '删除插件失败', 'error');
    }
  }

  function toggleStage(stage: string, checked: boolean) {
    setCreateStages((prev) => {
      if (checked) return prev.includes(stage) ? prev : [...prev, stage];
      const next = prev.filter((s) => s !== stage);
      return next.length > 0 ? next : ['PRE_ADMIT'];
    });
  }

  async function handleCreatePluginSubmit(ev: React.FormEvent) {
    ev.preventDefault();
    const name = createName.trim();
    if (!name) {
      setCreateError('请输入插件名称。');
      return;
    }
    const cfg = parseJsonObject(createConfigText, '插件配置');
    if (!cfg.ok) {
      setCreateError(cfg.error);
      return;
    }
    if (createType === 'JS' && !createSourceCode.trim()) {
      setCreateError('JS 插件必须提供源码。');
      return;
    }
    if ((createType === 'NATIVE_FFM' || createType === 'JAVA_SPI') && !createJarOrLibPath.trim()) {
      setCreateError('请提供 .so/.dylib/.dll 或 .jar 文件路径。');
      return;
    }
    setCreateError('');
    try {
      await createPlugin({
        name,
        type: createType,
        priority: Number(createPriority) || 100,
        failMode: createFailMode,
        timeoutMs: Math.max(1, Number(createTimeoutMs) || 50),
        stages: createStages,
        jarPath:
          createType === 'NATIVE_FFM' || createType === 'JAVA_SPI'
            ? createJarOrLibPath.trim()
            : undefined,
        className: createClassName.trim() || undefined,
        compileNative: createType === 'JAVA_SPI' ? createCompileNative : undefined,
        sourceCode: createType === 'JS' ? createSourceCode : undefined,
        config: cfg.value,
      });
      notify(`已注册插件 ${name}`, 'success');
      setCreateOpen(false);
      setCreateName('');
      setCreateJarOrLibPath('');
      setCreateClassName('');
      setCreateSourceCode('');
      await refreshAll();
    } catch (e: any) {
      setCreateError(e?.message || '创建插件失败');
    }
  }

  async function handleDryRunSubmit(ev: React.FormEvent) {
    ev.preventDefault();
    if (!dryPluginId.trim()) {
      setDryError('请输入要试运行的插件 ID 或名称。');
      return;
    }
    const headersParsed = parseJsonObject(dryHeadersText, '请求头');
    if (!headersParsed.ok) {
      setDryError(headersParsed.error);
      return;
    }
    const bodyParsed = parseJsonObject(dryBodyText, '请求体');
    if (!bodyParsed.ok) {
      setDryError(bodyParsed.error);
      return;
    }
    const cfgParsed = parseJsonObject(dryConfigText, '插件配置');
    if (!cfgParsed.ok) {
      setDryError(cfgParsed.error);
      return;
    }
    setDryError('');
    try {
      const stringHeaders: Record<string, string> = {};
      for (const [k, v] of Object.entries(headersParsed.value)) {
        stringHeaders[k] = String(v);
      }
      const res = await dryRunPlugin({
        pluginId: dryPluginId.trim(),
        stage: dryStage,
        headers: stringHeaders,
        requestBody: bodyParsed.value,
        config: cfgParsed.value,
      });
      setDryResult(res);
      notify(`试运行完成：${res.action} (HTTP ${res.httpStatus})`, 'success');
    } catch (e: any) {
      setDryError(e?.message || '试运行失败');
    }
  }

  const enabledCount = plugins.filter((p) => p.enabled).length;
  const totalNativePlugins = nativeState.items.reduce((acc, item) => acc + item.pluginIds.length, 0);
  const totalInFlight = nativeState.items.reduce((acc, item) => acc + item.inFlightCalls, 0);

  return (
    <>
      <PageHeader
        title="插件管理"
        description="管理 8 阶段微内核插件与 JDK 25 FFM Native 共享库（.so / .dylib / .dll），支持无修改 .jar 编译为 --shared 共享库后热加载、零中断热替换与确定性卸载。"
        actions={
          canWrite ? (
            <>
              <UiButton
                variant="primary"
                onClick={() => {
                  setLoadError('');
                  setLoadOpen(true);
                }}
              >
                + 热加载 Native / JAR
              </UiButton>
              <UiButton
                variant="secondary"
                onClick={() => {
                  setCreateError('');
                  setCreateOpen(true);
                }}
              >
                注册持久化插件
              </UiButton>
            </>
          ) : undefined
        }
      />

      <div {...stylex.props(ui.metricGrid)}>
        <MetricCard
          label="已挂载 Native 共享库"
          value={fmtInt(nativeState.items.length)}
          unit="个"
          note={`导出 ${fmtInt(totalNativePlugins)} 个插件 · 在途调用 ${fmtInt(totalInFlight)}`}
        />
        <MetricCard
          label="已注册插件"
          value={fmtInt(plugins.length)}
          unit="个"
          note={`其中 ${fmtInt(enabledCount)} 个已启用`}
        />
        <MetricCard
          label="当前运行模式"
          value={nativeState.nativeImageRuntime ? 'Native Image' : 'JVM'}
          note={
            nativeState.nativeImageRuntime
              ? '通过 JDK 25 FFM 动态加载/卸载共享库'
              : '同时支持 JVM URLClassLoader 与 JDK 25 FFM'
          }
        />
        <MetricCard
          label="JAR → --shared 编译器"
          value={nativeState.nativeImageCompilerAvailable ? '可用' : '不可用'}
          note={
            nativeState.nativeImageCompilerAvailable
              ? '检测到 GraalVM native-image，可在线编译 .jar'
              : '未检测到 native-image，仅可加载预编译共享库'
          }
        />
      </div>

      <Tabs value={tab} onValueChange={(v) => setTab(v as 'native' | 'registry' | 'dryrun')}>
        <TabsList {...stylex.props(ui.tabsList)}>
          <TabsTab
            value="native"
            className={(state) =>
              stylex.props(ui.tabsTab, state.selected && ui.tabsTabSelected).className
            }
          >
            Native FFM 运行时 ({nativeState.items.length})
          </TabsTab>
          <TabsTab
            value="registry"
            className={(state) =>
              stylex.props(ui.tabsTab, state.selected && ui.tabsTabSelected).className
            }
          >
            已注册插件 ({plugins.length})
          </TabsTab>
          <TabsTab
            value="dryrun"
            className={(state) =>
              stylex.props(ui.tabsTab, state.selected && ui.tabsTabSelected).className
            }
          >
            在线试运行
          </TabsTab>
        </TabsList>

        <TabsPanel value="native" style={{ marginTop: '16px' }}>
          <div {...stylex.props(ui.card)}>
            <div {...stylex.props(ui.cardHeader)}>
              <div>
                <h3 {...stylex.props(ui.cardTitle)}>已挂载 FFM 共享库</h3>
                <p {...stylex.props(ui.cardSubtitle)}>
                  通过 Copy-on-Load 版本化暂存加载；热重载与卸载均会先排空在途请求，再销毁 GraalVM Isolate 并关闭 Arena 句柄。
                </p>
              </div>
              <UiButton size="sm" variant="outline" disabled={loading} onClick={refreshAll}>
                刷新状态
              </UiButton>
            </div>
            <div {...stylex.props(ui.cardBodyFlush)}>
              {nativeState.items.length === 0 ? (
                <EmptyBox
                  title="尚未挂载 Native FFM 共享库"
                  description="可直接热加载 .so / .dylib / .dll 共享库，或选择标准 GatewayPlugin .jar 由 GraalVM native-image --shared 自动编译并挂载。"
                  action={
                    canWrite ? (
                      <UiButton
                        variant="primary"
                        onClick={() => {
                          setLoadError('');
                          setLoadOpen(true);
                        }}
                      >
                        立即加载共享库 / JAR
                      </UiButton>
                    ) : undefined
                  }
                />
              ) : (
                <div {...stylex.props(ui.tableWrap)}>
                  <table {...stylex.props(ui.table)}>
                    <thead>
                      <tr>
                        <th {...stylex.props(ui.th)}>共享库路径</th>
                        <th {...stylex.props(ui.th)}>ABI 模式</th>
                        <th {...stylex.props(ui.th)}>导出插件 ID</th>
                        <th {...stylex.props(ui.th, ui.thNum)}>在途调用</th>
                        <th {...stylex.props(ui.th)}>挂载时间</th>
                        <th {...stylex.props(ui.th, ui.tdActions)}>操作</th>
                      </tr>
                    </thead>
                    <tbody>
                      {nativeState.items.map((item) => (
                        <tr key={item.libraryPath}>
                          <td {...stylex.props(ui.td, ui.tdMono)}>
                            <div {...stylex.props(ui.inlineRow)}>
                              <span>{item.libraryPath}</span>
                              <UiButton
                                size="sm"
                                variant="outline"
                                onClick={() => copyToClipboard(item.libraryPath)}
                              >
                                复制
                              </UiButton>
                            </div>
                          </td>
                          <td {...stylex.props(ui.td)}>
                            <UiBadge
                              tone={item.abiMode === 'GRAAL_ISOLATE' ? 'info' : 'neutral'}
                              label={abiModeLabel(item.abiMode)}
                            />
                          </td>
                          <td {...stylex.props(ui.td)}>
                            <div {...stylex.props(ui.inlineRow)}>
                              {item.pluginIds.map((pid) => (
                                <UiBadge key={pid} tone="success" label={pid} />
                              ))}
                            </div>
                          </td>
                          <td {...stylex.props(ui.td, ui.tdNum)}>
                            {fmtInt(item.inFlightCalls)}
                          </td>
                          <td {...stylex.props(ui.td)}>{fmtDate(item.loadedAtMillis)}</td>
                          <td {...stylex.props(ui.td, ui.tdActions)}>
                            {canWrite && (
                              <div {...stylex.props(ui.inlineRow)} style={{ justifyContent: 'flex-end' }}>
                                <UiButton
                                  size="sm"
                                  variant="secondary"
                                  onClick={() => handleHotReloadLib(item.libraryPath)}
                                >
                                  热重载
                                </UiButton>
                                <UiButton
                                  size="sm"
                                  variant="destructive"
                                  onClick={() => setUnloadTarget(item)}
                                >
                                  卸载
                                </UiButton>
                              </div>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        </TabsPanel>

        <TabsPanel value="registry" style={{ marginTop: '16px' }}>
          <div {...stylex.props(ui.card)}>
            <div {...stylex.props(ui.cardHeader)}>
              <div>
                <h3 {...stylex.props(ui.cardTitle)}>工作空间插件注册表</h3>
                <p {...stylex.props(ui.cardSubtitle)}>
                  包含 NATIVE_FFM、JAVA_SPI、JS 沙箱、Webhook 与内置插件；删除最后一个引用某动态库的记录时会自动卸载对应运行时句柄。
                </p>
              </div>
            </div>
            <div {...stylex.props(ui.cardBodyFlush)}>
              {plugins.length === 0 ? (
                <EmptyBox
                  title="暂无已注册插件"
                  description="点击右上角“注册持久化插件”将 Native 共享库、JAR、JS 或 Webhook 插件写入工作空间配置。"
                />
              ) : (
                <div {...stylex.props(ui.tableWrap)}>
                  <table {...stylex.props(ui.table)}>
                    <thead>
                      <tr>
                        <th {...stylex.props(ui.th)}>插件名称 / 路径</th>
                        <th {...stylex.props(ui.th)}>类型</th>
                        <th {...stylex.props(ui.th)}>执行阶段</th>
                        <th {...stylex.props(ui.th, ui.thNum)}>优先级</th>
                        <th {...stylex.props(ui.th)}>容错策略</th>
                        <th {...stylex.props(ui.th)}>启用状态</th>
                        <th {...stylex.props(ui.th, ui.tdActions)}>操作</th>
                      </tr>
                    </thead>
                    <tbody>
                      {plugins.map((p) => (
                        <tr key={p.id}>
                          <td {...stylex.props(ui.td)}>
                            <span {...stylex.props(ui.cellPrimary)}>{p.name}</span>
                            <span {...stylex.props(ui.cellSub, ui.tdMono)}>
                              {p.jarPath || p.className || `v${p.version}`}
                            </span>
                          </td>
                          <td {...stylex.props(ui.td)}>
                            <UiBadge tone={pluginTypeTone(p.type)} label={pluginTypeLabel(p.type)} />
                          </td>
                          <td {...stylex.props(ui.td)}>
                            <div {...stylex.props(ui.inlineRow)}>
                              {p.stages.map((st) => (
                                <UiBadge key={st} tone="neutral" label={st} />
                              ))}
                            </div>
                          </td>
                          <td {...stylex.props(ui.td, ui.tdNum)}>{fmtInt(p.priority)}</td>
                          <td {...stylex.props(ui.td)}>
                            <UiBadge
                              tone={p.failMode === 'FAIL_OPEN' ? 'warning' : 'neutral'}
                              label={p.failMode}
                            />
                          </td>
                          <td {...stylex.props(ui.td)}>
                            {canWrite ? (
                              <UiSwitch
                                checked={p.enabled}
                                onChange={(next) => handleToggleEnabled(p, next)}
                                label={p.enabled ? '已启用' : '已停用'}
                              />
                            ) : (
                              <UiBadge status={p.enabled ? 'enabled' : 'disabled'} />
                            )}
                          </td>
                          <td {...stylex.props(ui.td, ui.tdActions)}>
                            {canWrite && (
                              <div {...stylex.props(ui.inlineRow)} style={{ justifyContent: 'flex-end' }}>
                                {(p.type === 'NATIVE_FFM' || p.type === 'JAVA_SPI') && (
                                  <UiButton
                                    size="sm"
                                    variant="outline"
                                    onClick={() => handleUnloadRegisteredRuntime(p)}
                                  >
                                    卸载运行时
                                  </UiButton>
                                )}
                                <UiButton
                                  size="sm"
                                  variant="destructive"
                                  onClick={() => setDeleteTarget(p)}
                                >
                                  删除
                                </UiButton>
                              </div>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        </TabsPanel>

        <TabsPanel value="dryrun" style={{ marginTop: '16px' }}>
          <div {...stylex.props(ui.grid2)}>
            <div {...stylex.props(ui.card)}>
              <div {...stylex.props(ui.cardHeader)}>
                <div>
                  <h3 {...stylex.props(ui.cardTitle)}>在线试运行 (Dry-Run)</h3>
                  <p {...stylex.props(ui.cardSubtitle)}>
                    对已挂载的 Native FFM、JAR 或内置插件执行模拟请求，不影响线上流量与账单。
                  </p>
                </div>
              </div>
              <div {...stylex.props(ui.cardBody)}>
                <form onSubmit={handleDryRunSubmit} {...stylex.props(ui.formGrid)}>
                  <div {...stylex.props(ui.formRow)}>
                    <Field {...stylex.props(ui.field)}>
                      <FieldLabel {...stylex.props(ui.fieldLabel)}>插件 ID / 名称</FieldLabel>
                      <Input
                        {...stylex.props(ui.input)}
                        value={dryPluginId}
                        onChange={(e) => setDryPluginId(e.target.value)}
                        placeholder="例如 ext-quota-header 或 pii-guardrail"
                      />
                    </Field>
                    <Field {...stylex.props(ui.field)}>
                      <FieldLabel {...stylex.props(ui.fieldLabel)}>执行阶段</FieldLabel>
                      <Select value={dryStage} onValueChange={(v) => v && setDryStage(String(v))}>
                        <SelectTrigger {...stylex.props(ui.selectTrigger)}>
                          <SelectValue>{dryStage}</SelectValue>
                          <span aria-hidden="true">▾</span>
                        </SelectTrigger>
                        <SelectContent>
                          {PLUGIN_STAGES.map((st) => (
                            <SelectItem
                              key={st}
                              value={st}
                              className={(state) =>
                                stylex.props(
                                  ui.selectItem,
                                  state.selected && ui.selectItemActive,
                                ).className
                              }
                            >
                              <span>{st}</span>
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </Field>
                  </div>

                  <Field {...stylex.props(ui.field)}>
                    <FieldLabel {...stylex.props(ui.fieldLabel)}>模拟请求头 (JSON)</FieldLabel>
                    <textarea
                      {...stylex.props(ui.textarea, ui.tdMono)}
                      value={dryHeadersText}
                      onChange={(e) => setDryHeadersText(e.target.value)}
                    />
                  </Field>

                  <Field {...stylex.props(ui.field)}>
                    <FieldLabel {...stylex.props(ui.fieldLabel)}>模拟请求体 (JSON)</FieldLabel>
                    <textarea
                      {...stylex.props(ui.textarea, ui.tdMono)}
                      value={dryBodyText}
                      onChange={(e) => setDryBodyText(e.target.value)}
                    />
                  </Field>

                  <Field {...stylex.props(ui.field)}>
                    <FieldLabel {...stylex.props(ui.fieldLabel)}>插件配置覆盖 (JSON)</FieldLabel>
                    <textarea
                      {...stylex.props(ui.textarea, ui.tdMono)}
                      value={dryConfigText}
                      onChange={(e) => setDryConfigText(e.target.value)}
                    />
                  </Field>

                  {dryError && <p {...stylex.props(ui.fieldError)}>{dryError}</p>}

                  <div>
                    <UiButton type="submit" variant="primary" disabled={!canWrite}>
                      执行试运行
                    </UiButton>
                  </div>
                </form>
              </div>
            </div>

            <div {...stylex.props(ui.card)}>
              <div {...stylex.props(ui.cardHeader)}>
                <div>
                  <h3 {...stylex.props(ui.cardTitle)}>试运行结果</h3>
                  <p {...stylex.props(ui.cardSubtitle)}>
                    查看插件返回动作（CONTINUE / REJECT / SHORT_CIRCUIT）与上下文变更详情。
                  </p>
                </div>
              </div>
              <div {...stylex.props(ui.cardBody)}>
                {!dryResult ? (
                  <EmptyBox
                    title="尚未执行试运行"
                    description="在左侧填写目标插件 ID 与模拟报文后点击“执行试运行”。"
                  />
                ) : (
                  <div {...stylex.props(ui.stackSm)}>
                    <div {...stylex.props(ui.inlineRow)}>
                      <UiBadge
                        tone={
                          dryResult.action === 'CONTINUE'
                            ? 'success'
                            : dryResult.action === 'REJECT'
                              ? 'danger'
                              : 'warning'
                        }
                        label={`${dryResult.action} · HTTP ${dryResult.httpStatus}`}
                      />
                      <UiBadge tone="neutral" label={`阶段 ${dryResult.stage}`} />
                      {dryResult.mutatedModelAlias && (
                        <UiBadge tone="info" label={`模型别名 ${dryResult.mutatedModelAlias}`} />
                      )}
                    </div>
                    <pre {...stylex.props(ui.codeBlock)}>
                      {JSON.stringify(dryResult, null, 2)}
                    </pre>
                  </div>
                )}
              </div>
            </div>
          </div>
        </TabsPanel>
      </Tabs>

      {/* Dialog: Hot-Load Native Shared Library or Compile & Load JAR */}
      <UiDialog
        open={loadOpen}
        onOpenChange={setLoadOpen}
        wide
        title="热加载 Native 共享库 / 编译加载 JAR"
        description="支持直接挂载已编译的 .so / .dylib / .dll 共享库，或对标准 GatewayPlugin .jar 自动执行 native-image --shared 编译并热加载。"
        footer={
          <>
            <UiButton variant="outline" onClick={() => setLoadOpen(false)}>
              取消
            </UiButton>
            <UiButton
              type="submit"
              form="load-native-plugin-form"
              variant="primary"
              disabled={loadBusy}
            >
              {loadBusy
                ? loadMode === 'jar'
                  ? '正在编译并加载…'
                  : '正在挂载…'
                : loadMode === 'jar'
                  ? '编译并热加载'
                  : '立即挂载'}
            </UiButton>
          </>
        }
      >
        <form
          id="load-native-plugin-form"
          onSubmit={handleLoadNativeSubmit}
          {...stylex.props(ui.formGrid)}
        >
          <div {...stylex.props(ui.inlineRow)}>
            <UiButton
              size="sm"
              variant={loadMode === 'lib' ? 'primary' : 'outline'}
              onClick={() => {
                setLoadMode('lib');
                setLoadError('');
              }}
            >
              直接加载共享库 (.so / .dylib / .dll)
            </UiButton>
            <UiButton
              size="sm"
              variant={loadMode === 'jar' ? 'primary' : 'outline'}
              onClick={() => {
                setLoadMode('jar');
                setLoadError('');
              }}
            >
              无修改 .jar 编译为 --shared 并加载
            </UiButton>
          </div>

          {loadMode === 'jar' && !nativeState.nativeImageCompilerAvailable && (
            <div {...stylex.props(ui.callout, ui.calloutWarning)}>
              当前控制面节点未检测到 GraalVM <code {...stylex.props(ui.inlineCode)}>native-image</code> 命令。请先在构建机执行{' '}
              <code {...stylex.props(ui.inlineCode)}>./gradlew :gateway-common:compilePluginSharedLib -PpluginJar=...</code>{' '}
              生成共享库后再选择直接加载模式。
            </div>
          )}

          <Field {...stylex.props(ui.field)}>
            <FieldLabel {...stylex.props(ui.fieldLabel)}>
              {loadMode === 'lib' ? '共享库绝对路径 (libPath)' : '插件 JAR 绝对路径 (jarPath)'}
            </FieldLabel>
            <Input
              {...stylex.props(ui.input, ui.tdMono)}
              value={loadPath}
              onChange={(e) => setLoadPath(e.target.value)}
              placeholder={
                loadMode === 'lib'
                  ? '/opt/ai-gateway/plugins/libmy_plugin.so'
                  : '/opt/ai-gateway/plugins/my-plugin.jar'
              }
            />
            <FieldDescription {...stylex.props(ui.fieldHint)}>
              {loadMode === 'lib'
                ? '同路径重复加载会自动执行 Copy-on-Load 热替换，排空旧 Isolate 在途请求后安全释放。'
                : '将自动扫描 JAR 内 META-INF/services/com.jihuayu.aigateway.plugin.GatewayPlugin 并编译为共享库后挂载（耗时约 20–60 秒）。'}
            </FieldDescription>
          </Field>

          {loadMode === 'jar' && (
            <Field {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>
                输出共享库路径（可选 outputLibPath）
              </FieldLabel>
              <Input
                {...stylex.props(ui.input, ui.tdMono)}
                value={outputLibPath}
                onChange={(e) => setOutputLibPath(e.target.value)}
                placeholder="留空则输出到 JAR 同级目录"
              />
            </Field>
          )}

          {loadError && <p {...stylex.props(ui.fieldError)}>{loadError}</p>}
        </form>
      </UiDialog>

      {/* Dialog: Register Persistent Plugin */}
      <UiDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        wide
        title="注册持久化插件"
        description="将插件元数据写入数据库并立即发布到数据面快照；NATIVE_FFM 与 JAVA_SPI 类型会在保存时自动触发运行时加载。"
        footer={
          <>
            <UiButton variant="outline" onClick={() => setCreateOpen(false)}>
              取消
            </UiButton>
            <UiButton type="submit" form="create-plugin-form" variant="primary">
              保存并发布
            </UiButton>
          </>
        }
      >
        <form
          id="create-plugin-form"
          onSubmit={handleCreatePluginSubmit}
          {...stylex.props(ui.formGrid)}
        >
          <div {...stylex.props(ui.formRow)}>
            <Field {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>插件名称</FieldLabel>
              <Input
                {...stylex.props(ui.input)}
                value={createName}
                onChange={(e) => setCreateName(e.target.value)}
                placeholder="例如 ext-quota-header"
              />
            </Field>
            <Field {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>插件类型</FieldLabel>
              <Select
                value={createType}
                onValueChange={(v) => v && setCreateType(v as PluginType)}
              >
                <SelectTrigger {...stylex.props(ui.selectTrigger)}>
                  <SelectValue>{pluginTypeLabel(createType)}</SelectValue>
                  <span aria-hidden="true">▾</span>
                </SelectTrigger>
                <SelectContent>
                  {(['NATIVE_FFM', 'JAVA_SPI', 'JS', 'WEBHOOK'] as PluginType[]).map((t) => (
                    <SelectItem
                      key={t}
                      value={t}
                      className={(state) =>
                        stylex.props(ui.selectItem, state.selected && ui.selectItemActive).className
                      }
                    >
                      <span>{pluginTypeLabel(t)}</span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </Field>
          </div>

          <div {...stylex.props(ui.formRow)}>
            <Field {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>优先级</FieldLabel>
              <Input
                {...stylex.props(ui.input)}
                type="number"
                value={createPriority}
                onChange={(e) => setCreatePriority(e.target.value)}
              />
            </Field>
            <Field {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>超时 (ms)</FieldLabel>
              <Input
                {...stylex.props(ui.input)}
                type="number"
                value={createTimeoutMs}
                onChange={(e) => setCreateTimeoutMs(e.target.value)}
              />
            </Field>
            <Field {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>容错策略</FieldLabel>
              <Select
                value={createFailMode}
                onValueChange={(v) => v && setCreateFailMode(v as FailMode)}
              >
                <SelectTrigger {...stylex.props(ui.selectTrigger)}>
                  <SelectValue>{createFailMode}</SelectValue>
                  <span aria-hidden="true">▾</span>
                </SelectTrigger>
                <SelectContent>
                  {(['FAIL_CLOSED', 'FAIL_OPEN'] as FailMode[]).map((fm) => (
                    <SelectItem
                      key={fm}
                      value={fm}
                      className={(state) =>
                        stylex.props(ui.selectItem, state.selected && ui.selectItemActive).className
                      }
                    >
                      <span>{fm}</span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </Field>
          </div>

          {(createType === 'NATIVE_FFM' || createType === 'JAVA_SPI') && (
            <>
              <Field {...stylex.props(ui.field)}>
                <FieldLabel {...stylex.props(ui.fieldLabel)}>
                  {createType === 'NATIVE_FFM'
                    ? '共享库路径（或待编译 .jar 路径）'
                    : 'JAR 文件路径'}
                </FieldLabel>
                <Input
                  {...stylex.props(ui.input, ui.tdMono)}
                  value={createJarOrLibPath}
                  onChange={(e) => setCreateJarOrLibPath(e.target.value)}
                  placeholder="/opt/ai-gateway/plugins/libext_quota_plugin.so"
                />
              </Field>
              <Field {...stylex.props(ui.field)}>
                <FieldLabel {...stylex.props(ui.fieldLabel)}>实现类名（可选）</FieldLabel>
                <Input
                  {...stylex.props(ui.input, ui.tdMono)}
                  value={createClassName}
                  onChange={(e) => setCreateClassName(e.target.value)}
                  placeholder="com.example.MyGatewayPlugin"
                />
              </Field>
              {createType === 'JAVA_SPI' && (
                <UiSwitch
                  checked={createCompileNative}
                  onChange={setCreateCompileNative}
                  label="自动通过 GraalVM native-image --shared 编译并转换为 NATIVE_FFM"
                />
              )}
            </>
          )}

          {createType === 'JS' && (
            <Field {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>JavaScript 源码</FieldLabel>
              <textarea
                {...stylex.props(ui.textarea, ui.tdMono)}
                value={createSourceCode}
                onChange={(e) => setCreateSourceCode(e.target.value)}
                placeholder="function onRequest(ctx, cfg) { return { action: 'CONTINUE' }; }"
              />
            </Field>
          )}

          <div {...stylex.props(ui.field)}>
            <span {...stylex.props(ui.fieldLabel)}>挂载阶段</span>
            <div {...stylex.props(ui.grid3)}>
              {PLUGIN_STAGES.map((st) => (
                <UiCheckboxRow
                  key={st}
                  checked={createStages.includes(st)}
                  onChange={(checked) => toggleStage(st, checked)}
                  title={st}
                />
              ))}
            </div>
          </div>

          <Field {...stylex.props(ui.field)}>
            <FieldLabel {...stylex.props(ui.fieldLabel)}>默认配置 (JSON)</FieldLabel>
            <textarea
              {...stylex.props(ui.textarea, ui.tdMono)}
              value={createConfigText}
              onChange={(e) => setCreateConfigText(e.target.value)}
            />
          </Field>

          {createError && <p {...stylex.props(ui.fieldError)}>{createError}</p>}
        </form>
      </UiDialog>

      {/* Confirm Unload Native Library */}
      <UiConfirmDialog
        open={Boolean(unloadTarget)}
        onOpenChange={(open) => {
          if (!open) setUnloadTarget(null);
        }}
        title="卸载 Native FFM 共享库"
        description={
          unloadTarget
            ? `确认卸载 ${unloadTarget.libraryPath}？系统将先排空在途调用、销毁 GraalVM Isolate 并关闭 Arena 句柄；若该库覆盖了内置插件，将自动恢复基线实现。`
            : ''
        }
        confirmLabel="确认卸载"
        danger
        onConfirm={handleConfirmUnloadNative}
      />

      {/* Confirm Delete Registered Plugin */}
      <UiConfirmDialog
        open={Boolean(deleteTarget)}
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null);
        }}
        title="删除已注册插件"
        description={
          deleteTarget
            ? `确认删除插件 “${deleteTarget.name}”？若它是最后一个引用对应共享库或 JAR 的记录，系统将同步卸载其运行时句柄。`
            : ''
        }
        confirmLabel="确认删除"
        danger
        onConfirm={handleConfirmDeletePlugin}
      />
    </>
  );
}
