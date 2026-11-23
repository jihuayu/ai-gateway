import React, { useEffect, useState } from 'react';
import * as stylex from '@stylexjs/stylex';
import { Field, FieldDescription, FieldLabel } from '@ui-sdk/react/field';
import { Input } from '@ui-sdk/react/input';
import { ui } from '../styles/ui';
import { api, arr, fmtDate, fmtDuration, fmtInt, getSafe, items } from '../api/client';
import {
  EmptyBox,
  MetricCard,
  PageHeader,
  UiBadge,
  UiButton,
  UiConfirmDialog,
  UiDialog,
  UiSwitch,
  copyToClipboard,
  notify,
} from '../components/Primitives';

export function McpView({
  canWrite,
  refreshTick,
}: {
  canWrite: boolean;
  refreshTick: number;
}) {
  const [servers, setServers] = useState<any[]>([]);

  // Create / Edit modal
  const [modalOpen, setModalOpen] = useState(false);
  const [editingServer, setEditingServer] = useState<any | null>(null);
  const [alias, setAlias] = useState('');
  const [transport, setTransport] = useState<'streamable_http' | 'sse'>('streamable_http');
  const [endpointUrl, setEndpointUrl] = useState('');
  const [secret, setSecret] = useState('');
  const [allowedToolsInput, setAllowedToolsInput] = useState('');
  const [timeoutMs, setTimeoutMs] = useState('15000');
  const [enabled, setEnabled] = useState(true);
  const [formErr, setFormErr] = useState('');

  // Probe result modal
  const [probeResult, setProbeResult] = useState<any | null>(null);

  // Delete confirm
  const [deletingServer, setDeletingServer] = useState<any | null>(null);

  async function reload() {
    const res = await getSafe('/api/mcp-servers');
    if (res.ok) {
      setServers(items(res.data));
    } else {
      notify(res.error.message || '加载 MCP Server 列表失败', 'error');
    }
  }

  useEffect(() => {
    reload();
  }, [refreshTick]);

  function openNewServer() {
    setEditingServer(null);
    setAlias('');
    setTransport('streamable_http');
    setEndpointUrl('https://mcp.example.com/rpc');
    setSecret('');
    setAllowedToolsInput('search_docs, query_kb, create_ticket');
    setTimeoutMs('15000');
    setEnabled(true);
    setFormErr('');
    setModalOpen(true);
  }

  function openEditServer(srv: any) {
    setEditingServer(srv);
    setAlias(srv.alias || '');
    setTransport(srv.transport === 'sse' ? 'sse' : 'streamable_http');
    setEndpointUrl(srv.endpointUrl || srv.endpoint_url || '');
    setSecret('');
    setAllowedToolsInput(arr(srv.allowedTools || srv.allowed_tools).join(', '));
    setTimeoutMs(String(srv.timeoutMs ?? srv.timeout_ms ?? 15000));
    setEnabled(srv.enabled !== false);
    setFormErr('');
    setModalOpen(true);
  }

  async function saveServer() {
    setFormErr('');
    if (!alias.trim() || !endpointUrl.trim()) {
      setFormErr('请填写 MCP Server 别名与远端端点地址。');
      return;
    }
    const tools = allowedToolsInput
      .split(/[\n,]+/)
      .map((s) => s.trim())
      .filter(Boolean);
    const tMs = Math.max(500, Number(timeoutMs) || 15000);

    try {
      const body: Record<string, any> = {
        alias: alias.trim(),
        transport,
        endpointUrl: endpointUrl.trim(),
        allowedTools: tools,
        timeoutMs: tMs,
        enabled,
      };
      if (secret.trim()) {
        body.secret = secret.trim();
      }
      if (editingServer?.id) {
        await api(`/api/mcp-servers/${encodeURIComponent(editingServer.id)}`, {
          method: 'PATCH',
          body,
        });
        notify(`MCP Server “${alias}” 已更新`, 'success');
      } else {
        await api('/api/mcp-servers', { method: 'POST', body });
        notify(`MCP Server “${alias}” 已注册`, 'success');
      }
      setModalOpen(false);
      await reload();
    } catch (e: any) {
      setFormErr(e?.message || '保存 MCP Server 失败');
    }
  }

  async function runProbe(srv: any) {
    try {
      const r = await api(`/api/mcp-servers/${encodeURIComponent(srv.id)}/probe`, {
        method: 'POST',
      });
      setProbeResult(r);
      notify(`已完成对 ${srv.alias} 的连通性与安全策略探测`, 'success');
    } catch (e: any) {
      notify(e?.message || '探测 MCP Server 失败', 'error');
    }
  }

  async function handleDelete(srv: any) {
    try {
      await api(`/api/mcp-servers/${encodeURIComponent(srv.id)}`, { method: 'DELETE' });
      notify(`MCP Server “${srv.alias}” 已删除`, 'success');
      await reload();
    } catch (e: any) {
      notify(e?.message || '删除 MCP Server 失败', 'error');
    }
  }

  const enabledCount = servers.filter((s) => s.enabled !== false).length;
  const totalTools = servers.reduce(
    (acc, s) => acc + arr(s.allowedTools || s.allowed_tools).length,
    0,
  );
  const origin = typeof window !== 'undefined' ? window.location.origin : 'http://localhost:3000';
  const sampleAlias = servers[0]?.alias || 'knowledge-mcp';
  const sampleTool = arr(servers[0]?.allowedTools)[0] || 'search_docs';
  const mcpCurl = `curl "${origin}/mcp/${sampleAlias}" \\
  -H "Authorization: Bearer $AI_GATEWAY_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "${sampleTool}",
      "arguments": { "query": "micro-USD ledger" }
    }
  }'`;

  return (
    <>
      <PageHeader
        title="MCP 工具网关中心（Model Context Protocol）"
        description="统一托管企业内部 Streamable HTTP 与 SSE 远端 MCP Server。由网关集中执行 AES-GCM 凭据代填、SSRF 防护、Key 级工具白名单（allowed_mcp_tools）鉴权与调用参数审计。"
        actions={
          canWrite ? (
            <UiButton variant="primary" onClick={openNewServer}>
              注册 MCP Server
            </UiButton>
          ) : (
            <UiBadge tone="neutral" label="只读角色不可修改 MCP 配置" />
          )
        }
      />

      <div {...stylex.props(ui.metricGrid)}>
        <MetricCard
          label="已注册 MCP Server"
          value={fmtInt(servers.length)}
          unit="个"
          note={`启用中 ${fmtInt(enabledCount)} 个`}
        />
        <MetricCard
          label="受控开放工具总数"
          value={fmtInt(totalTools)}
          unit="项"
          note="未声明为空时默认允许该服务下全部工具"
        />
        <MetricCard
          label="Streamable HTTP 节点"
          value={fmtInt(servers.filter((s) => s.transport !== 'sse').length)}
          unit="个"
          note={`SSE 节点 ${fmtInt(servers.filter((s) => s.transport === 'sse').length)} 个`}
        />
        <MetricCard
          label="代理路由前缀"
          value="/mcp/{alias}"
          note="支持 JSON-RPC 2.0 initialize / tools/list / tools/call"
        />
      </div>

      <section {...stylex.props(ui.card)}>
        <div {...stylex.props(ui.cardHeader)}>
          <div>
            <h3 {...stylex.props(ui.cardTitle)}>工作空间 MCP Server 注册表</h3>
            <p {...stylex.props(ui.cardSubtitle)}>
              注册后可在「API Keys」页为每把项目密钥细粒度勾选允许调用的 `server:*` 或 `server:tool` 权限。
            </p>
          </div>
          <UiBadge tone="neutral" label={`${servers.length} 个服务`} />
        </div>
        <div {...stylex.props(ui.cardBodyFlush)}>
          {servers.length ? (
            <div {...stylex.props(ui.tableWrap)}>
              <table {...stylex.props(ui.table)}>
                <thead>
                  <tr>
                    <th {...stylex.props(ui.th)}>服务别名 / 传输协议</th>
                    <th {...stylex.props(ui.th)}>远端 Endpoint URL</th>
                    <th {...stylex.props(ui.th)}>允许暴露的工具白名单</th>
                    <th {...stylex.props(ui.th, ui.thNum)}>超时上限</th>
                    <th {...stylex.props(ui.th)}>状态</th>
                    <th {...stylex.props(ui.th)} />
                  </tr>
                </thead>
                <tbody>
                  {servers.map((srv) => {
                    const tools = arr(srv.allowedTools || srv.allowed_tools);
                    return (
                      <tr key={srv.id}>
                        <td {...stylex.props(ui.td)}>
                          <span {...stylex.props(ui.cellPrimary)}>{srv.alias}</span>
                          <span {...stylex.props(ui.cellSub, ui.tdMono)}>
                            {srv.transport || 'streamable_http'}
                          </span>
                        </td>
                        <td {...stylex.props(ui.td, ui.tdMono)}>
                          {srv.endpointUrl || srv.endpoint_url}
                        </td>
                        <td {...stylex.props(ui.td)}>
                          <div {...stylex.props(ui.inlineRow)}>
                            {tools.length ? (
                              tools.map((t: string) => (
                                <UiBadge key={t} tone="info" label={`${srv.alias}:${t}`} />
                              ))
                            ) : (
                              <UiBadge tone="neutral" label={`${srv.alias}:* (全部工具)`} />
                            )}
                          </div>
                        </td>
                        <td {...stylex.props(ui.td, ui.tdNum)}>
                          {fmtDuration(srv.timeoutMs ?? srv.timeout_ms ?? 15000)}
                        </td>
                        <td {...stylex.props(ui.td)}>
                          <UiBadge status={srv.enabled === false ? 'disabled' : 'enabled'} />
                        </td>
                        <td {...stylex.props(ui.td, ui.tdActions)}>
                          <div
                            {...stylex.props(ui.inlineRow)}
                            style={{ justifyContent: 'flex-end' }}
                          >
                            {canWrite && (
                              <>
                                <UiButton
                                  size="sm"
                                  variant="outline"
                                  onClick={() => runProbe(srv)}
                                >
                                  策略探测
                                </UiButton>
                                <UiButton
                                  size="sm"
                                  variant="secondary"
                                  onClick={() => openEditServer(srv)}
                                >
                                  编辑
                                </UiButton>
                                <UiButton
                                  size="sm"
                                  variant="outline"
                                  onClick={() => setDeletingServer(srv)}
                                >
                                  删除
                                </UiButton>
                              </>
                            )}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <EmptyBox
              title="尚未注册任何 MCP Server"
              description="注册企业内部知识库、工单系统或数据库查询 MCP 服务，通过网关统一收敛工具鉴权与参数安全审计。"
              action={
                canWrite && (
                  <UiButton size="sm" variant="primary" onClick={openNewServer}>
                    注册第一个 MCP Server
                  </UiButton>
                )
              }
            />
          )}
        </div>
      </section>

      <section {...stylex.props(ui.card)}>
        <div {...stylex.props(ui.cardHeader)}>
          <div>
            <h3 {...stylex.props(ui.cardTitle)}>MCP 代理端点调用示例</h3>
            <p {...stylex.props(ui.cardSubtitle)}>
              客户端或 Agent 仅需携带项目 API Key 访问网关 `/mcp/{'{alias}'}`，网关自动校验工具 ACL 并注入远端加密凭据。
            </p>
          </div>
          <UiButton size="sm" variant="secondary" onClick={() => copyToClipboard(mcpCurl)}>
            复制调用命令
          </UiButton>
        </div>
        <div {...stylex.props(ui.cardBody)}>
          <pre {...stylex.props(ui.codeBlock)}>
            <code>{mcpCurl}</code>
          </pre>
        </div>
      </section>

      {/* Create / Edit MCP Server Modal */}
      <UiDialog
        open={modalOpen}
        onOpenChange={setModalOpen}
        title={editingServer ? `编辑 MCP Server：${editingServer.alias}` : '注册 MCP Server'}
        description="远端鉴权密钥采用 AES-GCM 加密落盘；变更通过 Redis Pub/Sub 秒级同步至所有数据面节点。"
        wide
        footer={
          <>
            <UiButton variant="outline" onClick={() => setModalOpen(false)}>
              取消
            </UiButton>
            <UiButton variant="primary" onClick={saveServer}>
              {editingServer ? '保存更新' : '立即注册'}
            </UiButton>
          </>
        }
      >
        <div {...stylex.props(ui.formGrid)}>
          <div {...stylex.props(ui.formRow)}>
            <Field name="mcpAlias" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>
                MCP 服务别名（路由标识 `/mcp/{'{alias}'}`）
              </FieldLabel>
              <Input
                value={alias}
                onValueChange={setAlias}
                readOnly={!!editingServer}
                placeholder="例如：knowledge-mcp"
                {...stylex.props(ui.input)}
              />
            </Field>
            <div {...stylex.props(ui.field)}>
              <label {...stylex.props(ui.fieldLabel)}>传输协议（Transport）</label>
              <select
                value={transport}
                onChange={(e) => setTransport(e.target.value as 'streamable_http' | 'sse')}
                disabled={!!editingServer}
                {...stylex.props(ui.input)}
              >
                <option value="streamable_http">Streamable HTTP (streamable_http)</option>
                <option value="sse">Server-Sent Events (sse)</option>
              </select>
            </div>
          </div>

          <Field name="mcpEndpoint" {...stylex.props(ui.field)}>
            <FieldLabel {...stylex.props(ui.fieldLabel)}>远端 Endpoint URL</FieldLabel>
            <Input
              type="url"
              value={endpointUrl}
              onValueChange={setEndpointUrl}
              placeholder="https://mcp.example.com/rpc"
              {...stylex.props(ui.input)}
            />
            <FieldDescription {...stylex.props(ui.fieldHint)}>
              网关自动执行 DNS 与 SSRF 策略校验，阻断未授权的私网与云元数据地址。
            </FieldDescription>
          </Field>

          <div {...stylex.props(ui.formRow)}>
            <Field name="mcpSecret" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>
                {editingServer ? '远端 Bearer 密钥（留空保持不变）' : '远端 Bearer 密钥（可选）'}
              </FieldLabel>
              <Input
                type="password"
                value={secret}
                onValueChange={setSecret}
                placeholder="转发时由网关自动注入 Authorization 头"
                {...stylex.props(ui.input)}
              />
            </Field>
            <Field name="mcpTimeout" {...stylex.props(ui.field)}>
              <FieldLabel {...stylex.props(ui.fieldLabel)}>超时时间（ms，≥ 500）</FieldLabel>
              <Input
                type="number"
                min="500"
                step="500"
                value={timeoutMs}
                onValueChange={setTimeoutMs}
                {...stylex.props(ui.input)}
              />
            </Field>
          </div>

          <div {...stylex.props(ui.field)}>
            <label {...stylex.props(ui.fieldLabel)}>
              允许暴露的工具名称白名单（逗号或换行分隔，留空代表允许该服务下全部工具）
            </label>
            <textarea
              value={allowedToolsInput}
              onChange={(e) => setAllowedToolsInput(e.target.value)}
              placeholder="search_docs, query_kb, create_ticket"
              {...stylex.props(ui.textarea)}
            />
          </div>

          <UiSwitch
            checked={enabled}
            onChange={setEnabled}
            label={enabled ? '服务状态：已启用' : '服务状态：已禁用'}
          />

          {formErr && <p {...stylex.props(ui.fieldError)}>{formErr}</p>}
        </div>
      </UiDialog>

      {/* Probe Result Dialog */}
      <UiDialog
        open={!!probeResult}
        onOpenChange={(o) => !o && setProbeResult(null)}
        title="MCP Server 探测结果"
        footer={
          <UiButton variant="primary" onClick={() => setProbeResult(null)}>
            关闭
          </UiButton>
        }
      >
        {probeResult && (
          <div {...stylex.props(ui.stackSm)}>
            <div {...stylex.props(ui.inlineRow)}>
              <UiBadge status={probeResult.status} />
              <span {...stylex.props(ui.mutedText)}>
                校验耗时 {fmtDuration(probeResult.latencyMs)} · {fmtDate(probeResult.testedAt)}
              </span>
            </div>
            <dl {...stylex.props(ui.detailGrid)}>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>服务别名 / 协议</dt>
                <dd {...stylex.props(ui.detailDd)}>
                  {probeResult.alias} ({probeResult.transport})
                </dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>Endpoint URL</dt>
                <dd {...stylex.props(ui.detailDd, ui.tdMono)}>{probeResult.endpointUrl}</dd>
              </div>
              <div {...stylex.props(ui.detailItem)}>
                <dt {...stylex.props(ui.detailDt)}>诊断摘要</dt>
                <dd {...stylex.props(ui.detailDd)}>{probeResult.message}</dd>
              </div>
            </dl>
            <div>
              <span {...stylex.props(ui.fieldLabel)}>可授权给 API Key 的工具权限标识符：</span>
              <div {...stylex.props(ui.inlineRow)} style={{ marginTop: '6px' }}>
                <UiBadge tone="info" label={`${probeResult.alias}:*`} />
                {arr(probeResult.allowedTools).map((t: string) => (
                  <UiBadge key={t} tone="neutral" label={`${probeResult.alias}:${t}`} />
                ))}
              </div>
            </div>
          </div>
        )}
      </UiDialog>

      {/* Delete Confirm */}
      <UiConfirmDialog
        open={!!deletingServer}
        onOpenChange={(o) => !o && setDeletingServer(null)}
        title="删除 MCP Server"
        description={
          deletingServer
            ? `确认删除 MCP Server “${deletingServer.alias}”？删除后数据面 /mcp/${deletingServer.alias} 路由将立即停止响应。`
            : ''
        }
        confirmLabel="确认删除"
        danger
        onConfirm={async () => {
          if (deletingServer) await handleDelete(deletingServer);
        }}
      />
    </>
  );
}
