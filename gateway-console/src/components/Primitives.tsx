import React from 'react';
import * as stylex from '@stylexjs/stylex';
import { Button } from '@ui-sdk/react/button';
import {
  SegmentedControl,
  SegmentedControlIndicator,
  SegmentedControlItem,
} from '@ui-sdk/react/segmented-control';
import {
  Meter,
  MeterIndicator,
  MeterLabel,
  MeterTrack,
  MeterValue,
} from '@ui-sdk/react/meter';
import {
  Timeline,
  TimelineConnector,
  TimelineDescription,
  TimelineItem,
  TimelineMarker,
  TimelineTime,
  TimelineTitle,
} from '@ui-sdk/react/timeline';
import {
  Dialog,
  DialogBackdrop,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from '@ui-sdk/react/dialog';
import {
  AlertDialog,
  AlertDialogBackdrop,
  AlertDialogClose,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogTitle,
} from '@ui-sdk/react/alert-dialog';
import {
  Toast,
  ToastClose,
  ToastDescription,
  ToastRegion,
  ToastTitle,
  createToaster,
} from '@ui-sdk/react/toast';
import { Checkbox, CheckboxIndicator } from '@ui-sdk/react/checkbox';
import { Switch, SwitchThumb } from '@ui-sdk/react/switch';
import { createAgent } from '@ui-sdk/react/agent';
import { ui } from '../styles/ui';
import { badgeTone, fmtDate, fmtDuration, fmtInt, fmtMoney, statusText, type BadgeTone } from '../api/client';

// 1. UI SDK Runtime Agent Bridge (WebMCP / Agent introspection ready)
export const consoleAgent = createAgent({
  allow: ({ component, action }) =>
    ['input', 'select', 'segmented-control', 'tabs', 'checkbox', 'switch'].includes(component) ||
    ['setValue', 'select', 'toggle'].includes(action),
});

// 2. Global UI SDK Toaster & Region
export const toaster = createToaster();

export function notify(title: string, kind: 'info' | 'success' | 'error' = 'info', description?: string) {
  toaster(title, {
    description,
    type: kind === 'error' ? 'error' : kind === 'success' ? 'success' : 'info',
  });
}

export function UiToastRegion() {
  return (
    <ToastRegion toaster={toaster}>
      {() => (
        <Toast {...stylex.props(ui.toastCard)}>
          <div {...stylex.props(ui.stackSm)}>
            <ToastTitle style={{ fontWeight: 600, fontSize: '13px' }} />
            <ToastDescription style={{ fontSize: '12px', color: 'var(--fg-muted)' }} />
          </div>
          <ToastClose
            {...stylex.props(ui.btn, ui.btnSecondary, ui.btnSmall)}
            aria-label="关闭通知"
          >
            ✕
          </ToastClose>
        </Toast>
      )}
    </ToastRegion>
  );
}

// 3. UI SDK Button with shadcn/ui StyleX variants & automatic async pending lock
export function UiButton({
  variant = 'primary',
  size = 'md',
  block = false,
  disabled = false,
  type = 'button',
  onClick,
  children,
  ...rest
}: {
  variant?: 'primary' | 'secondary' | 'outline' | 'destructive';
  size?: 'md' | 'sm';
  block?: boolean;
  disabled?: boolean;
  type?: 'button' | 'submit' | 'reset';
  onClick?: (ev: React.MouseEvent<HTMLElement>) => unknown;
  children: React.ReactNode;
  [key: string]: any;
}) {
  return (
    <Button
      type={type}
      disabled={disabled}
      pendingAnnouncement="处理中"
      onClick={onClick ? (ev) => onClick(ev) : undefined}
      className={(state) =>
        stylex.props(
          ui.btn,
          variant === 'primary' && ui.btnPrimary,
          variant === 'secondary' && ui.btnSecondary,
          variant === 'outline' && ui.btnOutline,
          variant === 'destructive' && ui.btnDestructive,
          size === 'sm' && ui.btnSmall,
          block && ui.btnBlock,
          (disabled || state.pending) && ui.btnDisabled,
        ).className
      }
      {...rest}
    >
      {children}
    </Button>
  );
}

// 4. shadcn/ui-style Status Badge
export function UiBadge({
  status,
  label,
  tone,
}: {
  status?: unknown;
  label?: string;
  tone?: BadgeTone;
}) {
  const t = tone || badgeTone(status);
  return (
    <span
      {...stylex.props(
        ui.badge,
        t === 'success' && ui.badgeSuccess,
        t === 'warning' && ui.badgeWarning,
        t === 'danger' && ui.badgeDanger,
        t === 'neutral' && ui.badgeNeutral,
        t === 'info' && ui.badgeInfo,
      )}
    >
      {label ?? statusText(status)}
    </span>
  );
}

// 5. UI SDK SegmentedControl for 24h / 7d / 30d time ranges
export function UiRangeSegmented({
  value,
  onChange,
}: {
  value: string;
  onChange: (next: string) => void;
}) {
  return (
    <SegmentedControl
      aria-label="时间范围"
      value={value}
      onValueChange={(val) => {
        if (val) onChange(val);
      }}
      {...stylex.props(ui.segmentedRoot)}
    >
      <SegmentedControlIndicator />
      {[
        ['24h', '最近 24 小时'],
        ['7d', '最近 7 天'],
        ['30d', '最近 30 天'],
      ].map(([k, label]) => (
        <SegmentedControlItem
          key={k}
          value={k}
          className={(state) =>
            stylex.props(ui.segmentedItem, state.checked && ui.segmentedItemChecked).className
          }
        >
          {label}
        </SegmentedControlItem>
      ))}
    </SegmentedControl>
  );
}

// 6. UI SDK Meter for Budget & Quota visualization
export function UiQuotaMeter({
  label,
  spentMicros,
  budgetMicros,
}: {
  label: string;
  spentMicros: number;
  budgetMicros: number;
}) {
  const pct = budgetMicros > 0 ? Math.min(100, Math.max(0, (spentMicros / budgetMicros) * 100)) : 0;
  return (
    <Meter value={Number(pct.toFixed(1))} min={0} max={100} {...stylex.props(ui.meterRoot)}>
      <div {...stylex.props(ui.meterHeader)}>
        <MeterLabel style={{ fontSize: '13px', fontWeight: 600 }}>
          {label}：{fmtMoney(spentMicros)} / {fmtMoney(budgetMicros)}
        </MeterLabel>
        <MeterValue style={{ fontSize: '12px', color: 'var(--fg-muted)' }}>
          {pct.toFixed(1)}%
        </MeterValue>
      </div>
      <MeterTrack {...stylex.props(ui.meterTrack)}>
        <MeterIndicator
          style={{
            backgroundColor:
              pct >= 90
                ? 'var(--fg-destructive)'
                : pct >= 70
                  ? 'var(--fg-warning)'
                  : 'var(--bg-primary)',
          }}
        />
      </MeterTrack>
    </Meter>
  );
}

// 7. UI SDK Checkbox & Switch
export function UiCheckboxRow({
  checked,
  disabled,
  onChange,
  title,
  subtitle,
}: {
  checked: boolean;
  disabled?: boolean;
  onChange: (next: boolean) => void;
  title: React.ReactNode;
  subtitle?: React.ReactNode;
}) {
  return (
    <label {...stylex.props(ui.checkboxRow)}>
      <Checkbox
        checked={checked}
        disabled={disabled}
        onCheckedChange={(val) => onChange(val === true)}
        className={(state) =>
          stylex.props(ui.checkboxBox, state.checked && ui.checkboxChecked).className
        }
      >
        <CheckboxIndicator aria-hidden="true">✓</CheckboxIndicator>
      </Checkbox>
      <div>
        <div style={{ fontWeight: 500, fontSize: '13px' }}>{title}</div>
        {subtitle && <div {...stylex.props(ui.cellSub)}>{subtitle}</div>}
      </div>
    </label>
  );
}

export function UiSwitch({
  checked,
  onChange,
  label,
}: {
  checked: boolean;
  onChange: (next: boolean) => void;
  label?: string;
}) {
  return (
    <label {...stylex.props(ui.inlineRow)} style={{ cursor: 'pointer' }}>
      <Switch
        checked={checked}
        onCheckedChange={(val) => onChange(val === true)}
        className={(state) =>
          stylex.props(ui.switchTrack, state.checked && ui.switchTrackChecked).className
        }
      >
        <SwitchThumb
          className={(state) =>
            stylex.props(ui.switchThumb, state.checked && ui.switchThumbChecked).className
          }
        />
      </Switch>
      {label && <span style={{ fontSize: '13px', fontWeight: 500 }}>{label}</span>}
    </label>
  );
}

// 8. UI SDK Timeline for Request Attempt History
export function UiRequestWaterfall({ detail }: { detail: any }) {
  const bars = Array.isArray(detail?.waterfall) ? detail.waterfall : [];
  const totalMs = Math.max(1, Number(detail?.duration_ms ?? 1));
  const span = detail?.otel_span;

  if (!bars.length) return null;

  const colorForCategory = (cat: string, status: string) => {
    if (['failed', 'error', 'rejected', 'timeout'].includes(String(status || '').toLowerCase())) {
      return 'var(--fg-destructive)';
    }
    if (cat === 'hedge') return '#059669';
    if (cat === 'upstream') return '#2563eb';
    if (cat === 'plugin') return '#7c3aed';
    if (cat === 'settle') return '#0d9488';
    return 'var(--bg-primary)';
  };

  return (
    <div {...stylex.props(ui.detailItem, ui.stackSm)} style={{ marginTop: '8px' }}>
      <div {...stylex.props(ui.inlineRow)} style={{ justifyContent: 'space-between' }}>
        <div {...stylex.props(ui.inlineRow)}>
          <strong style={{ fontSize: '13px' }}>
            全链路甘特瀑布图（8 阶段微内核 + 主备对冲竞速 + OTel Span）
          </strong>
          {detail?.trace_id && (
            <UiBadge tone="info" label={`Trace: ${String(detail.trace_id).slice(0, 16)}…`} />
          )}
        </div>
        <span {...stylex.props(ui.tdMono)}>
          总耗时: {fmtDuration(totalMs)}
          {detail?.ttft_ms != null ? ` · 首包 TTFT: ${fmtDuration(detail.ttft_ms)}` : ''}
        </span>
      </div>

      <div {...stylex.props(ui.stackSm)} style={{ marginTop: '4px' }}>
        {bars.map((b: any, idx: number) => {
          const offset = Math.max(0, Number(b.start_offset_ms ?? 0));
          const dur = Math.max(1, Number(b.duration_ms ?? 1));
          const leftPct = Math.min(92, Math.max(0, (offset / totalMs) * 100));
          const widthPct = Math.min(100 - leftPct, Math.max(4, (dur / totalMs) * 100));
          const barColor = colorForCategory(b.category, b.status);
          return (
            <div key={`${b.stage || idx}-${idx}`} style={{ fontSize: '12px' }}>
              <div
                {...stylex.props(ui.inlineRow)}
                style={{ justifyContent: 'space-between', marginBottom: '3px' }}
              >
                <div {...stylex.props(ui.inlineRow)}>
                  <span style={{ fontWeight: 500 }}>{b.label}</span>
                  {b.category === 'hedge' && (
                    <UiBadge tone="success" label="Peak-EWMA 对冲/备路并发" />
                  )}
                  {b.ttft_ms != null && (
                    <UiBadge tone="info" label={`TTFT ${fmtDuration(b.ttft_ms)}`} />
                  )}
                </div>
                <span {...stylex.props(ui.tdMono)} style={{ color: 'var(--fg-muted)' }}>
                  +{offset} ms · 耗时 {fmtDuration(dur)}
                </span>
              </div>
              <div
                style={{
                  position: 'relative',
                  height: '10px',
                  width: '100%',
                  backgroundColor: 'var(--bg-muted)',
                  borderRadius: '999px',
                  overflow: 'hidden',
                }}
              >
                <div
                  style={{
                    position: 'absolute',
                    left: `${leftPct}%`,
                    width: `${widthPct}%`,
                    top: 0,
                    bottom: 0,
                    backgroundColor: barColor,
                    borderRadius: '999px',
                  }}
                />
              </div>
            </div>
          );
        })}
      </div>

      {span?.attributes && (
        <div style={{ marginTop: '6px' }}>
          <div {...stylex.props(ui.mutedText)} style={{ marginBottom: '4px' }}>
            关联 OpenTelemetry GenAI Span 属性（spanId: {span.spanId}）：
          </div>
          <pre
            {...stylex.props(ui.codeBlock)}
            style={{ fontSize: '11px', padding: '8px 10px', maxHeight: '130px' }}
          >
            <code>{JSON.stringify(span.attributes, null, 2)}</code>
          </pre>
        </div>
      )}
    </div>
  );
}

export function UiAttemptTimeline({
  attempts,
  targets,
}: {
  attempts: any[];
  targets: any[];
}) {
  const targetName = (id: string) =>
    targets.find((x) => x.id === id)?.upstream_model || id || '—';

  if (!attempts.length) {
    return (
      <div {...stylex.props(ui.callout)}>
        尚无上游尝试记录。该请求可能在准入或鉴权阶段结束，未发送到上游。
      </div>
    );
  }

  return (
    <Timeline aria-label="请求上游尝试时间线" {...stylex.props(ui.timeline)}>
      {attempts.map((a, idx) => {
        const dur =
          a.finished_at && a.started_at
            ? new Date(a.finished_at).getTime() - new Date(a.started_at).getTime()
            : null;
        return (
          <TimelineItem key={a.id || idx} {...stylex.props(ui.timelineItem)}>
            <TimelineMarker />
            <TimelineConnector />
            <TimelineTitle {...stylex.props(ui.timelineHeader)}>
              <strong>尝试 {fmtInt(a.attempt_no ?? idx + 1)}</strong>
              <UiBadge status={a.status} />
              {a.error_code && <UiBadge tone="danger" label={String(a.error_code)} />}
            </TimelineTitle>
            <TimelineTime
              dateTime={a.started_at || undefined}
              style={{ fontSize: '12px', color: 'var(--fg-muted)' }}
            >
              开始于 {fmtDate(a.started_at)}
            </TimelineTime>
            <TimelineDescription>
              <div {...stylex.props(ui.timelineMeta)}>
                <span>目标：{targetName(a.target_id)}</span>
                <span>耗时：{fmtDuration(dur)}</span>
                <span>
                  Token：{fmtInt(a.input_tokens)} / {fmtInt(a.output_tokens)}
                </span>
                <span>费用：{fmtMoney(a.cost_micros)}</span>
              </div>
              {a.error_code && (
                <p style={{ margin: '6px 0 0 0', fontSize: '12px', color: 'var(--fg-destructive)' }}>
                  错误码：{String(a.error_code)}。请结合请求 ID 与服务端日志排查，控制台不回显上游原始敏感响应。
                </p>
              )}
            </TimelineDescription>
          </TimelineItem>
        );
      })}
    </Timeline>
  );
}

// 9. UI SDK Controlled Dialog & AlertDialog
export function UiDialog({
  open,
  onOpenChange,
  title,
  description,
  wide = false,
  children,
  footer,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description?: string;
  wide?: boolean;
  children: React.ReactNode;
  footer?: React.ReactNode;
}) {
  return (
    <Dialog open={open} onOpenChange={(next) => onOpenChange(next)}>
      <DialogBackdrop />
      <DialogContent {...stylex.props(ui.dialogBox, wide && ui.dialogBoxWide)}>
        <div {...stylex.props(ui.dialogHeader)}>
          <div>
            <DialogTitle {...stylex.props(ui.dialogTitle)}>{title}</DialogTitle>
            {description && (
              <DialogDescription {...stylex.props(ui.dialogDesc)}>
                {description}
              </DialogDescription>
            )}
          </div>
          <DialogClose
            {...stylex.props(ui.btn, ui.btnOutline, ui.btnSmall)}
            aria-label="关闭对话框"
          >
            ✕
          </DialogClose>
        </div>
        <div>{children}</div>
        {footer && <div {...stylex.props(ui.dialogFooter)}>{footer}</div>}
      </DialogContent>
    </Dialog>
  );
}

export function UiConfirmDialog({
  open,
  onOpenChange,
  title,
  description,
  confirmLabel = '确认',
  danger = false,
  onConfirm,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description: string;
  confirmLabel?: string;
  danger?: boolean;
  onConfirm: () => Promise<void> | void;
}) {
  return (
    <AlertDialog open={open} onOpenChange={(next) => onOpenChange(next)}>
      <AlertDialogBackdrop />
      <AlertDialogContent {...stylex.props(ui.dialogBox)}>
        <div {...stylex.props(ui.dialogHeader)}>
          <div>
            <AlertDialogTitle {...stylex.props(ui.dialogTitle)}>{title}</AlertDialogTitle>
            <AlertDialogDescription {...stylex.props(ui.dialogDesc)}>
              {description}
            </AlertDialogDescription>
          </div>
        </div>
        <div {...stylex.props(ui.dialogFooter)}>
          <AlertDialogClose {...stylex.props(ui.btn, ui.btnOutline)}>取消</AlertDialogClose>
          <UiButton
            variant={danger ? 'destructive' : 'primary'}
            onClick={async () => {
              await onConfirm();
              onOpenChange(false);
            }}
          >
            {confirmLabel}
          </UiButton>
        </div>
      </AlertDialogContent>
    </AlertDialog>
  );
}

// 10. Page Header, Metric Card & Empty State helpers
export function PageHeader({
  title,
  description,
  actions,
}: {
  title: string;
  description?: string;
  actions?: React.ReactNode;
}) {
  return (
    <div {...stylex.props(ui.pageHeader)}>
      <div>
        <h2 {...stylex.props(ui.pageTitle)}>{title}</h2>
        {description && <p {...stylex.props(ui.pageDesc)}>{description}</p>}
      </div>
      {actions && <div {...stylex.props(ui.pageActions)}>{actions}</div>}
    </div>
  );
}

export function MetricCard({
  label,
  value,
  unit,
  note,
}: {
  label: string;
  value: React.ReactNode;
  unit?: string;
  note?: string;
}) {
  return (
    <div {...stylex.props(ui.metricCard)}>
      <div {...stylex.props(ui.metricLabel)}>{label}</div>
      <div {...stylex.props(ui.metricValue)}>
        {value}
        {unit && <span {...stylex.props(ui.metricUnit)}>{unit}</span>}
      </div>
      {note && <div {...stylex.props(ui.metricNote)}>{note}</div>}
    </div>
  );
}

export function EmptyBox({
  title,
  description,
  action,
}: {
  title: string;
  description: string;
  action?: React.ReactNode;
}) {
  return (
    <div {...stylex.props(ui.emptyState)}>
      <h3 {...stylex.props(ui.emptyTitle)}>{title}</h3>
      <p {...stylex.props(ui.emptyDesc)}>{description}</p>
      {action && <div style={{ marginTop: '6px' }}>{action}</div>}
    </div>
  );
}

export async function copyToClipboard(text: string) {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
    } else {
      const ta = document.createElement('textarea');
      ta.value = text;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      ta.remove();
    }
    notify('已复制到剪贴板', 'success');
  } catch {
    notify('复制失败，请手动选择文本复制', 'error');
  }
}
