import React, { useEffect, useState } from 'react';
import * as stylex from '@stylexjs/stylex';
import { Tabs, TabsList, TabsPanel, TabsTab } from '@ui-sdk/react/tabs';
import { Field, FieldDescription, FieldLabel } from '@ui-sdk/react/field';
import { Input } from '@ui-sdk/react/input';
import { ui } from '../styles/ui';
import { api } from '../api/client';
import { UiButton, notify } from '../components/Primitives';

export function AuthView({
  initialMode = 'bootstrap',
  initialInviteToken = '',
  externalError = '',
  onSuccess,
}: {
  initialMode?: 'bootstrap' | 'login' | 'accept';
  initialInviteToken?: string;
  externalError?: string;
  onSuccess: (payload: any, msg: string) => void;
}) {
  const [tab, setTab] = useState<'bootstrap' | 'login' | 'accept'>(initialMode);
  const [error, setError] = useState(externalError);

  // Bootstrap form state
  const [bootEmail, setBootEmail] = useState('');
  const [bootPassword, setBootPassword] = useState('');
  const [bootProject, setBootProject] = useState('');

  // Login form state
  const [loginEmail, setLoginEmail] = useState('');
  const [loginPassword, setLoginPassword] = useState('');

  // Accept invitation state
  const [inviteToken, setInviteToken] = useState(initialInviteToken);
  const [invitePassword, setInvitePassword] = useState('');
  const [inviteConfirm, setInviteConfirm] = useState('');

  useEffect(() => {
    setTab(initialMode);
  }, [initialMode]);

  useEffect(() => {
    if (initialInviteToken) setInviteToken(initialInviteToken);
  }, [initialInviteToken]);

  useEffect(() => {
    if (externalError) setError(externalError);
  }, [externalError]);

  async function handleBootstrap() {
    setError('');
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(bootEmail.trim())) {
      setError('请输入有效邮箱地址。');
      return;
    }
    if (bootPassword.length < 10) {
      setError('密码至少 10 个字符。');
      return;
    }
    if (!bootProject.trim()) {
      setError('请输入首个项目名称。');
      return;
    }
    try {
      const res = await api('/api/bootstrap', {
        method: 'POST',
        body: {
          email: bootEmail.trim(),
          password: bootPassword,
          projectName: bootProject.trim(),
        },
        silentAuth: true,
      });
      onSuccess(res, '管理员与首个项目已创建。');
    } catch (e: any) {
      setError(e?.message || '初始化失败，请稍后重试。');
      if (e?.status === 409) {
        setTab('login');
      }
    }
  }

  async function handleLogin() {
    setError('');
    if (!loginEmail.trim() || !loginPassword) {
      setError('请输入邮箱与密码。');
      return;
    }
    try {
      const res = await api('/api/login', {
        method: 'POST',
        body: {
          email: loginEmail.trim(),
          password: loginPassword,
        },
        silentAuth: true,
      });
      onSuccess(res, '登录成功。');
    } catch (e: any) {
      setError(e?.message || '登录失败，请检查账号或密码。');
    }
  }

  async function handleAccept() {
    setError('');
    if (!inviteToken.trim()) {
      setError('请粘贴邀请令牌。');
      return;
    }
    if (invitePassword.length < 10) {
      setError('密码至少 10 个字符。');
      return;
    }
    if (invitePassword !== inviteConfirm) {
      setError('两次输入的密码不一致。');
      return;
    }
    try {
      const res = await api('/api/invitations/accept', {
        method: 'POST',
        body: {
          token: inviteToken.trim(),
          password: invitePassword,
        },
        silentAuth: true,
      });
      notify('邀请已接受，已登录工作空间', 'success');
      onSuccess(res, '邀请已接受，已登录工作空间。');
    } catch (e: any) {
      setError(e?.message || '接受邀请失败。');
    }
  }

  return (
    <section {...stylex.props(ui.authView)} aria-label="登录或初始化 AI Gateway">
      <div {...stylex.props(ui.authIntro)}>
        <div>
          <div {...stylex.props(ui.inlineRow)}>
            <span {...stylex.props(ui.brandMark, ui.brandMarkLight)} aria-hidden="true">
              AG
            </span>
            <span style={{ fontSize: '16px', fontWeight: 600 }}>AI Gateway</span>
          </div>
          <h1 {...stylex.props(ui.authIntroTitle)}>
            统一模型路由、整数微美元账本与多协议受控接入。
          </h1>
          <p {...stylex.props(ui.authIntroLead)}>
            连接兼容 OpenAI Chat Completions、Anthropic Messages 与 Embeddings
            的上游服务，为每个项目发布不可变路由版本与模型别名。控制台不落盘提示词或模型回答正文。
          </p>
          <ul {...stylex.props(ui.authList)}>
            <li>供应商凭据服务端 AES-GCM 加密只写保存，不在浏览器中回显</li>
            <li>项目密钥仅单次创建时展示明文，支持按模型范围与单 Key RPM 限流</li>
            <li>Redis Lua 原子三阶段微美元账本，已结算与未知用量严格分离</li>
          </ul>
        </div>
        <div style={{ fontSize: '12px', color: '#71717a' }}>
          React 19 · @ui-sdk/react · StyleX 静态控制台
        </div>
      </div>

      <div {...stylex.props(ui.authPanel)}>
        <div {...stylex.props(ui.authCard)}>
          <Tabs
            value={tab}
            onValueChange={(val) => {
              if (val === 'bootstrap' || val === 'login' || val === 'accept') {
                setError('');
                setTab(val);
              }
            }}
          >
            <TabsList aria-label="登录或初始化" {...stylex.props(ui.tabsList, ui.tabsListFull)}>
              <TabsTab
                value="bootstrap"
                className={(state) =>
                  stylex.props(ui.tabsTab, state.selected && ui.tabsTabSelected).className
                }
              >
                首次部署
              </TabsTab>
              <TabsTab
                value="login"
                className={(state) =>
                  stylex.props(ui.tabsTab, state.selected && ui.tabsTabSelected).className
                }
              >
                账号登录
              </TabsTab>
              <TabsTab
                value="accept"
                className={(state) =>
                  stylex.props(ui.tabsTab, state.selected && ui.tabsTabSelected).className
                }
              >
                接受邀请
              </TabsTab>
            </TabsList>

            <TabsPanel value="bootstrap">
              <div {...stylex.props(ui.formGrid)}>
                <div>
                  <h2 style={{ margin: 0, fontSize: '18px', fontWeight: 600 }}>创建首个管理员</h2>
                  <p {...stylex.props(ui.fieldHint)}>
                    仅在工作空间尚无用户时可用，创建后将同时初始化第一个项目。
                  </p>
                </div>
                <Field name="email" {...stylex.props(ui.field)}>
                  <FieldLabel {...stylex.props(ui.fieldLabel)}>管理员邮箱</FieldLabel>
                  <Input
                    type="email"
                    autoComplete="username"
                    placeholder="admin@example.com"
                    value={bootEmail}
                    onValueChange={(v) => setBootEmail(v)}
                    {...stylex.props(ui.input)}
                  />
                </Field>
                <Field name="password" {...stylex.props(ui.field)}>
                  <FieldLabel {...stylex.props(ui.fieldLabel)}>管理员密码</FieldLabel>
                  <Input
                    type="password"
                    autoComplete="new-password"
                    placeholder="至少 10 个字符"
                    value={bootPassword}
                    onValueChange={(v) => setBootPassword(v)}
                    {...stylex.props(ui.input)}
                  />
                  <FieldDescription {...stylex.props(ui.fieldHint)}>
                    至少 10 个字符；控制台不会将密码写入浏览器存储。
                  </FieldDescription>
                </Field>
                <Field name="projectName" {...stylex.props(ui.field)}>
                  <FieldLabel {...stylex.props(ui.fieldLabel)}>首个项目名称</FieldLabel>
                  <Input
                    type="text"
                    placeholder="例如：核心 AI 助手"
                    value={bootProject}
                    onValueChange={(v) => setBootProject(v)}
                    {...stylex.props(ui.input)}
                  />
                </Field>
                {error && <p {...stylex.props(ui.fieldError)}>{error}</p>}
                <UiButton variant="primary" block onClick={handleBootstrap}>
                  创建管理员并进入
                </UiButton>
              </div>
            </TabsPanel>

            <TabsPanel value="login">
              <div {...stylex.props(ui.formGrid)}>
                <div>
                  <h2 style={{ margin: 0, fontSize: '18px', fontWeight: 600 }}>登录控制台</h2>
                  <p {...stylex.props(ui.fieldHint)}>
                    会话使用 HttpOnly 同源安全 Cookie，所有写操作校验 X-CSRF-Token。
                  </p>
                </div>
                <Field name="loginEmail" {...stylex.props(ui.field)}>
                  <FieldLabel {...stylex.props(ui.fieldLabel)}>邮箱</FieldLabel>
                  <Input
                    type="email"
                    autoComplete="username"
                    placeholder="admin@example.com"
                    value={loginEmail}
                    onValueChange={(v) => setLoginEmail(v)}
                    {...stylex.props(ui.input)}
                  />
                </Field>
                <Field name="loginPassword" {...stylex.props(ui.field)}>
                  <FieldLabel {...stylex.props(ui.fieldLabel)}>密码</FieldLabel>
                  <Input
                    type="password"
                    autoComplete="current-password"
                    value={loginPassword}
                    onValueChange={(v) => setLoginPassword(v)}
                    {...stylex.props(ui.input)}
                  />
                </Field>
                {error && <p {...stylex.props(ui.fieldError)}>{error}</p>}
                <UiButton variant="primary" block onClick={handleLogin}>
                  登录
                </UiButton>
              </div>
            </TabsPanel>

            <TabsPanel value="accept">
              <div {...stylex.props(ui.formGrid)}>
                <div>
                  <h2 style={{ margin: 0, fontSize: '18px', fontWeight: 600 }}>接受成员邀请</h2>
                  <p {...stylex.props(ui.fieldHint)}>
                    使用管理员发送的一次性令牌设置密码（有效期 7 天）。
                  </p>
                </div>
                <Field name="token" {...stylex.props(ui.field)}>
                  <FieldLabel {...stylex.props(ui.fieldLabel)}>邀请令牌</FieldLabel>
                  <Input
                    type="text"
                    placeholder="粘贴一次性邀请令牌"
                    value={inviteToken}
                    onValueChange={(v) => setInviteToken(v)}
                    {...stylex.props(ui.input)}
                  />
                </Field>
                <Field name="newPassword" {...stylex.props(ui.field)}>
                  <FieldLabel {...stylex.props(ui.fieldLabel)}>设置密码</FieldLabel>
                  <Input
                    type="password"
                    autoComplete="new-password"
                    placeholder="至少 10 个字符"
                    value={invitePassword}
                    onValueChange={(v) => setInvitePassword(v)}
                    {...stylex.props(ui.input)}
                  />
                </Field>
                <Field name="confirmPassword" {...stylex.props(ui.field)}>
                  <FieldLabel {...stylex.props(ui.fieldLabel)}>确认密码</FieldLabel>
                  <Input
                    type="password"
                    autoComplete="new-password"
                    value={inviteConfirm}
                    onValueChange={(v) => setInviteConfirm(v)}
                    {...stylex.props(ui.input)}
                  />
                </Field>
                {error && <p {...stylex.props(ui.fieldError)}>{error}</p>}
                <UiButton variant="primary" block onClick={handleAccept}>
                  接受邀请并登录
                </UiButton>
              </div>
            </TabsPanel>
          </Tabs>
        </div>
      </div>
    </section>
  );
}
