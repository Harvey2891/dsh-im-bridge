// dsh-im-dingtalk 渠道适配器（PRD FR-1.2/1.3/1.4/1.5）
//
// 只做「钉钉 Stream Mode ⇄ 统一模型」转换，不含业务逻辑：
//   - Stream 模式长连接（WebSocket，官方 SDK dingtalk-stream）——免公网，无需回调 URL（FR-1.3）
//   - 机器人消息 → ImMessage；卡片回调 → handleCallback
//   - 出站：优先 sessionWebhook（机器人回复），降级为企业机器人主动推送 API
//   - 断线由官方 SDK 自动重连；本适配器维护状态 + 最近事件心跳（可观测三件套）
//
// 密钥：clientId/clientSecret 支持 "env:DINGTALK_CLIENT_ID" 引用（规则 #6：密钥不落明文）。
// SDK 可注入（internals.sdk），测试用 stub，CI 不碰真实凭据（规则 #3）。

import z from '@deepseek-ai/schemastery';

const name = 'im-dingtalk';
const inject = ['im'];

/** 钉钉 Stream 消息 topic（官方常量，见 dingtalk-stream/dist/constants）。 */
export const TOPIC_ROBOT = '/v1.0/im/bot/messages/get';
export const TOPIC_CARD = '/v1.0/card/instances/callback';

const Config = z.object({
  clientId: z.string().default('env:DINGTALK_CLIENT_ID'),
  clientSecret: z.string().default('env:DINGTALK_CLIENT_SECRET'),
  // 企业机器人主动推送（outbound 回退路径）；未配置时仅能通过 sessionWebhook 回复。
  // 实测：Stream 模式机器人的 robotCode 就是 Client ID（传 AgentId 会报 robotCode.notExsit）。
  robotCode: z.string().default('env:DINGTALK_ROBOT_CODE'),
  apiBase: z.string().default('https://api.dingtalk.com'), // 测试/代理用
  debug: z.boolean().default(false),
});

/** 解析密钥引用：'env:NAME' → process.env.NAME；否则原样返回。 */
export function resolveSecret(value) {
  if (typeof value === 'string' && value.startsWith('env:')) {
    return process.env[value.slice(4)] ?? '';
  }
  return value;
}

export function apply(ctx, config = {}, internals = {}) {
  const clientId = resolveSecret(config.clientId);
  const clientSecret = resolveSecret(config.clientSecret);
  // 主动推送的 robotCode：显式配置优先；未配置时回退为 clientId。
  // 实测（2026-10-05，真实企业应用）：Stream 机器人的 robotCode == Client ID；
  // 传 AgentId 会得到 invalidParameter.robotCode.notExsit。
  const robotCode = resolveSecret(config.robotCode) || clientId;
  const logger = ctx.logger?.(name) ?? console;
  const apiBase = config.apiBase ?? 'https://api.dingtalk.com';
  const fetchImpl = internals.fetchImpl ?? globalThis.fetch;
  let disposed = false;
  let client = null;
  // access_token 缓存（必须在 send() 首次可达前初始化，否则触发 TDZ）
  let cachedToken = { value: '', expiresAt: 0 };
  // chatId → sessionWebhook（机器人回复用，仅在该会话上下文中有效）
  const sessionWebhooks = new Map();
  // chatId → 钉钉 userId（主动推送时必须用 userId，不能用 conversationId）
  const chatUsers = new Map();

  const channel = {
    platform: 'dingtalk',
    displayName: '钉钉',
    status: {
      connected: false,
      detail: clientId ? 'starting' : 'missing clientId (DINGTALK_CLIENT_ID)',
      lastEventAt: null, // 最近事件心跳（可观测三件套 #2）
    },
    send,
    sendFile,
    dispose: async () => {
      disposed = true;
      try {
        client?.disconnect();
      } catch {
        /* 断开失败不影响卸载 */
      }
    },
  };
  const registration = ctx.get('im').registerChannel(channel);

  if (!clientId || !clientSecret) {
    logger.error(
      'dsh-im-dingtalk: missing credentials — set DINGTALK_CLIENT_ID and DINGTALK_CLIENT_SECRET; channel stays disconnected | 缺少凭据，通道保持断开',
    );
    return () => channel.dispose();
  }

  void start();

  return () => channel.dispose();

  // ── 连接（官方 SDK 薄封装） ───────────────────────────────────────────────

  async function start() {
    try {
      const { DWClient, TOPIC_ROBOT: robotTopic = TOPIC_ROBOT, TOPIC_CARD: cardTopic = TOPIC_CARD } = await loadSdk();
      client = createClient(DWClient, clientId, clientSecret);
      // ⚠️ 关键：机器人消息与卡片回调都是 CALLBACK 类型，不是 EVENT。
      // registerAllEventListener 只订阅 EVENT/*（topic:'*' 不含 CALLBACK），
      // 因此必须用 registerCallbackListener 显式注册 topic——否则连得上但永远收不到消息。
      // 依据：官方 Python SDK 对每个 callback handler 都会 push {type:'CALLBACK', topic}。
      client.registerCallbackListener(robotTopic, handleDownstream);
      client.registerCallbackListener(cardTopic, handleDownstream);
      client.registerAllEventListener(handleDownstream);
      await client.connect();
      channel.status = { ...channel.status, connected: true, detail: `stream:${clientId.slice(0, 8)}…${robotTopic}` };
      logger.info('dsh-im-dingtalk: stream connected | Stream 长连接已建立（已订阅 CALLBACK %s）', robotTopic);
    } catch (err) {
      channel.status = { ...channel.status, connected: false, detail: `connect failed: ${err.message}` };
      logger.error('dsh-im-dingtalk: connect failed | 连接失败: %s', err.message);
    }
  }

  /** 延后加载官方 SDK；internals.sdk 允许测试注入 stub（规则 #3）。 */
  async function loadSdk() {
    if (internals.sdk) return internals.sdk;
    return import('dingtalk-stream');
  }

  function createClient(DWClient, id, secret) {
    const c = new DWClient({
      clientId: id,
      clientSecret: secret,
      debug: config.debug ?? false,
      keepAlive: true,
    });
    // 不手动改 subscriptions：SDK 默认订阅 [type:'EVENT', topic:'*']（subscribe-all），
    // 已在真实环境验证可收到机器人消息与卡片回调。手动注入具体 topic 反而可能漏事件。
    return c;
  }

  // ── 入站 ──────────────────────────────────────────────────────────────────

  /** Stream 下行事件统一入口（SDK 的 registerAllEventListener 回调）。 */
  function handleDownstream(downstream) {
    const topic = downstream?.headers?.topic;
    // 心跳：任何事件都更新（连接活着 ≠ 事件在流）
    channel.status = { ...channel.status, connected: true, lastEventAt: Date.now() };
    try {
      if (topic === TOPIC_CARD) {
        void handleCardCallback(downstream);
      } else {
        void handleRobotMessage(downstream);
      }
    } catch (err) {
      logger.warn('dsh-im-dingtalk: downstream handler failed | 事件处理失败: %s', err.message);
    }
    // 立即 ACK，避免服务端 60s 重推
    return { status: 'SUCCESS' };
  }

  async function handleRobotMessage(downstream) {
    const data = parseJsonSafe(downstream?.data);
    if (!data) return;
    const msg = normalizeRobotMessage(data);
    // 学习 sessionWebhook（供后续主动回复）与真实 userId（供机器人 API 回推）
    if (msg.sessionWebhook) {
      sessionWebhooks.set(msg.chatId, { url: msg.sessionWebhook, expiresAt: msg.sessionWebhookExpiredTime });
    }
    if (msg.userId) chatUsers.set(msg.chatId, msg.userId);
    if (!msg.text && msg.attachments.length === 0) return;
    await ctx.get('im').dispatchInbound({
      platform: 'dingtalk',
      chatId: msg.chatId,
      userId: msg.userId,
      userName: msg.userName,
      text: msg.text,
      msgId: msg.msgId,
      chatType: msg.chatType,
      attachments: msg.attachments,
    });
  }

  async function handleCardCallback(downstream) {
    const data = parseJsonSafe(downstream?.data);
    if (!data) return;
    // 卡片回调载荷：{ userId, outTrackId, ... } 或按钮 value（见 parseCardCallback）
    const cb = parseCardCallback(data);
    if (!cb || !cb.data) return;
    await ctx.get('im').handleCallback({
      platform: 'dingtalk',
      chatId: cb.chatId,
      userId: cb.userId,
      userName: cb.userName,
      data: cb.data,
    });
    // 卡片回调需回执，否则客户端一直转圈
    try {
      const messageId = downstream?.headers?.messageId;
      if (messageId && typeof client?.socketCallBackResponse === 'function') {
        client.socketCallBackResponse(messageId, { status: 'SUCCESS' });
      }
    } catch (err) {
      logger.debug('dsh-im-dingtalk: card ack failed | 卡片回执失败: %s', err.message);
    }
  }

  // ── 出站 ──────────────────────────────────────────────────────────────────

  async function send(out) {
    const chatId = out.chatId;
    if (out.attachments?.length) {
      for (const att of out.attachments) {
        if (att.kind === 'file' && att.text != null) {
          await sendFile(chatId, att.name ?? 'output.txt', att.text);
        }
      }
    }
    if (!out.text) return {};
    const text = withButtonsAsText(out);
    const result = await deliver(chatId, text, out);
    return result ?? {};
  }

  /**
   * 出站投递：优先 sessionWebhook（会话内回复，最简单）；
   * 无 webhook 或已过期时降级为企业机器人主动推送（需 robotCode）。
   */
  async function deliver(chatId, text, out) {
    const hook = sessionWebhooks.get(chatId);
    if (hook && (!hook.expiresAt || hook.expiresAt > Date.now())) {
      try {
        return await postJson(hook.url, buildWebhookBody(text, out));
      } catch (err) {
        logger.warn('dsh-im-dingtalk: sessionWebhook send failed, falling back | sessionWebhook 发送失败，降级: %s', err.message);
      }
    }
    // 最终降级：机器人主动推送。失败必须带平台错误码可见（规则 #5），
    // 且不得向上抛成未处理拒绝——出站失败由核心的记录/告警路径承担。
    try {
      return await sendViaRobotApi(chatId, text, out);
    } catch (err) {
      logger.warn(
        'dsh-im-dingtalk: robot API send failed | 机器人推送失败: %s (code=%s)',
        err.message,
        err.code ?? 'n/a',
      );
      return {};
    }
  }

  /** 钉钉 sessionWebhook 请求体（markdown 以兼容换行与粗体）。 */
  function buildWebhookBody(text, out) {
    const body = {
      msgtype: 'markdown',
      markdown: { title: out.title ?? 'DeepSeek Harness', text },
    };
    if (out.buttons?.length) {
      // Webhook 消息不支持交互卡片；按钮降级为文本指令（钉钉无按钮时与企微/微信一致）
      body.markdown.text += '\n\n' + buttonsAsCommands(out.buttons);
    }
    return body;
  }

  /** 企业机器人主动推送（oToMessages/batchSend）。
   *
   * ⚠️ 关键：`userIds` 必须传**钉钉 userId（senderStaffId）**，不能传 `conversationId`。
   * conversationId 是会话级不透明串，传给机器人 API 会得到 `staffId.notExisted`。
   * 私聊时二者不同，因此入站时记下 chatId → userId 映射，出站时用它回推。
   */
  async function sendViaRobotApi(chatId, text, out) {
    if (!robotCode) {
      logger.warn(
        'dsh-im-dingtalk: no sessionWebhook and no DINGTALK_ROBOT_CODE — cannot send | 无 sessionWebhook 且未配置 robotCode，无法发送',
      );
      return {};
    }
    // 优先用入站学到的真实 userId；退化为 chatId（仅当 chatId 本身就是 userId 时才正确）
    const targetUserId = chatUsers.get(chatId) ?? chatId;
    if (!chatUsers.has(chatId)) {
      logger.warn(
        'dsh-im-dingtalk: no userId learned for chat %s; falling back to chatId as userId (may fail with staffId.notExisted) | 未学到该会话的 userId，回退用 chatId 作为 userId（可能报 staffId.notExisted）',
        chatId,
      );
    }
    const token = await getAccessToken();
    const body = {
      robotCode,
      userIds: [targetUserId],
      msgKey: 'sampleMarkdown',
      msgParam: JSON.stringify({ title: out.title ?? 'DeepSeek Harness', text: withButtonsAsText(out) }),
    };
    const resp = await postJson(`${apiBase}/v1.0/robot/oToMessages/batchSend`, body, token);
    // batchSend 即使 HTTP 200 也可能把目标放进 invalidStaffIdList —— 必须显式检查
    if (resp?.invalidStaffIdList?.length) {
      const err = new Error(
        `DingTalk rejected target userId "${targetUserId}" | 钉钉拒绝该 userId（invalidStaffIdList）`,
      );
      err.code = 'staffId.notExisted';
      throw err;
    }
    return { messageId: resp?.processQueryKey };
  }

  /** 文件投递（/log 全量交付，FR-3.4）：钉钉需先上传媒体再发 file 消息。 */
  async function sendFile(chatId, fileName, text, mime = 'text/plain') {
    const hook = sessionWebhooks.get(chatId);
    try {
      // 简化路径：以 markdown 代码块形式交付；钉钉 webhook 不支持直接 file 上传
      if (hook) {
        await postJson(hook.url, {
          msgtype: 'markdown',
          markdown: {
            title: fileName,
            text: `**📎 ${fileName}**\n\n\`\`\`\n${truncate(text, 4000)}\n\`\`\``,
          },
        });
      }
      return {};
    } catch (err) {
      logger.warn('dsh-im-dingtalk: sendFile failed | 文件发送失败: %s', err.message);
      return {};
    }
  }

  // ── 凭据 ──────────────────────────────────────────────────────────────────

  async function getAccessToken() {
    if (cachedToken.value && cachedToken.expiresAt > Date.now()) return cachedToken.value;
    // 官方 token 端点用 appkey/appsecret（非 Bearer）
    const qs = new URLSearchParams({ appkey: clientId, appsecret: clientSecret });
    const resp = await fetchImpl(`https://oapi.dingtalk.com/gettoken?${qs}`);
    const data = await resp.json().catch(() => null);
    if (!data || data.errcode !== 0) {
      const err = new Error(`DingTalk gettoken failed: ${data?.errmsg ?? `HTTP ${resp.status}`} (${data?.errcode ?? resp.status})`);
      err.code = data?.errcode ?? resp.status;
      throw err;
    }
    cachedToken = { value: data.access_token, expiresAt: Date.now() + (data.expires_in ?? 7200) * 1000 - 60_000 };
    return cachedToken.value;
  }

  async function postJson(url, body, token) {
    const resp = await fetchImpl(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { 'x-acs-dingtalk-access-token': token } : {}),
      },
      body: JSON.stringify(body),
    });
    const data = await resp.json().catch(() => null);
    // 钉钉 webhook 与 OpenAPI 的错误码字段不同，统一带码抛出（规则 #5 边界日志带平台错误码）
    const code = data?.errcode ?? data?.code;
    if (!resp.ok || (code !== undefined && code !== 0)) {
      const err = new Error(`DingTalk send failed: ${data?.errmsg ?? data?.message ?? `HTTP ${resp.status}`} (${code ?? resp.status})`);
      err.code = code ?? resp.status;
      throw err;
    }
    return data;
  }
}

// ── 纯函数（平台形状解析，无网络单测） ──────────────────────────────────────

/**
 * 规范化机器人消息 → 统一模型。
 * 钉钉字段：conversationId/conversationType/senderStaffId/senderNick/msgId/text.content/sessionWebhook。
 * @param {object} data Stream 下行 data（已 JSON.parse）
 */
export function normalizeRobotMessage(data) {
  const conversationType = data.conversationType === '2' ? 'group' : 'private';
  const chatId = data.conversationId ?? data.senderStaffId ?? data.senderId ?? '';
  const attachments = [];
  if (data.msgtype === 'picture' && data.content?.downloadCode) {
    attachments.push({ kind: 'image', name: 'image', path: '' });
  } else if (data.msgtype === 'file' && data.content?.downloadCode) {
    attachments.push({ kind: 'file', name: data.content.fileName ?? 'file', path: '' });
  }
  return {
    platform: 'dingtalk',
    chatId: String(chatId),
    userId: String(data.senderStaffId ?? data.senderId ?? chatId),
    userName: data.senderNick ?? String(data.senderStaffId ?? ''),
    text: parseContent(data.msgtype, data),
    msgId: data.msgId ? String(data.msgId) : undefined,
    chatType: conversationType,
    attachments,
    sessionWebhook: data.sessionWebhook ?? '',
    sessionWebhookExpiredTime: data.sessionWebhookExpiredTime ?? 0,
  };
}

/**
 * 消息内容解析（纯函数，规则 #8）。
 * 支持 text / markdown / richText / picture / file；未知类型降级为空串。
 * @param {string} msgType
 * @param {object} msg 原始消息对象
 */
export function parseContent(msgType, msg) {
  if (!msgType) return '';
  switch (msgType) {
    case 'text':
      return stripAt(String(msg.text?.content ?? ''));
    case 'markdown':
      return stripAt(String(msg.markdown?.text ?? ''));
    case 'richText': {
      // richText 是段落数组，每段含 text/picture 等元素
      const parts = msg.content?.richText ?? [];
      return stripAt(
        parts
          .map((para) =>
            Array.isArray(para)
              ? para.map((el) => (el?.text != null ? String(el.text) : el?.type === 'picture' ? '[图片]' : '')).join('')
              : '',
          )
          .join('\n')
          .trim(),
      );
    }
    case 'picture':
      return '[图片]';
    case 'file':
      return `[文件] ${msg.content?.fileName ?? ''}`.trim();
    default:
      return '';
  }
}

/** 去掉钉钉 @机器人 留下的前导空白（@昵称 会被替换为空）。 */
export function stripAt(text) {
  return String(text ?? '').replace(/^[\s\u2005]+/, '').trim();
}

/**
 * 卡片回调 → 中性载荷。
 * 钉钉卡片回调把按钮 value 放在 data.content（JSON 字符串）里；核心约定 approve:<id>:yes|no。
 * @param {object} data Stream 下行 data
 * @returns {{userId:string,userName:string,chatId:string,data:string}|null}
 */
export function parseCardCallback(data) {
  if (!data) return null;
  let payload = data.content ?? data;
  if (typeof payload === 'string') {
    try {
      payload = JSON.parse(payload);
    } catch {
      // 裸字符串也可能就是 data 载荷本身
      payload = { action: String(data.content) };
    }
  }
  // 按钮 value 形状：{ action: 'approve:xxx:yes' } 或 { data: 'approve:xxx:yes' }
  const raw = payload?.action ?? payload?.data ?? payload?.value ?? '';
  const action = typeof raw === 'string' ? raw : String(raw?.action ?? '');
  if (!action) return null;
  // 身份字段优先取外层 data（钉钉把操作人放在回调外层），再回退到按钮 value 内层。
  const userId = String(data.userId ?? payload?.userId ?? '');
  const userName = String(data.userName ?? data.nick ?? payload?.userName ?? payload?.nick ?? '');
  const chatId = String(data.conversationId ?? payload?.conversationId ?? data.userId ?? payload?.userId ?? '');
  return { userId, userName, chatId, data: action };
}

/** 按钮 → 文本命令（钉钉 webhook 消息无内联按钮，降级为可复制的文本指令）。 */
export function buttonsAsCommands(buttons) {
  return buttons.map((b) => `• ${b.label} → \`${b.id}\``).join('\n');
}

/** 出站文本统一追加按钮文本（无按钮渠道的降级契约）。 */
function withButtonsAsText(out) {
  let text = out.text ?? '';
  if (out.buttons?.length) text += '\n\n' + buttonsAsCommands(out.buttons);
  return text;
}

function parseJsonSafe(value) {
  if (value == null) return null;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(String(value));
  } catch {
    return null;
  }
}

function truncate(text, max) {
  const s = String(text ?? '');
  return s.length <= max ? s : `${s.slice(0, max)}\n…（已截断，用 /log 获取全文）`;
}

export { name, inject, Config };
export default { name, inject, Config, apply };
export const plugin = { name, inject, Config, apply };