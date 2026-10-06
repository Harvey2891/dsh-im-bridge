// dsh-im 核心插件（PRD §8 架构）
//
// 提供 ctx.im 服务：渠道注册、统一消息模型、会话映射、命令解析、
// 渲染、通知总线、审批管理。渠道 adapter 只做协议转换。
//
// 挂载顺序（PRD §12：MockChannel 契约测试先行 → core → adapter）：
//   1. 加载会话映射（mappings.json，重启恢复）
//   2. 挂接审批门（tools/pre-execute）与审批 answerer（approval/request）
//   3. 挂接通知总线（session/event、agent/error）
//   4. 注册命令集
//   5. 等待渠道 adapter 注册（registerChannel）

import { Service } from '@deepseek-ai/cordis';
import z from '@deepseek-ai/schemastery';
import { createUserMessage } from '@deepseek-ai/dsh-llm';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { appendFile, mkdir, copyFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';

import { chatKey, userKey, parseUserKey, sessionIdFor } from './message.js';
import { validateAdapterContract } from './channel.js';
import { SessionMap } from './session-map.js';
import { registerCommand, parseCommand, helpText, commands } from './commands.js';
import { markdownToText, splitLongText, summarizeLongOutput, splitByBytes } from './renderer.js';
import { ApprovalManager } from './approvals.js';
import { NotifyBus } from './notify.js';
import { UserQuestionAnswerer } from './user-questions.js';
import { defaultRiskRules } from './risk.js';

const name = 'im';
const inject = ['agents'];

/** 核心配置（PRD §9；密钥一律 env: 引用，绝不落明文）。 */
const Config = z.object({
  security: z.object({
    allowlist: z.array(z.string()).default([]),       // ["feishu:user_a", "telegram:12345"]
    admins: z.array(z.string()).default([]),          // 可执行敏感命令/审批
    autoCreate: z.boolean().default(false),           // 新聊天是否自动建会话（FR-2.3）
    maxSessions: z.number().default(10),              // FR-2.4
    trustOnFirstContact: z.boolean().default(false),  // FR-9.2 MVP 形态：true=首条消息自动信任（个人自用）；
                                                      // 有 admin 时走「推送管理员确认」路径，无需开此开关
  }),
  approvals: z.object({
    enabled: z.boolean().default(true),
    timeoutSec: z.number().default(300),              // FR-6.4：超时 → pending（可恢复拒绝）
    pendingMaxSec: z.number().default(3600),          // 兜底：仍无人响应 → 失败关闭
    autoApproveRisk: z.union([z.const('none'), z.const('low'), z.const('medium')]).default('none'), // FR-6.6
    riskRules: z.array(z.object({
      tool: z.string(),
      args: z.string(),
      risk: z.union([z.const('low'), z.const('medium'), z.const('high')]),
    })), // 缺省时在 apply 中用 defaultRiskRules()（schemastery 不支持函数默认值）
  }),
  notifications: z.object({
    onTurnEnd: z.boolean().default(true),             // FR-5.3
    onError: z.boolean().default(true),
    includeReasoning: z.boolean().default(false),
    includeCost: z.boolean().default(true),
    pricing: z.object({
      inputPerM: z.number().default(0),
      outputPerM: z.number().default(0),
    }), // 0=仅显示 token，不估算金额
    quietHours: z.array(z.string()).default([]),      // 如 ["22:00-08:00"]（FR-5.4）
    streamWhileOnline: z.boolean().default(true),      // FR-3.3/5.5
    onlineWindowMin: z.number().default(10),           // FR-5.5 在线判定窗口
    flushIntervalMs: z.number().default(400),
  }),
  agent: z.object({
    provider: z.string().default(''),                  // 空 → 用 agentDefaultModel 或内置默认
    model: z.string().default(''),
    workspace: z.string().default(''),                 // 空 → process.cwd()
    // IM 会话使用的 agent preset id。
    // 必须挂载 preset，否则 agent 拿不到文件/shell 工具（read/write/edit/glob/grep/pwsh）——
    // 这些工具声明在 preset 里，不是根级插件。缺它时 IM 会话「读不了文件」。
    preset: z.string().default('standard'),
  }),
  storeDir: z.string().default(''),                    // 空 → $DSH_HOME/dsh-im
  userQuestions: z.object({
    // 把 agent 的 ask_user_question 转成 IM 消息并等待应答。
    // 关闭时该请求交给其他应答者（GUI 等）；IM 场景下关闭 = agent 提问会失败。
    enabled: z.boolean().default(true),
    // 0 = 无限等待（对齐 ask_user_question 默认「等待用户」语义）
    timeoutSec: z.number().default(0),
  }),
});

/** 默认模型兜底（与官方 base agent-default-model 一致）。 */
const DEFAULT_MODEL = { provider: 'deepseek-official', model: 'deepseek-v4-flash' };

/**
 * 把中性按钮降级为正文命令文本（供**文本型渠道**预算用）。
 *
 * 适配器侧的 `buttonsAsCommands` 会按 `text.includes(command)` 去重，所以核心只要
 * 保证这段文本里**出现 command 原文**，适配器就不会重复追加。
 * 返回空串表示该消息没有可降级的按钮。
 */
function buttonsAsText(buttons) {
  if (!Array.isArray(buttons) || buttons.length === 0) return '';
  return buttons
    .map((b) => `• ${b.label ?? ''} → ${b.command ?? b.id}`)
    .join('\n');
}

export class ImRuntime extends Service {
  static inject = inject;
  static Config = Config;

  constructor(ctx, config = {}) {
    super(ctx, name);
    this.cfg = config;
    this.channels = new Map();
    this.lastUserTexts = new Map(); // sessionId → 最近一条用户文本（重试用，FR-5.6）
    this._lastFullOutput = new Map(); // sessionId → 最近 turn 的完整输出（/log 用）
    this.agentHandles = new Map(); // sessionId → AgentHandle（释放 agent 的唯一途径）
    this.log = ctx.logger('im');
    this._dispose = [];
    this._ready = this.init();
    // 消费方 await 初始化完成
    this.whenReady = () => this._ready;
    // 随插件卸载自动清理（cordis fiber effect；构造函数内同步注册，避免 await 后注册失效）
    this.ctx.effect(() => () => {
      void this.dispose();
    });
  }

  // ── 初始化 ────────────────────────────────────────────────────────────────

  async init() {
    const cfg = this.cfg;
    const home = process.env.DSH_HOME ?? join(homedir(), '.dsh');
    this.storeDir = cfg.storeDir || join(home, 'dsh-im');

    this.map = new SessionMap(this.storeDir);
    await this.map.load();
    // 配置中的 allowlist/admins 为权威来源，与运行期追加合并
    for (const u of cfg.security.allowlist ?? []) this.map.allowlist.add(u);
    for (const u of cfg.security.admins ?? []) this.map.admins.add(u);

    // 安全默认引导：allowlist 与 admins 全空 = deny-all，管理员需一次性配置自己的键
    if ((cfg.security.allowlist?.length ?? 0) === 0 && (cfg.security.admins?.length ?? 0) === 0) {
      this.log.warn(
        'security: deny-all — no allowlist/admins configured yet. Add your own key once, e.g. im.security.admins: ["feishu:ou_xxx"], then every new user is approved by you with one tap. | '
        + '安全：当前拒绝所有用户。请一次性在配置 im.security.admins 填入你自己的用户键（如 ["feishu:ou_xxx"]），之后新用户首条消息会触发你的一键信任确认。'
      );
    }

    // 审批
    this.approvals = new ApprovalManager({
      ctx: this.ctx,
      map: this.map,
      send: (chat, out) => this.send(chat, out),
      logLine: (line) => this.appendLog('approvals', line),
    });
    this.approvals.configure({
      enabled: cfg.approvals.enabled,
      timeoutSec: cfg.approvals.timeoutSec,
      pendingMaxSec: cfg.approvals.pendingMaxSec,
      autoApproveRisk: cfg.approvals.autoApproveRisk,
      riskRules: cfg.approvals.riskRules ?? defaultRiskRules(),
    });
    this.approvals.mount();

    // 用户提问应答者：把 ask_user_question 转成 IM 消息并等待回答。
    // 没有它，IM 会话里 agent 提问会落到 noAnswerer 并抛 NO_PROVIDER。
    this.userQuestions = new UserQuestionAnswerer({
      ctx: this.ctx,
      map: this.map,
      send: (chat, out) => this.send(chat, out),
      logLine: (line) => this.appendLog('questions', line),
      cfg: cfg.userQuestions,
    });
    this.userQuestions.mount();

    // 通知
    this.notify = new NotifyBus({
      ctx: this.ctx,
      map: this.map,
      send: (chat, out) => this.send(chat, out),
      log: (line) => this.log.info(line),
      lastUserTextFor: (sessionId) => this.lastUserTexts.get(sessionId) ?? '',
    });
    this.notify.configure({
      onTurnEnd: cfg.notifications.onTurnEnd,
      onError: cfg.notifications.onError,
      includeReasoning: cfg.notifications.includeReasoning,
      includeCost: cfg.notifications.includeCost,
      pricing: cfg.notifications.pricing ?? null,
      quietHours: cfg.notifications.quietHours ?? [],
      streamWhileOnline: cfg.notifications.streamWhileOnline,
      onlineWindowMin: cfg.notifications.onlineWindowMin,
      flushIntervalMs: cfg.notifications.flushIntervalMs,
    });
    this.notify.mount();

    // 命令
    this.registerCommands();

    // 会话事件：捕获完整输出（/log 用）
    this._dispose.push(this.ctx.on('session/event', (session, event) => {
      if (event.type !== 'assistant/message') return;
      if (!this.map.bySessionId(session.id)) return;
      const text = event.data.message?.content
        ?.filter((b) => b.type === 'text')
        .map((b) => b.text ?? '').join('\n');
      if (text) this._lastFullOutput.set(session.id, text);
    }));

    this._dispose.push(() => {
      this.approvals.dispose();
      this.userQuestions?.dispose();
      this.notify.dispose();
      void this.map.dispose();
    });
  }

  // ── 渠道注册（FR-1.1 / §8.3 扩展点） ─────────────────────────────────────

  /** 注册渠道 adapter（契约不完整 → 抛错，FR-9.4）。 */
  registerChannel(channel) {
    const contract = validateAdapterContract(channel);
    for (const w of contract.warnings) this.log.warn(w);
    if (this.channels.has(channel.platform)) {
      throw new Error(`im: channel "${channel.platform}" already registered`);
    }
    if (typeof channel.attach === 'function') channel.attach(this);
    this.channels.set(channel.platform, channel);
    this.log.info(`channel "${channel.platform}" registered | 渠道已注册`);
    return () => {
      this.channels.delete(channel.platform);
      if (typeof channel.dispose === 'function') void channel.dispose();
    };
  }

  getChannel(platform) {
    return this.channels.get(platform) ?? null;
  }

  /**
   * 出站路由：统一模型 → 渠道 send()。
   *
   * 附带把该会话已知的 `userId` 一并下传：有些平台（钉钉）的**主动推送**必须用
   * 平台 userId，而 `chatId` 是会话级不透明串（conversationId），拿它当 userId 会被
   * 平台拒绝（staffId.notExisted）。会话绑定是**持久化**的（SessionMap），所以由核心
   * 在这里补齐，而不是让适配器自己维护易失的内存映射——后者重启即丢，导致出站静默失败。
   */
  async send({ platform, chatId }, out) {
    await this._ready;
    const channel = this.channels.get(platform);
    if (!channel) throw new Error(`im: no channel registered for "${platform}"`);
    // 取该会话最近活跃的用户 id（私聊即对话对象；群聊取最近发言者）。
    // 注意：binding.users 是 Map<userId, {name,lastActiveAt}> —— userId 是**键**，
    // 不在值对象里，所以必须读 keys()，读 values() 会拿到 undefined。
    const binding = this.map.get(platform, chatId);
    // 取该会话**最近活跃**的用户 id。
    // 注意 1：binding.users 是 Map<userId, {name,lastActiveAt}> —— userId 是**键**，
    //         不在值对象里，必须读 keys()（读 values() 会拿到 undefined）。
    // 注意 2：Map 对**已有键**重新 set **不会**改变迭代顺序，所以 `at(-1)` 拿到的是
    //         「最早加入」而非「最近活跃」。群聊 A→B→A 时键序仍是 [A,B]，会回推给 B。
    //         因此按 lastActiveAt 显式取最近者。
    let userId;
    if (binding?.users?.size) {
      let newest;
      let newestAt = -1;
      for (const [id, info] of binding.users) {
        const at = info?.lastActiveAt ?? 0;
        if (at >= newestAt) { newestAt = at; newest = id; }
      }
      userId = newest;
    }
    const body = { ...out, platform, chatId, ...(userId ? { userId } : {}) };

    // 🔴 出站文本必须**只有一处**生成（单一真源），否则预算与实发不符。
    //
    // 文本型渠道（声明 `buttonsAsText`，如钉钉）渲染不了内联按钮，只能把按钮降级成
    // 正文命令。此前核心只在预算里"估算"这段文本、却仍把 buttons 交给适配器去追加：
    // 适配器用**子串**判断是否已存在，命令跨段或作为子串出现时会重复追加或漏加，
    // 最终实发正文超限被平台截断（round-2 发现 1、7）。
    // 现在由核心**直接拼好最终文本并清掉 buttons**，适配器原样发送即可 ——
    // 预算与实发字节数因此逐字节一致，也不再需要任何子串去重。
    //
    // 但命令块必须**不可拆分**：若与正文一起切分，一条命令会被切成两半，用户无法
    // 复制（round-3 发现）。因此正文与命令块**分开切分**，命令块整体放在最后一段；
    // 放不下就单独作为一条消息发出。
    const degradesButtons = channel.buttonsAsText === true;
    const commands = (degradesButtons && body.buttons?.length) ? buttonsAsText(body.buttons) : '';
    if (commands) body.buttons = undefined;   // 已渲染进正文；避免适配器二次追加

    const maxBytes = channel.maxMessageBytes;
    const mainText = body.text ?? '';
    if (!maxBytes) {
      if (commands) body.text = mainText ? `${mainText}\n\n${commands}` : commands;
      return this._dispatch(channel, body);
    }

    const full = commands ? (mainText ? `${mainText}\n\n${commands}` : commands) : mainText;
    if (!full) return this._dispatch(channel, body);
    if (Buffer.byteLength(full, 'utf8') <= maxBytes) {
      if (commands) body.text = full;
      return this._dispatch(channel, body);
    }

    // 前缀 `(i/n) ` 的字节数是 `digits(i)+digits(n)+4`，随段数增长：
    // 固定 12 字节在 5 位段数（>9999 段）时不敷。这里按实际段数迭代求出预留量。
    // 只切分**正文**；命令块独立处理，保证不被切开。
    let reserve = 8;
    let parts = splitByBytes(mainText || full, Math.max(1, maxBytes - reserve));
    for (let attempt = 0; attempt < 3; attempt++) {
      const need = String(parts.length + 1).length * 2 + 4;
      if (need <= reserve) break;
      reserve = need;
      parts = splitByBytes(mainText || full, Math.max(1, maxBytes - reserve));
    }

    // 命令块整体作为**独立末段**：正文末段通常已接近预算上限，把命令并进去
    // 会重新超限（实测 300 上限得到 356 字节），所以只要发生分段就让命令独占一段。
    const cmdOwnSegment = !!commands;
    const total = parts.length + (cmdOwnSegment ? 1 : 0);

    let last;
    // 带按钮的原生渠道（未声明 buttonsAsText）：按钮**只在整体最后一条**消息上。
    // 每段都带会让用户点击前面那些已失效的按钮（返回 not-found），是无效操作。
    const lastBodyIdx = cmdOwnSegment ? -1 : parts.length - 1;
    for (let i = 0; i < parts.length; i++) {
      const isLastBody = i === lastBodyIdx;
      last = await this._dispatch(channel, {
        ...body,
        text: `(${i + 1}/${total}) ${parts[i]}`,
        // 按钮与附件都只在最后一段：附件每段都带会被重复投递
        ...(isLastBody ? {} : { buttons: undefined, attachments: undefined }),
      });
    }
    if (cmdOwnSegment) {
      const cmdParts = splitByBytes(commands, Math.max(1, maxBytes - reserve));
      for (let j = 0; j < cmdParts.length; j++) {
        const idx = parts.length + j + 1;
        const isVeryLast = j === cmdParts.length - 1;
        last = await this._dispatch(channel, {
          ...body,
          text: `(${idx}/${total}) ${cmdParts[j]}`,
          ...(isVeryLast ? { attachments: undefined } : { buttons: undefined, attachments: undefined }),
        });
      }
    }
    return last;
  }

  /**
   * 真正调用渠道 `send()`，并把「发送失败」统一转成异常。
   *
   * 适配器可能在**不抛异常**的情况下失败（如钉钉无 sessionWebhook 且无 userId 时
   * 返回 `{failed:true}`）。若不在此转成异常，调用方（提问/审批应答者）会以为
   * 送达成功，记录一直停在 waiting —— 用户什么也没收到，agent 永远等不到答案。
   */
  async _dispatch(channel, body) {
    const result = await channel.send(body);
    if (result && result.failed) {
      const err = new Error(
        `im: channel "${channel.platform}" failed to deliver: ${result.reason ?? 'unknown'}`,
      );
      err.code = result.reason;
      throw err;
    }
    return result;
  }

  /** 出站路由（显式目标，含按钮/附件），供其他插件复用。 */
  async push(platform, chatId, out) {
    return this.send({ platform, chatId }, out);
  }

  // ── 入站管道（adapter → 核心） ───────────────────────────────────────────

  /**
   * 统一入站入口：adapter 构造 ImMessage 后调用。
   * 流程：去重 → 信任校验（FR-9.2）→ 会话映射（FR-2）→ 命令/派活（FR-3.1）。
   */
  async dispatchInbound(msg) {
    await this._ready;
    if (!msg || typeof msg.platform !== 'string' || typeof msg.chatId !== 'string') {
      this.log.warn('dropped malformed inbound message | 丢弃格式错误的入站消息: %o', msg);
      return;
    }
    const { platform, chatId, userId, text = '' } = msg;
    if (!this.channels.has(platform)) {
      this.log.warn(`inbound from unregistered platform "${platform}" ignored | 忽略来自未注册平台的消息`);
      return;
    }
    if (!this.map.dedupe(platform, msg.msgId)) return; // FR-1.4 幂等去重

    // 先记录发言者：出站要带平台 userId（钉钉主动推送必须用它），而 send() 从
    // 会话映射读取。必须早于任何回复，否则首条消息（如首接触提示）会缺 userId。
    // touch 只在已有绑定时生效；首条消息可能尚未建绑定，故缺失时先建绑定。
    if (!this.map.get(platform, chatId)) {
      this.map.create(platform, chatId, { chatType: msg.chatType ?? 'private' });
    }
    this.map.touch(platform, chatId, userId, msg.userName);

    // FR-8.2：allowlist 之外的用户「可读不可写」→ 派活/命令一律拒绝（admins 隐式放行）
    if (!this.isAllowed(platform, userId)) {
      await this.trustGate(msg);
      return;
    }

    // 有文本 → 命令优先（FR-4.1：未知命令回 /help 摘要，不吞消息）
    const parsed = text ? parseCommand(text) : null;
    if (parsed && commands.has(parsed.name)) {
      await this.handleCommand(msg, parsed);
      return;
    }

    // 待答提问：只消费**无歧义**的作答形式，其余留给用户表达真实意图。
    //
    // 统一作答语法（round-6 重构）：
    //   · 选项题  → 卡片有待选项时，直接回数字；或 `/answer <id> <编号>`
    //   · 文字题  → 仅当**整张卡片都是文字题**时，普通文字自动当答案；
    //               混合题型必须用显式 `/answer <id> text <内容>`
    //   · 跳过    → `skip` / `/answer <id> skip`
    //   · 提交    → `/answer <id> done`
    //
    // 🔴 为什么混合题型的普通文字**不**再自动当答案：此前只要存在未填文字题，任何非命令
    // 文本都会被吃成答案 —— 用户本想派的新任务（如「部署生产环境」）会**丢失**
    // （round-6 P2）。代价是混合题型要显式写 `text`，卡片已写明。
    const pendingQ = this.userQuestions?.pendingFor(platform, chatId);
    if (pendingQ && text) {
      const t = text.trim();
      const isSkip = /^(skip|跳过)$/i.test(t);
      const isChoice = /^\d+(\.\d+)?([,，\s]+\d+(\.\d+)?)*$/.test(t);
      const optionQs = pendingQ.questions
        .map((q, qi) => ((q.options ?? []).length ? qi : -1))
        .filter((qi) => qi >= 0);
      const textQs = pendingQ.questions
        .map((q, qi) => ((q.options ?? []).length ? -1 : qi))
        .filter((qi) => qi >= 0);
      const unfilledText = textQs.some((qi) => !pendingQ.draftCustom.has(qi));

      if (isSkip) {
        await this.commandAnswer(msg, [pendingQ.id, t]);
        return;
      }
      // 回数字：只有当卡片**确实有待选项**时才算选项作答；
      // 纯文字题卡片回数字应作为文字答案（round-6 P2：数字文本此前答不进去）。
      if (isChoice && optionQs.length > 0) {
        await this.commandAnswer(msg, [pendingQ.id, t]);
        return;
      }
      // 纯文字题卡片：普通文字自动当答案（整张卡片就是一份问卷，无歧义）。
      // 注意**不要**排除数字：纯文字题卡片没有可选项，`123` 只能是文字
      // （round-6 P2：此前数字文本答不进去）。
      const pureTextCard = textQs.length > 0 && optionQs.length === 0;
      if (pureTextCard && unfilledText && !parseCommand(t)) {
        const r = this.userQuestions.answerText(String(pendingQ.id), t, { platform, chatId });
        await this.send({ platform, chatId }, {
          text: r.status === 'partial'
            ? `📝 已记录提问 #${pendingQ.id} 第 ${(r.qi ?? 0) + 1} 题的文字；还有 ${r.remainingText} 道文字题未答。`
            : `✅ 已回答提问 #${pendingQ.id}，agent 继续。`,
        });
        return;
      }
    }

    // 普通消息 → 派活
    await this.dispatchTask(msg);
  }

  /**
   * 按钮/回调统一入口（adapter 解析平台回调后调用）。
   * data 载荷约定：approve:<id>:yes|no / trust:<platform>:<userId> / retry:<sessionId>
   */
  async handleCallback({ platform, chatId, userId, userName, data }) {
    await this._ready;
    if (!data || typeof data !== 'string') return;
    const parts = data.split(':');
    const kind = parts[0];
    const reply = (text) => this.send({ platform, chatId }, { text }).catch(() => {});

    switch (kind) {
      case 'approve': {
        // 审批按钮回调身份校验（§10）：点按钮的人必须 ∈ allowlist
        if (!this.isAllowed(platform, userId)) {
          return reply(`⛔ 无权限：审批需要 allowlist 成员身份（当前 ${platform}:${userId} 未授权）。`);
        }
        const [, id, answer] = parts;
        const result = this.approvals.respond(id, answer, { platform, userId, userName });
        const texts = {
          accepted: `✅ 已批准 #${id}，agent 继续执行。`,
          rejected: `❌ 已拒绝 #${id}。`,
          ignored: `ℹ️ 审批 #${id} 已被响应或不存在。`,
          'not-found': `ℹ️ 审批 #${id} 不存在或已结束。`,
          forbidden: `⛔ 无权限。`,
        };
        return reply(texts[result] ?? `ℹ️ ${result}`);
      }
      case 'q': {
        // 用户提问的选项按钮：q:<questionId>:<qIndex>:<oIndex>
        if (!this.isAllowed(platform, userId)) {
          return reply(`⛔ 无权限：回答提问需要 allowlist 成员身份（当前 ${platform}:${userId} 未授权）。`);
        }
        if (!this.userQuestions) return reply('ℹ️ 提问应答未启用。');
        const [, qid, qIdxRaw, oIdxRaw] = parts;
        // 按钮应答**累计**进草稿：答全自动提交；未答全保持等待并回报进度。
        // （此前一次点击即结题，多问题卡片其余题会被当成空答案，余下按钮失效。）
        const acc = this.userQuestions.accumulate(
          qid,
          [{ qIndex: Number(qIdxRaw), oIndex: Number(oIdxRaw) }],
          { platform, chatId },   // 归属校验：只有提问所在会话能作答
        );
        const qTexts = {
          answered: `✅ 已回答提问 #${qid}，agent 继续。`,
          'not-found': `ℹ️ 提问 #${qid} 不存在、已回答或已超时。`,
          invalid: `⚠️ 选项无效，提问仍在等待——请按卡片编号重发。`,
          forbidden: `⛔ 该提问属于其它会话，不能在此代答。`,
        };
        if (acc.status === 'partial') {
          const hint = acc.needsText
            ? `\n该卡片含**自由文本题**，按钮无法作答——请直接回复文字，或发 \`/answer ${qid} done\` 提交当前选择。`
            : `\n进度 ${acc.answered}/${acc.total}——继续点其余题，或发 \`/answer ${qid} done\` 直接提交。`;
          return reply(
            `📝 已记录提问 #${qid} 的第 ${(Number(qIdxRaw) || 0) + 1} 题：${acc.label ?? ''}${hint}`,
          );
        }
        return reply(qTexts[acc.status] ?? `ℹ️ ${acc.status}`);
      }
      case 'trust': {
        const [, targetPlatform, ...rest] = parts;
        const targetUserId = rest.join(':');
        if (!this.isAdmin(platform, userId)) {
          return reply('⛔ 仅管理员可信任用户。');
        }
        if (!targetPlatform || !targetUserId) return reply('用法：/trust <platform:userId>');
        this.map.addToAllowlist(targetPlatform, targetUserId);
        this.log.info(`admin ${platform}:${userId} trusted ${targetPlatform}:${targetUserId}`);
        return reply(`✅ 已信任 ${targetPlatform}:${targetUserId}，对方可开始派活。`);
      }
      case 'retry': {
        const sessionId = parts.slice(1).join(':');
        const text = this.lastUserTexts.get(sessionId);
        if (!text) return reply('ℹ️ 没有可重试的任务。');
        const agent = this.ctx.agents.get(sessionId);
        if (!agent) return reply('ℹ️ 会话不在线（重启后需先发一条消息恢复）。');
        agent.followup(this.userMessage(text));
        return reply('🔁 已重新提交任务。');
      }
      default:
        this.log.warn(`unknown callback data: "${data}"`);
    }
  }

  // ── 信任与授权（FR-9.2 / FR-8.2） ────────────────────────────────────────

  isAllowed(platform, userId) {
    return this.map.isAllowed(platform, userId);
  }

  isAdmin(platform, userId) {
    return this.map.isAdmin(platform, userId);
  }

  /** 首次接触：未知用户 → 信任确认（默认拒绝，但授权从改配置变成点一次确认）。 */
  async trustGate(msg) {
    const { platform, chatId, userId, userName } = msg;
    const cfg = this.cfg.security;

    // MVP 形态：trustOnFirstContact=true（个人自用）→ 自动信任
    if (cfg.trustOnFirstContact) {
      this.map.addToAllowlist(platform, userId);
      this.log.info(`trust-on-first-contact: auto-trusted ${platform}:${userId} | 首次接触：已自动信任 ${platform}:${userId}`);
      await this.send({ platform, chatId }, {
        text: `👋 首次接触，已自动信任 ${userName ?? userId}（security.trustOnFirstContact=true）。\n发送 /new 创建会话开始派活。`,
      });
      return;
    }

    // 有 admin → 推送管理员确认；否则提示配置
    const adminKeys = [...this.map.admins];
    if (adminKeys.length > 0) {
      const pending = `${platform}:${userId}`;
      this.log.info(`trust request from ${pending}, notifying admins`);
      for (const adminKey of adminKeys) {
        const admin = parseUserKey(adminKey);
        if (!admin) continue;
        await this.send({ platform: admin.platform, chatId: admin.userId }, {
          text: `🔐 信任确认：用户 ${userName ?? userId}（${platform}）想与 agent 对话。\n回复 /trust ${pending} 信任，或 /revoke ${pending} 拒绝。`,
          buttons: [{ id: `trust:${platform}:${userId}`, label: '✅ 信任', style: 'primary' }],
        }).catch(() => {});
      }
      await this.send({ platform, chatId }, {
        text: `⏳ 你尚未被授权（${platform}:${userId}）。已向管理员发送信任确认，请稍候。`,
      });
      return;
    }

    // 无 admin 也无自动信任：拒绝并给出配置指引（FR-9.3 /status 缺口提示），
    // 同时控制台提示管理员自己的一次性配置（普通用户无需碰配置）
    const pendingKey = `${platform}:${userId}`;
    this.log.warn(
      `untrusted inbound ${pendingKey}; no admins configured. To approve users by one tap, add yourself once: im.security.admins: ["${pendingKey}"] | `
      + `收到未授权消息 ${pendingKey}，且未配置管理员。想一键审批用户，请一次性在配置 im.security.admins 填入你自己的键：["${pendingKey}"]`
    );
    await this.send({ platform, chatId }, {
      text: `⛔ 当前未授权（${platform}:${userId}）。\n请管理员在配置中授权：\n  im.security.admins: ["${platform}:${userId}"]\n（只需管理员配置一次；之后新用户首条消息会推送一键信任确认，普通用户无需改任何配置。）`,
    });
  }

  // ── 派活（FR-3.1） ───────────────────────────────────────────────────────

  async dispatchTask(msg) {
    const { platform, chatId, userId, userName, text } = msg;
    const cfg = this.cfg.security;
    let binding = this.map.get(platform, chatId);

    // 新聊天默认不自动建 session（FR-2.3）
    if (!binding) {
      if (!cfg.autoCreate) {
        await this.send({ platform, chatId }, {
          text: '📝 当前未创建会话。\n发送 /new 创建会话后即可派活（或设置 security.autoCreate: true 自动创建）。',
        });
        return;
      }
      if (this.map.size >= cfg.maxSessions) {
        await this.send({ platform, chatId }, {
          text: `⛔ 会话数已达上限（${cfg.maxSessions}）。请用 /status 查看，或清理旧会话后重试。`,
        });
        return;
      }
      binding = this.createBinding(platform, chatId, msg.chatType ?? 'private');
    }

    this.map.touch(platform, chatId, userId, userName);

    // 附件：落盘 session 工作区 im-inbox/<chatId>/，文本附路径（FR-7.1 v1）
    let payload = text;
    if (msg.attachments?.length) {
      const inbox = join(this.storeDir, 'im-inbox', String(chatId));
      await mkdir(inbox, { recursive: true });
      const paths = [];
      for (const att of msg.attachments) {
        if (att.path) {
          try {
            const dest = join(inbox, `${Date.now()}-${att.name ?? 'file'}`);
            await copyFile(att.path, dest);
            paths.push(dest);
          } catch (err) {
            this.log.warn(`attachment copy failed | 附件保存失败: ${err.message}`);
          }
        } else if (att.url) {
          // v1 不主动下载远程 URL（SSRF 风险），留 v2 视觉桥
          this.log.warn('remote attachment url ignored (v2 vision bridge) | 远程附件 URL 暂不下载（v2 视觉桥）: %s', att.url);
        }
      }
      if (paths.length) {
        payload = payload ? `${payload}\n\n[附件已保存] ${paths.join('\n')}` : `[附件已保存]\n${paths.join('\n')}`;
      }
    }

    let agent = this.ctx.agents.get(binding.sessionId);
    if (!agent) {
      // DSH 重启后：优先恢复原会话（FR-2.2 / UC6），失败才新建
      agent = await this.tryResume(binding.sessionId);
      if (!agent) {
        agent = await this.createAgent(binding.sessionId);
      }
    }

    this.lastUserTexts.set(binding.sessionId, text);
    agent.followup(this.userMessage(payload));
    this.emit('im/dispatch', { sessionId: binding.sessionId, platform, chatId, text });
  }

  createBinding(platform, chatId, chatType, sessionId) {
    return this.map.create(platform, chatId, { chatType, sessionId });
  }

  /**
   * `/new` 用的新会话 id：在确定性 id 基础上加时间戳后缀。
   * 必须换 id —— 确定性 id 对应的会话日志已持久化在磁盘上，
   * 再用同一 id 去 create 会抛 `session "<id>" already exists`。
   * 映射会记住新 id，所以重启后仍能 resume 到最新会话。
   */
  freshSessionId(platform, chatId) {
    return `${sessionIdFor(platform, chatId)}-${Date.now().toString(36)}`;
  }

  async tryResume(sessionId) {
    try {
      const handle = await this.ctx.agents.resume({
        resumeSessionId: sessionId,
        agentOptions: await this.agentOptions(),
      });
      // 同样要记住 handle：resume 出来的 agent 也只有 handle 能释放（见 disposeAgent）。
      // 漏了这一步，重启后 resume 的会话在 /new 时无法释放。
      this.agentHandles.set(sessionId, handle);
      return handle.agent;
    } catch (err) {
      this.log.warn(`resume ${sessionId} failed (will create fresh) | 恢复会话失败（将新建）: ${err.message}`);
      return null;
    }
  }

  async createAgent(sessionId) {
    const options = await this.agentOptions();
    const presetId = this.cfg.agent.preset || 'standard';
    const handle = await this.ctx.agents.create({
      sessionId,
      meta: { cwd: this.workspace(), agentPreset: presetId },
      agentOptions: options,
      // ⚠️ 必须挂载 agent preset：文件/shell 工具（read/write/edit/glob/grep/pwsh）
      // 都由 preset 声明。不挂载时 agent 只剩根级插件贡献的工具（agent-team/
      // schedule/skill），表现为「IM 会话没有文件读取权限」——实为工具未装载。
      // 官方 webhook 运行时与本仓库 registry 测试用的都是同样的 setup 写法。
      // 用 ctx.get（而非 ctx.agentPresets 属性代理）：本包 inject 未声明该服务，
      // 属性代理对拓扑敏感，严格读取应走全局服务存储（见 DSH packages/AGENTS.md）。
      setup: async (agentCtx) => {
        const presets = this.ctx.get('agentPresets');
        if (!presets) {
          this.log.error(
            `agentPresets service unavailable — IM agent "${sessionId}" was created WITHOUT tools | `
            + 'agentPresets 服务不可用，该 IM 会话未挂载任何工具（文件/shell 工具不可用）。'
            + ` 请确认 profile 已加载 agent preset registry，且 preset "${presetId}" 存在。`,
          );
          return;
        }
        await presets.mount(agentCtx, presetId);
      },
    });
    // 记住 handle：释放 agent 只能通过 handle.dispose()（agents 服务没有 dispose 方法）。
    // 没有这一步，/new 无法释放旧 agent，确定性 sessionId 会永远冲突
    // （`session "<id>" already exists`）。
    this.agentHandles.set(sessionId, handle);
    return handle.agent;
  }

  /** 释放某个会话的 agent（幂等）。仅 handle 能释放，服务层无此能力。 */
  async disposeAgent(sessionId) {
    const handle = this.agentHandles.get(sessionId);
    if (!handle) return false;
    this.agentHandles.delete(sessionId);
    try {
      await handle.dispose();
      return true;
    } catch (err) {
      this.log.warn(`dispose agent ${sessionId} failed | 释放 agent 失败: ${err.message}`);
      return false;
    }
  }

  async agentOptions() {
    const cfg = this.cfg.agent;
    if (cfg.provider && cfg.model) return { provider: cfg.provider, model: cfg.model };
    const defaultModel = this.ctx.get('agentDefaultModel');
    if (defaultModel && typeof defaultModel.get === 'function') {
      try {
        const m = await defaultModel.get();
        if (m?.provider && m?.model) return { provider: m.provider, model: m.model };
      } catch { /* fall through */ }
    }
    return { ...DEFAULT_MODEL, ...(cfg.provider ? { provider: cfg.provider } : {}), ...(cfg.model ? { model: cfg.model } : {}) };
  }

  workspace() {
    return this.cfg.agent.workspace || process.cwd();
  }

  /** 构造 UserMessage（dsh-llm 词汇；官方助手生成稳定 id，避免并发 followup 冲突）。 */
  userMessage(text) {
    return createUserMessage({
      content: [{ type: 'text', text }],
      source: { kind: 'user' },
    });
  }

  // ── 命令（FR-4） ─────────────────────────────────────────────────────────

  registerCommands() {
    const core = this;

    registerCommand('start', {
      perm: 'user',
      usage: '',
      desc: '创建新会话',
      descEn: 'create a new session',
      run: (c) => c.core.commandNew(c.msg),
    });
    registerCommand('new', {
      perm: 'user',
      usage: '',
      desc: '创建新会话',
      descEn: 'create a new session',
      run: (c) => c.core.commandNew(c.msg),
    });
    registerCommand('status', {
      perm: 'user',
      usage: '',
      desc: '当前会话、渠道连接、审批',
      descEn: 'sessions, channels, approvals',
      run: (c) => c.core.commandStatus(c.msg),
    });
    registerCommand('log', {
      perm: 'user',
      usage: '',
      desc: '把当前/上一任务完整输出发回（文件或长文本）',
      descEn: 'deliver the full output as file',
      run: (c) => c.core.commandLog(c.msg),
    });
    registerCommand('help', {
      perm: 'user',
      usage: '',
      desc: '命令与用法',
      descEn: 'show help',
      run: (c) => c.core.commandHelp(c.msg),
    });
    registerCommand('mute', {
      perm: 'user',
      usage: '',
      desc: '本聊天通知开关（关）',
      descEn: 'mute notifications for this chat',
      run: (c) => c.core.commandMute(c.msg, true),
    });
    registerCommand('unmute', {
      perm: 'user',
      usage: '',
      desc: '本聊天通知开关（开）',
      descEn: 'unmute notifications',
      run: (c) => c.core.commandMute(c.msg, false),
    });
    registerCommand('approve', {
      perm: 'user',
      usage: '<id> yes|no',
      desc: '文本方式审批（无按钮渠道降级路径）',
      descEn: 'approve/reject by text',
      run: (c, args) => c.core.commandApprove(c.msg, args),
    });
    registerCommand('answer', {
      perm: 'user',
      usage: '<id> <编号>[,<编号>] | skip',
      desc: '回答 agent 的提问（无按钮渠道降级路径）；skip 跳过让 agent 自行决定',
      descEn: 'answer an agent question by option number, or skip',
      run: (c, args) => c.core.commandAnswer(c.msg, args),
    });
    registerCommand('trust', {
      perm: 'admin',
      usage: '<platform:userId>',
      desc: '信任用户（写入 allowlist）',
      descEn: 'trust a user',
      run: (c, args) => c.core.commandTrust(c.msg, args),
    });
    registerCommand('revoke', {
      perm: 'admin',
      usage: '<platform:userId>',
      desc: '撤销用户授权',
      descEn: 'revoke a user',
      run: (c, args) => c.core.commandRevoke(c.msg, args),
    });
    // 预留（v2）：/resume <id>、/attach <path>、/bind、/share
  }

  async handleCommand(msg, parsed) {
    const def = commands.get(parsed.name);
    const ctx = {
      msg,
      map: this.map,
      core: this,
    };
    const allowed = def.perm === 'admin'
      ? this.isAdmin(msg.platform, msg.userId)
      : this.isAllowed(msg.platform, msg.userId);
    if (!allowed) {
      await this.send({ platform: msg.platform, chatId: msg.chatId }, {
        text: `⛔ 命令 /${parsed.name} 需要 ${def.perm === 'admin' ? '管理员' : 'allowlist'} 权限。`,
      });
      return;
    }
    try {
      await def.run(ctx, parsed.args);
    } catch (err) {
      this.log.error(`command /${parsed.name} failed: %o`, err);
      await this.send({ platform: msg.platform, chatId: msg.chatId }, {
        text: `❌ 命令执行失败：${err instanceof Error ? err.message : String(err)}`,
      }).catch(() => {});
    }
  }

  async commandNew(msg) {
    const { platform, chatId, userId } = msg;
    const cfg = this.cfg.security;
    const existing = this.map.get(platform, chatId);
    if (existing) {
      // 旧会话释放（历史日志保留在磁盘，/resume v2 可恢复）
      const oldAgent = this.ctx.agents.get(existing.sessionId);
      if (oldAgent && oldAgent.status !== 'idle') {
        await this.send({ platform, chatId }, { text: '⏳ 当前会话仍在运行，先等它结束或取消后再新建。' });
        return;
      }
      // 释放旧 agent：它只要还活在 session store 里，同名会话就会冲突。
      // 注意仅 handle 能释放（agents 服务没有 dispose 方法），见 disposeAgent。
      await this.disposeAgent(existing.sessionId);
      this.map.remove(platform, chatId);
    }
    if (this.map.size >= cfg.maxSessions) {
      await this.send({ platform, chatId }, {
        text: `⛔ 会话数已达上限（${cfg.maxSessions}）。请用 /status 查看，或清理旧会话后重试。`,
      });
      return;
    }
    // ⚠️ /new 必须换一个**新的** session id。确定性 id（im-<platform>-<chatId>）
    // 对应的会话日志已持久化在磁盘上，即使旧 agent 已释放，再用同一 id 去
    // create 仍会抛 `session "<id>" already exists`。
    const newSessionId = this.freshSessionId(platform, chatId);
    const binding = this.createBinding(platform, chatId, msg.chatType ?? 'private', newSessionId);
    await this.createAgent(binding.sessionId);
    await this.send({ platform, chatId }, {
      text: `✅ 新会话已创建（${binding.sessionId}）。\n直接发送任务即可，例如：\n> 跑一下 tests 目录的 pytest`,
    });
  }

  async commandStatus(msg) {
    const { platform, chatId } = msg;
    const lines = ['📡 **渠道连接**'];
    for (const [p, ch] of this.channels) {
      const st = ch.status;
      const ok = !st || st.connected !== false;
      lines.push(`  ${ok ? '✅' : '❌'} ${p}${st?.detail ? ` (${st.detail})` : ''}`);
    }
    lines.push('', `💬 **会话** ${this.map.size}/${this.cfg.security.maxSessions}`);
    for (const s of this.map.list().slice(-5)) {
      lines.push(`  • ${s.platform}:${s.chatId} → ${s.sessionId}（${s.chatType}）`);
    }
    const pending = this.approvals.pendingList();
    lines.push('', `🔐 **审批** ${pending.length} 个等待中`);
    for (const p of pending.slice(-5)) {
      lines.push(`  • #${p.id} ${p.tool}（${p.state}，${p.ageSec}s）`);
    }
    const allowlist = [...this.map.allowlist];
    lines.push('', `👥 **allowlist** ${allowlist.length} 人`);
    if (allowlist.length > 5) lines.push(`  ${allowlist.slice(0, 5).join(', ')} …`);
    else if (allowlist.length) lines.push(`  ${allowlist.join(', ')}`);
    await this.send({ platform, chatId }, { text: lines.join('\n') });
  }

  async commandLog(msg) {
    const binding = this.map.get(msg.platform, msg.chatId);
    if (!binding) {
      return this.send({ platform: msg.platform, chatId: msg.chatId }, { text: 'ℹ️ 尚未创建会话。' });
    }
    const full = this._lastFullOutput.get(binding.sessionId);
    if (!full) {
      return this.send({ platform: msg.platform, chatId: msg.chatId }, { text: 'ℹ️ 暂无完整输出。' });
    }
    const channel = this.channels.get(msg.platform);
    if (channel && typeof channel.sendFile === 'function') {
      const r = await channel.sendFile(binding.chatId, `im-${binding.sessionId}.md`, full, 'text/markdown');
      // 适配器可能在没有投递路径时返回 {failed:true} —— 不能就此当作已交付
      if (r && r.failed) {
        await this.send({ platform: msg.platform, chatId: msg.chatId }, {
          text: `⚠️ 无法投递全文（${r.reason ?? '未知原因'}）。请在聊天里发一条消息刷新会话后再试。`,
        });
      }
      return;
    }
    // 无 sendFile 的渠道：长文本分段发送
    const chunks = splitLongText(markdownToText(full), { maxChunks: 6 });
    for (const chunk of chunks) {
      await this.send({ platform: msg.platform, chatId: msg.chatId }, { text: chunk });
    }
  }

  async commandHelp(msg) {
    await this.send({ platform: msg.platform, chatId: msg.chatId }, { text: helpText() });
  }

  async commandMute(msg, muted) {
    const binding = this.map.get(msg.platform, msg.chatId);
    if (!binding) {
      return this.send({ platform: msg.platform, chatId: msg.chatId }, { text: 'ℹ️ 先创建会话（/new）。' });
    }
    binding.muted = muted;
    binding.mutedBy = `${msg.platform}:${msg.userId}`;
    await this.map.save();
    await this.send({ platform: msg.platform, chatId: msg.chatId }, {
      text: muted ? '🔕 本聊天通知已关闭（审批仍会推送）。' : '🔔 本聊天通知已开启。',
    });
  }

  async commandApprove(msg, args) {
    const [id, answer] = args;
    if (!id || !answer || !['yes', 'no'].includes(answer)) {
      return this.send({ platform: msg.platform, chatId: msg.chatId }, {
        text: '用法：/approve <id> yes|no（如 /approve a1b2c3d4 yes）',
      });
    }
    const result = this.approvals.respond(id, answer, {
      platform: msg.platform, userId: msg.userId, userName: msg.userName,
    });
    const texts = {
      accepted: `✅ 已批准 #${id}，agent 继续执行。`,
      rejected: `❌ 已拒绝 #${id}。`,
      ignored: `ℹ️ 审批 #${id} 已被响应或不存在。`,
      'not-found': `ℹ️ 审批 #${id} 不存在或已结束。`,
      forbidden: `⛔ 无权限：审批需要 allowlist 成员身份。`,
    };
    await this.send({ platform: msg.platform, chatId: msg.chatId }, { text: texts[result] ?? `ℹ️ ${result}` });
  }

  /**
   * `/answer <id> <编号>[,<编号>] [自定义文字]`
   * 按钮不可用时的降级路径。多问题时编号形如 `1.2`（问题序号.选项序号）；
   * 单问题时直接写选项序号。
   *
   * `/answer <id> skip`（或 `0`）→ 跳过选择，让 agent 自行决定后继续。
   */
  async commandAnswer(msg, args) {
    const chat = { platform: msg.platform, chatId: msg.chatId };
    const [id, picksRaw, ...customParts] = args;
    if (!id || !picksRaw) {
      return this.send(chat, {
        text: '用法：/answer <id> <编号>[,<编号>] [自定义文字]\n'
          + '      /answer <id> skip        跳过选择，让 agent 自行决定\n'
          + '例：/answer 01 2   或   /answer 01 1.1,1.3   （编号见提问卡片）',
      });
    }
    if (!this.userQuestions) {
      return this.send(chat, { text: 'ℹ️ 提问应答未启用。' });
    }

    // findRecord 同时覆盖「等待中」与「已挂起可续答（timed 超时后）」两种记录
    const rec = this.userQuestions.findRecord(String(id));
    if (!rec) {
      return this.send(chat, { text: `ℹ️ 提问 #${id} 不存在、已回答或已超时。` });
    }
    const isContinued = !this.userQuestions.records.has(String(id));

    // 提交草稿：/answer <id> done —— 未答的题按跳过处理（配合按钮逐题累计）
    if (/^(done|提交|ok)$/i.test(String(picksRaw).trim())) {
      const r = this.userQuestions.commitDraft(String(id), { platform: msg.platform, chatId: msg.chatId });
      const t = {
        answered: `✅ 已提交提问 #${id} 的当前选择，agent 继续。`,
        empty: `ℹ️ 提问 #${id} 还没有任何选择——请先按编号或点按钮选择。`,
        'not-found': `ℹ️ 提问 #${id} 不存在、已回答或已超时。`,
        forbidden: `⛔ 该提问属于其它会话，不能在此代答。`,
        continued: `✅ 已提交提问 #${id} 的当前选择（此前已超时挂起，答案已转交 agent）。`,
        'no-continuation': '⚠️ 该提问已超时挂起，当前无法转交答案——请让 agent 重新提问。',
        'delivery-error': '⚠️ 答案转交失败（不是你的格式问题），请稍后重试。',
      };
      return this.send(chat, { text: t[r] ?? `ℹ️ ${r}` });
    }

    // 跳过：不选任何选项，返回空答案，agent 自行决定
    if (/^(skip|跳过|0)$/i.test(String(picksRaw).trim())) {
      const r = this.userQuestions.skip(String(id), { platform: msg.platform, chatId: msg.chatId });
      const t = {
        skipped: `⏭️ 已跳过提问 #${id}，agent 将自行决定后继续。`,
        'not-found': `ℹ️ 提问 #${id} 不存在、已回答或已超时。`,
        forbidden: `⛔ 该提问属于其它会话，不能在此代答。`,
        continued: `⏭️ 已跳过提问 #${id}（此前已超时挂起，答案已转交 agent）。`,
        'no-continuation': '⚠️ 该提问已超时挂起，当前无法转交答案。',
        'delivery-error': '⚠️ 答案转交失败（不是你操作的问题），请稍后重试。',
      };
      return this.send(chat, { text: t[r] ?? `ℹ️ ${r}` });
    }

    // 自由文本作答：`/answer <id> text <内容>`。
    // 混合题型（部分有选项、部分是自由文本）必须走这条**显式**路径 ——
    // 入站普通文字不再被自动吞成答案（那会丢掉用户想派的新任务，round-6 发现）。
    const textMatch = String(picksRaw).match(/^text$/i);
    if (textMatch) {
      const content = customParts.join(' ').trim();
      if (!content) {
        return this.send(chat, { text: `用法：/answer ${id} text <内容>` });
      }
      const r = this.userQuestions.answerText(String(id), content, { platform: msg.platform, chatId: msg.chatId });
      const t = {
        answered: `✅ 已回答提问 #${id} 的文字题，agent 继续。`,
        partial: `📝 已记录提问 #${id} 第 ${(r.qi ?? 0) + 1} 题的文字；还有 ${r.remainingText} 道文字题未答。`,
        continued: `✅ 已回答提问 #${id}（此前已超时挂起，答案已转交 agent）。`,
        invalid: r.reason === 'no-text-question'
          ? '⚠️ 该提问没有自由文本题，请用编号作答。'
          : '⚠️ 该提问的文字题都已作答。',
        forbidden: `⛔ 该提问属于其它会话，不能在此代答。`,
        'not-found': `ℹ️ 提问 #${id} 不存在、已回答或已超时。`,
        'no-continuation': '⚠️ 该提问已超时挂起，当前无法转交答案——请让 agent 重新提问。',
        'delivery-error': '⚠️ 答案转交失败（不是你的格式问题），请稍后重试。',
      };
      return this.send(chat, { text: t[r.status] ?? `ℹ️ ${r.status}` });
    }

    const multi = rec.questions.length > 1;
    // 分隔符与入站劫持正则保持一致：`,`、中文逗号、空白都接受
    // （此前入站接受 `1 2`/`1，2` 并劫持消息，这里却只 split(',')，导致整条消息被吞掉后报"编号无效"）
    const picks = [];
    for (const token of String(picksRaw).split(/[,，\s]+/)) {
      const t = token.trim();
      if (!t) continue;
      if (multi && t.includes('.')) {
        const [q, o] = t.split('.').map((x) => Number(x));
        picks.push({ qIndex: q - 1, oIndex: o - 1 });
      } else {
        // 多问题但只给裸编号 → 视为第 1 题的该选项；单问题 → 本就如此
        picks.push({ qIndex: 0, oIndex: Number(t) - 1 });
      }
    }
    if (picks.length === 0 || picks.some((p) => !Number.isInteger(p.oIndex) || p.oIndex < 0)) {
      return this.send(chat, { text: '⚠️ 编号无效，请按卡片上的编号回复。' });
    }

    const custom = customParts.length ? customParts.join(' ') : undefined;
    const result = this.userQuestions.respond(String(id), picks, custom, {
      platform: msg.platform, chatId: msg.chatId,
    }, isContinued ? { allowPartial: true } : {});
    const texts = {
      answered: `✅ 已回答提问 #${id}，agent 继续。`,
      'not-found': `ℹ️ 提问 #${id} 不存在、已回答或已超时。`,
      invalid: rec.questions.some((q) => !(q.options ?? []).length)
        // 卡片含自由文本题：文字题必须用显式语法，编号只能答选项题
        ? `⚠️ 还没答完——该卡片含**自由文本题**，请用 \`/answer ${id} text <内容>\` 回答文字题，或发 \`/answer ${id} done\` 提交当前选择。`
        : (isContinued
          ? '⚠️ 编号无效（该提问此前已超时挂起，请按原卡片编号重发）。'
          : '⚠️ 编号越界，提问仍在等待——请按卡片编号重发。'),
      forbidden: `⛔ 该提问属于其它会话，不能在此代答。`,
      continued: `✅ 已回答提问 #${id}（此前已超时挂起，答案已转交 agent）。`,
      'no-continuation': '⚠️ 该提问已超时挂起，当前无法转交答案——请让 agent 重新提问。',
      'delivery-error': '⚠️ 答案转交失败（不是你的格式问题），请稍后重试。',
    };
    await this.send(chat, { text: texts[result] ?? `ℹ️ ${result}` });
  }

  async commandTrust(msg, args) {
    const [raw] = args;
    const parsed = parseUserKey(raw, msg.platform);
    if (!parsed) {
      return this.send({ platform: msg.platform, chatId: msg.chatId }, { text: '用法：/trust <platform:userId>' });
    }
    this.map.addToAllowlist(parsed.platform, parsed.userId);
    this.map.addAdmin(parsed.platform, parsed.userId);
    await this.send({ platform: msg.platform, chatId: msg.chatId }, {
      text: `✅ 已信任 ${parsed.key}（管理员）。对方可开始派活。`,
    });
  }

  async commandRevoke(msg, args) {
    const [raw] = args;
    const parsed = parseUserKey(raw, msg.platform);
    if (!parsed) {
      return this.send({ platform: msg.platform, chatId: msg.chatId }, { text: '用法：/revoke <platform:userId>' });
    }
    this.map.removeFromAllowlist(parsed.platform, parsed.userId);
    this.map.removeAdmin(parsed.platform, parsed.userId);
    await this.send({ platform: msg.platform, chatId: msg.chatId }, {
      text: `✅ 已撤销 ${parsed.key}。`,
    });
  }

  // ── 工具 ─────────────────────────────────────────────────────────────────

  /** 追加式日志（approvals.log 等，FR-6.7）。 */
  async appendLog(kind, line) {
    try {
      await mkdir(this.storeDir, { recursive: true });
      await appendFile(join(this.storeDir, `${kind}.log`), JSON.stringify(line) + '\n', 'utf8');
    } catch (err) {
      this.log.warn(`append ${kind}.log failed: ${err.message}`);
    }
  }

  /** 内部事件（其他插件可复用：im/message、im/command、im/dispatch、im/approval）。 */
  emit(type, payload) {
    this.ctx.emit(`im/${type}`, payload);
  }

  /** /status 用：渠道连接状态。 */
  channelsStatus() {
    return [...this.channels.entries()].map(([p, ch]) => ({
      platform: p,
      connected: !ch.status || ch.status.connected !== false,
      detail: ch.status?.detail ?? '',
    }));
  }

  async dispose() {
    await this._ready;
    for (const d of this._dispose) d();
    this._dispose = [];
  }
}

export default ImRuntime;
export { name, inject, Config, sessionIdFor, chatKey, userKey };
export { MockChannel } from './mock-channel.js';
