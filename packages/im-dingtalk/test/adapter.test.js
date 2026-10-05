// 接线测试（AGENTS.md 规则 #3/#9）：stub 官方 SDK，CI 不碰真实凭据
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { apply, parseContent, stripAt, normalizeRobotMessage, parseCardCallback, buttonsAsCommands } from '../lib/index.js';
import { TOPIC_ROBOT, TOPIC_CARD } from '../lib/index.js';

/** 构造一个假 ctx：捕获 registerChannel / dispatchInbound / handleCallback。 */
function makeCtx() {
  const captured = { channel: null, inbound: [], callbacks: [] };
  const im = {
    registerChannel(ch) {
      captured.channel = ch;
      return () => {};
    },
    async dispatchInbound(msg) {
      captured.inbound.push(msg);
    },
    async handleCallback(cb) {
      captured.callbacks.push(cb);
    },
  };
  const ctx = {
    get: (k) => (k === 'im' ? im : undefined),
    logger: () => ({ info() {}, warn() {}, error() {}, debug() {} }),
  };
  return { ctx, captured };
}

/**
 * stub dingtalk-stream：记录订阅与连接，暴露触发下行事件的入口。
 * 关键：区分 registerCallbackListener（CALLBACK）与 registerAllEventListener（EVENT），
 * 这样"只订 EVENT 导致收不到机器人消息"这类回归会在测试里直接暴露。
 */
function makeSdkStub() {
  const state = {
    connected: false,
    listener: null,
    callbackTopics: [], // registerCallbackListener 注册的 topic
    allEventRegistered: false,
    subscriptions: [],
    acked: [],
    responses: [],
  };
  class DWClient {
    constructor(opts) {
      state.opts = opts;
      state.instance = this;
    }
    /** 复刻真实 SDK：CALLBACK 订阅会被 push 进 subscriptions。 */
    registerCallbackListener(topic, fn) {
      state.callbackTopics.push(topic);
      if (!state.subscriptions.some((s) => s.topic === topic && s.type === 'CALLBACK')) {
        state.subscriptions.push({ type: 'CALLBACK', topic });
      }
      state.listener = fn;
      return this;
    }
    /** 复刻真实 SDK：registerAllEventListener 只订阅 EVENT/*。 */
    registerAllEventListener(fn) {
      state.allEventRegistered = true;
      if (!state.subscriptions.some((s) => s.topic === '*' && s.type === 'EVENT')) {
        state.subscriptions.push({ type: 'EVENT', topic: '*' });
      }
      state.listener = fn;
      return this;
    }
    getConfig() {
      return { subscriptions: state.subscriptions };
    }
    async connect() {
      state.connected = true;
    }
    disconnect() {
      state.connected = false;
    }
    socketCallBackResponse(messageId, result) {
      state.responses.push({ messageId, result });
    }
  }
  return { sdk: { DWClient, TOPIC_ROBOT }, state };
}

const CREDS = { clientId: 'test-client-id', clientSecret: 'test-secret', robotCode: '' };

// ── 回归测试：机器人消息必须用 CALLBACK 订阅（真实环境踩过的坑） ──────────
//
// 症状：连接成功、status.connected=true，但永远收不到任何机器人消息。
// 根因：机器人消息与卡片回调都是 type=CALLBACK；registerAllEventListener 只订阅
//        EVENT/*（topic:'*' 不含 CALLBACK）。必须显式 registerCallbackListener。
// 依据：官方 Python SDK 对每个 callback handler 都 push {type:'CALLBACK', topic}。
// 这组测试锁死该行为——改回只订 EVENT 即变红。

test('回归：必须用 registerCallbackListener 订阅机器人消息 topic（CALLBACK）', async () => {
  const { ctx } = makeCtx();
  const { sdk, state } = makeSdkStub();
  apply(ctx, CREDS, { sdk });
  await new Promise((r) => setTimeout(r, 10));

  assert.ok(
    state.callbackTopics.includes(TOPIC_ROBOT),
    `未订阅 CALLBACK ${TOPIC_ROBOT} —— 机器人消息永远收不到（本回归的由来）`,
  );
  const subs = state.subscriptions;
  assert.ok(
    subs.some((s) => s.type === 'CALLBACK' && s.topic === TOPIC_ROBOT),
    'CALLBACK 订阅必须真的进入 subscriptions（会被发给网关）',
  );
});

test('回归：卡片回调 topic 也必须是 CALLBACK 订阅', async () => {
  const { ctx } = makeCtx();
  const { sdk, state } = makeSdkStub();
  apply(ctx, CREDS, { sdk });
  await new Promise((r) => setTimeout(r, 10));

  assert.ok(
    state.callbackTopics.includes(TOPIC_CARD),
    `未订阅 CALLBACK ${TOPIC_CARD} —— 审批卡片按钮收不到回调`,
  );
  assert.ok(
    state.subscriptions.some((s) => s.type === 'CALLBACK' && s.topic === TOPIC_CARD),
    '卡片 CALLBACK 订阅必须进入 subscriptions',
  );
});

test('回归：CALLBACK 与 EVENT 订阅并存（EVENT/* 不能替代 CALLBACK）', async () => {
  const { ctx } = makeCtx();
  const { sdk, state } = makeSdkStub();
  apply(ctx, CREDS, { sdk });
  await new Promise((r) => setTimeout(r, 10));

  // 两者都要有：EVENT/* 用于系统事件，CALLBACK 用于机器人消息
  assert.ok(state.allEventRegistered, 'registerAllEventListener 仍应保留');
  assert.equal(
    state.subscriptions.find((s) => s.topic === '*')?.type,
    'EVENT',
    "topic:'*' 只能是 EVENT 类型——它不是 CALLBACK 的替代品",
  );
  assert.ok(
    state.subscriptions.filter((s) => s.type === 'CALLBACK').length >= 2,
    '机器人与卡片两个 CALLBACK 订阅都应存在',
  );
});

test('stub SDK：注册渠道、连接成功、status 带 lastEventAt 心跳', async () => {
  const { ctx, captured } = makeCtx();
  const { sdk, state } = makeSdkStub();
  const dispose = apply(ctx, CREDS, { sdk });
  await new Promise((r) => setTimeout(r, 10));

  assert.equal(captured.channel.platform, 'dingtalk');
  assert.equal(typeof captured.channel.send, 'function');
  assert.equal(typeof captured.channel.sendFile, 'function');
  assert.equal(typeof captured.channel.dispose, 'function');
  assert.equal(state.connected, true);
  assert.equal(captured.channel.status.connected, true);
  // 可观测三件套：心跳字段必须存在
  assert.ok('lastEventAt' in captured.channel.status);
  await dispose();
  assert.equal(state.connected, false);
});

test('stub SDK：机器人消息 → ImMessage → dispatchInbound（含入站心跳）', async () => {
  const { ctx, captured } = makeCtx();
  const { sdk, state } = makeSdkStub();
  apply(ctx, CREDS, { sdk });
  await new Promise((r) => setTimeout(r, 10));

  assert.ok(state.listener, 'CALLBACK listener 必须已注册（机器人消息走 CALLBACK）');
  const ack = state.listener({
    headers: { topic: TOPIC_ROBOT, messageId: 'm1' },
    data: JSON.stringify({
      msgtype: 'text',
      text: { content: ' 列出当前目录 ' },
      conversationId: 'cid-1',
      conversationType: '1',
      senderStaffId: 'staff-9',
      senderNick: '张三',
      msgId: 'msg-1',
      sessionWebhook: 'https://oapi.dingtalk.com/robot/sendBySession?session=abc',
      sessionWebhookExpiredTime: Date.now() + 3600_000,
    }),
  });
  await new Promise((r) => setTimeout(r, 10));

  assert.equal(ack.status, 'SUCCESS', '必须立即 ACK 避免服务端重推');
  assert.equal(captured.inbound.length, 1);
  const m = captured.inbound[0];
  assert.equal(m.platform, 'dingtalk');
  assert.equal(m.chatId, 'cid-1');
  assert.equal(m.userId, 'staff-9');
  assert.equal(m.userName, '张三');
  assert.equal(m.text, '列出当前目录');
  assert.equal(m.chatType, 'private');
  assert.equal(m.msgId, 'msg-1');
  assert.ok(captured.channel.status.lastEventAt > 0, '收到事件必须更新心跳');
});

test('stub SDK：卡片回调 → handleCallback（中性 approve 载荷）', async () => {
  const { ctx, captured } = makeCtx();
  const { sdk, state } = makeSdkStub();
  apply(ctx, CREDS, { sdk });
  await new Promise((r) => setTimeout(r, 10));

  state.listener({
    headers: { topic: TOPIC_CARD, messageId: 'card-1' },
    data: JSON.stringify({
      userId: 'staff-9',
      userName: '张三',
      conversationId: 'cid-1',
      content: JSON.stringify({ action: 'approve:ab12cd:yes' }),
    }),
  });
  await new Promise((r) => setTimeout(r, 10));

  assert.equal(captured.callbacks.length, 1);
  assert.deepEqual(captured.callbacks[0], {
    platform: 'dingtalk',
    chatId: 'cid-1',
    userId: 'staff-9',
    userName: '张三',
    data: 'approve:ab12cd:yes',
  });
  // 卡片必须回执，否则客户端转圈
  assert.equal(state.responses.length, 1);
  assert.equal(state.responses[0].messageId, 'card-1');
});

test('出站：sessionWebhook 优先，发送 markdown 载荷', async () => {
  const { ctx, captured } = makeCtx();
  const { sdk, state } = makeSdkStub();
  const sent = [];
  const fetchImpl = async (url, init) => {
    sent.push({ url, body: JSON.parse(init.body) });
    return { ok: true, json: async () => ({ errcode: 0 }) }  ;
  };
  apply(ctx, CREDS, { sdk, fetchImpl });
  await new Promise((r) => setTimeout(r, 10));

  // 先经入站学习 sessionWebhook
  state.listener({
    headers: { topic: TOPIC_ROBOT },
    data: JSON.stringify({
      msgtype: 'text', text: { content: 'hi' },
      conversationId: 'cid-1', conversationType: '1',
      senderStaffId: 'staff-9', msgId: 'm1',
      sessionWebhook: 'https://hook.example/session', sessionWebhookExpiredTime: Date.now() + 3600_000,
    }),
  });
  await new Promise((r) => setTimeout(r, 10));

  await captured.channel.send({ chatId: 'cid-1', text: '任务完成' });
  assert.equal(sent.length, 1);
  assert.equal(sent[0].url, 'https://hook.example/session');
  assert.equal(sent[0].body.msgtype, 'markdown');
  assert.equal(sent[0].body.markdown.text, '任务完成');
});

test('回归：机器人回推必须用 userId，不能用 conversationId（staffId.notExisted）', async () => {
  const { ctx, captured } = makeCtx();
  const { sdk, state } = makeSdkStub();
  const calls = [];
  const fetchImpl = async (url, init) => {
    if (String(url).includes('gettoken')) {
      return { ok: true, json: async () => ({ errcode: 0, access_token: 'tok', expires_in: 7200 }) };
    }
    calls.push({ body: JSON.parse(init.body) });
    return { ok: true, json: async () => ({ processQueryKey: 'pq', invalidStaffIdList: [], filteredStaffIdList: [] }) };
  };
  apply(ctx, { clientId: 'ding-x', clientSecret: 's' }, { sdk, fetchImpl });
  await new Promise((r) => setTimeout(r, 10));

  // 入站：私聊场景，conversationId ≠ userId（钉钉真实形态）
  state.listener({
    headers: { topic: TOPIC_ROBOT },
    data: JSON.stringify({
      msgtype: 'text',
      text: { content: 'hi' },
      conversationId: 'cidyOPAQUE+/==', // 会话级不透明串
      conversationType: '1',
      senderStaffId: '10001', // 真正的 userId（示例值）
      senderNick: '张三',
      msgId: 'm1',
      sessionWebhookExpiredTime: 0, // 无可用 webhook → 走机器人 API
    }),
  });
  await new Promise((r) => setTimeout(r, 10));

  await captured.channel.send({ chatId: 'cidyOPAQUE+/==', text: '回复' });
  assert.equal(calls.length, 1, '应走机器人推送');
  assert.deepEqual(
    calls[0].body.userIds,
    ['10001'],
    'userIds 必须是 senderStaffId（userId），传 conversationId 会 staffId.notExisted',
  );
});

test('回归：batchSend 返回 invalidStaffIdList 必须报错（HTTP 200 也会静默失败）', async () => {
  const { ctx, captured } = makeCtx();
  const { sdk, state } = makeSdkStub();
  const fetchImpl = async (url) => {
    if (String(url).includes('gettoken')) {
      return { ok: true, json: async () => ({ errcode: 0, access_token: 'tok', expires_in: 7200 }) };
    }
    // HTTP 200 但目标被拒 —— 这是钉钉的真实行为
    return { ok: true, json: async () => ({ processQueryKey: 'pq', invalidStaffIdList: ['bad-id'] }) };
  };
  apply(ctx, { clientId: 'ding-x', clientSecret: 's' }, { sdk, fetchImpl });
  await new Promise((r) => setTimeout(r, 10));
  state.listener({
    headers: { topic: TOPIC_ROBOT },
    data: JSON.stringify({
      msgtype: 'text', text: { content: 'hi' },
      conversationId: 'c1', conversationType: '1', senderStaffId: 'bad-id',
      msgId: 'm1', sessionWebhookExpiredTime: 0,
    }),
  });
  await new Promise((r) => setTimeout(r, 10));

  // 不得伪造成功：send() 内部吞错返回 {}，但不能返回 messageId
  const r = await captured.channel.send({ chatId: 'c1', text: 'x' });
  assert.deepEqual(r, {}, 'invalidStaffIdList 非空时不得报告成功');
});

test('出站：无 sessionWebhook → 走机器人推送且不抛错（robotCode 回退 clientId）', async () => {
  const { ctx, captured } = makeCtx();
  const { sdk } = makeSdkStub();
  const fetchImpl = async (url) => {
    if (String(url).includes('gettoken')) {
      return { ok: true, json: async () => ({ errcode: 0, access_token: 'tok', expires_in: 7200 }) };
    }
    return { ok: true, json: async () => ({ processQueryKey: 'pq-x' }) };
  };
  apply(ctx, CREDS, { sdk, fetchImpl });
  await new Promise((r) => setTimeout(r, 10));
  const r = await captured.channel.send({ chatId: 'nobody', text: 'x' });
  assert.equal(r.messageId, 'pq-x');
});

test('出站：机器人 API 报错必须带平台错误码（规则 #5 边界日志）', async () => {
  const { ctx, captured } = makeCtx();
  const { sdk } = makeSdkStub();
  const fetchImpl = async (url) => {
    if (String(url).includes('gettoken')) {
      return { ok: true, json: async () => ({ errcode: 0, access_token: 'tok', expires_in: 7200 }) };
    }
    return { ok: false, status: 400, json: async () => ({ code: 'invalidParameter.robotCode.notExsit', message: 'robotCode 不存在' }) };
  };
  apply(ctx, { clientId: 'ding-x', clientSecret: 's' }, { sdk, fetchImpl });
  await new Promise((r) => setTimeout(r, 10));
  // send() 内部 deliver 会吞掉错误并返回 {}，但错误必须可被观测到（此处仅断言不崩溃且不误报成功）
  const r = await captured.channel.send({ chatId: 'u1', text: 'x' });
  assert.deepEqual(r, {}, '失败时不得伪造成功结果');
});

test('出站：robotCode 未配置时回退为 clientId（实测 Stream 机器人 robotCode == Client ID）', async () => {
  const { ctx, captured } = makeCtx();
  const { sdk } = makeSdkStub();
  const calls = [];
  const fetchImpl = async (url, init) => {
    // token 端点
    if (String(url).includes('gettoken')) {
      return { ok: true, json: async () => ({ errcode: 0, access_token: 'tok', expires_in: 7200 }) };
    }
    calls.push({ url: String(url), body: JSON.parse(init.body) });
    return { ok: true, json: async () => ({ processQueryKey: 'pq-1' }) };
  };
  // 不配 robotCode → 应回退为 clientId
  apply(ctx, { clientId: 'ding-abc', clientSecret: 'sec' }, { sdk, fetchImpl });
  await new Promise((r) => setTimeout(r, 10));

  // 无任何 sessionWebhook → 走机器人 API 分支
  const r = await captured.channel.send({ chatId: 'staff-1', text: '主动通知' });
  assert.equal(calls.length, 1, '应调用机器人推送 API');
  assert.equal(calls[0].body.robotCode, 'ding-abc', 'robotCode 必须回退为 clientId');
  assert.deepEqual(calls[0].body.userIds, ['staff-1']);
  assert.equal(r.messageId, 'pq-1');
});

test('出站：按钮降级为文本命令（钉钉 webhook 无内联按钮）', async () => {
  const { ctx, captured } = makeCtx();
  const { sdk, state } = makeSdkStub();
  const sent = [];
  const fetchImpl = async (url, init) => {
    sent.push(JSON.parse(init.body));
    return { ok: true, json: async () => ({ errcode: 0 }) };
  };
  apply(ctx, CREDS, { sdk, fetchImpl });
  await new Promise((r) => setTimeout(r, 10));
  state.listener({
    headers: { topic: TOPIC_ROBOT },
    data: JSON.stringify({
      msgtype: 'text', text: { content: 'hi' }, conversationId: 'cid-1', conversationType: '1',
      senderStaffId: 's', msgId: 'm1', sessionWebhook: 'https://hook.example/s', sessionWebhookExpiredTime: Date.now() + 3600_000,
    }),
  });
  await new Promise((r) => setTimeout(r, 10));

  await captured.channel.send({
    chatId: 'cid-1',
    text: '⚠️ 需要审批',
    buttons: [
      { id: 'approve:ab12cd:yes', label: '✅ 批准' },
      { id: 'approve:ab12cd:no', label: '❌ 拒绝' },
    ],
  });
  assert.equal(sent.length, 1);
  const text = sent[0].markdown.text;
  assert.ok(text.includes('approve:ab12cd:yes'), '按钮 id 必须出现在文本命令中');
  assert.ok(text.includes('✅ 批准'));
});

test('缺凭据：status 断开并给出可操作提示，不抛错', async () => {
  const { ctx, captured } = makeCtx();
  const { sdk, state } = makeSdkStub();
  apply(ctx, { clientId: '', clientSecret: '' }, { sdk });
  await new Promise((r) => setTimeout(r, 10));
  assert.equal(captured.channel.status.connected, false);
  assert.match(captured.channel.status.detail, /missing clientId/);
  assert.equal(state.connected, false, '缺凭据不得尝试连接');
  // 仍必须满足契约
  assert.ok('lastEventAt' in captured.channel.status);
});

test('连接失败：status 记录错误、不崩溃', async () => {
  const { ctx, captured } = makeCtx();
  class FailingClient {
    registerAllEventListener() { return this; }
    registerCallbackListener() { return this; }
    async connect() { throw new Error('gateway refused'); }
    disconnect() {}
  }
  apply(ctx, CREDS, { sdk: { DWClient: FailingClient, TOPIC_ROBOT } });
  await new Promise((r) => setTimeout(r, 10));
  assert.equal(captured.channel.status.connected, false);
  assert.match(captured.channel.status.detail, /gateway refused/);
});

// ── 纯函数单测（规则 #8：无网络） ──────────────────────────────────────────

test('parseContent：text/markdown/richText/picture/file + 未知降级', () => {
  assert.equal(parseContent('text', { text: { content: '  你好 ' } }), '你好');
  assert.equal(parseContent('markdown', { markdown: { text: '**粗体**' } }), '**粗体**');
  assert.equal(
    parseContent('richText', { content: { richText: [[{ text: '第一行' }], [{ text: '第二行' }, { type: 'picture' }]] } }),
    '第一行\n第二行[图片]',
  );
  assert.equal(parseContent('picture', {}), '[图片]');
  assert.equal(parseContent('file', { content: { fileName: 'a.txt' } }), '[文件] a.txt');
  assert.equal(parseContent('audio', {}), '');
  assert.equal(parseContent('', {}), '');
});

test('stripAt：去掉 @机器人 前导空白', () => {
  assert.equal(stripAt('  任务'), '任务');
  assert.equal(stripAt('\u2005任务'), '任务');
  assert.equal(stripAt(''), '');
  assert.equal(stripAt(null), '');
});

test('parseCardCallback：多种载荷形状 + 无效输入', () => {
  // 操作人在外层、按钮 value 内层 —— 两个层级都要能取到
  const outer = parseCardCallback({ content: JSON.stringify({ action: 'approve:a:yes' }), userId: 'u1', userName: '张三', conversationId: 'c1' });
  assert.equal(outer.data, 'approve:a:yes');
  assert.equal(outer.userId, 'u1');
  assert.equal(outer.userName, '张三', '操作人昵称在外层时必须保留');
  assert.equal(outer.chatId, 'c1');
  // 内层带身份时也能兜底
  const inner = parseCardCallback({ content: JSON.stringify({ data: 'trust:dingtalk:u1', userId: 'in1', nick: '李四' }) });
  assert.equal(inner.data, 'trust:dingtalk:u1');
  assert.equal(inner.userId, 'in1');
  assert.equal(inner.userName, '李四');
  assert.equal(parseCardCallback({ content: JSON.stringify({ value: { action: 'approve:b:no' } }) }).data, 'approve:b:no');
  assert.equal(parseCardCallback({}), null);
  assert.equal(parseCardCallback(null), null);
  assert.equal(parseCardCallback({ content: 'not-json' }).data, 'not-json');
});

test('normalizeRobotMessage：群聊/私聊判定与字段兜底', () => {
  const group = normalizeRobotMessage({ conversationType: '2', conversationId: 'g1', senderStaffId: 's1', senderNick: 'N', msgtype: 'text', text: { content: 'hi' } });
  assert.equal(group.chatType, 'group');
  const priv = normalizeRobotMessage({ conversationType: '1', conversationId: 'p1', senderStaffId: 's1', msgtype: 'text', text: { content: 'hi' } });
  assert.equal(priv.chatType, 'private');
  // chatId 兜底到 senderStaffId
  assert.equal(normalizeRobotMessage({ senderStaffId: 'only-staff', msgtype: 'text', text: { content: 'x' } }).chatId, 'only-staff');
});

test('buttonsAsCommands：按钮 → 可复制文本命令', () => {
  const s = buttonsAsCommands([{ id: 'approve:1:yes', label: '✅ 批准' }]);
  assert.ok(s.includes('approve:1:yes'));
  assert.ok(s.includes('✅ 批准'));
  assert.equal(buttonsAsCommands([]), '');
});