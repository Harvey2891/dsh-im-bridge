// 用户提问应答者（user-questions answerer，PRD 扩展）
//
// 背景：DSH 的 `ask_user_question` 工具通过 `ctx.userQuestions` 这个「能力接缝」
// 提问，由**调用方提供应答 UI**。GUI 有 dsh-client-ui-user-questions 弹选择框；
// 而 IM 渠道此前没有任何应答者，于是 waterfall 落到 noAnswerer，抛：
//     UserQuestionError: no user-questions answerer accepted the request (NO_PROVIDER)
// 表现就是「agent 在钉钉里提问，用户什么都看不到，任务直接失败」。
//
// 本模块把提问转成 IM 消息（选项做成按钮，复用审批卡片那套回调机制），
// 等用户在 IM 里回答后把结果回传给 waterfall。
//
// 应答通道与审批一致：
//   1) 按钮回调 → adapter → ctx.im.handleCallback({ data: 'q:<id>:<idx>' })
//   2) 文字命令 → /answer <id> <编号，逗号分隔>   （按钮不可用时的降级路径）

/** 用户提问的应答状态。 */
const STATE = {
  WAITING: 'waiting',
  ANSWERED: 'answered',
  TIMEOUT: 'timeout',
  CANCELLED: 'cancelled',
  CONTINUED: 'continued',
  UNAVAILABLE: 'unavailable',
};

export class UserQuestionAnswerer {
  /**
   * @param {object} deps
   * @param {import('@deepseek-ai/cordis').Context} deps.ctx
   * @param {object} deps.map   会话映射器（chatId → binding）
   * @param {(chat: {platform: string, chatId: string}, out: object) => Promise<any>} deps.send
   * @param {(line: string) => void} [deps.logLine]
   * @param {object} [deps.cfg] { enabled, timeoutSec }
   */
  constructor({ ctx, map, send, logLine, cfg = {} }) {
    this.ctx = ctx;
    this.map = map;
    this.send = send;
    this.logLine = logLine ?? (() => {});
    this.cfg = {
      enabled: cfg.enabled ?? true,
      // 0 = 无限等待（与 ask_user_question 的默认语义一致：默认等待）
      timeoutSec: cfg.timeoutSec ?? 0,
    };
    /** @type {Map<string, object>} questionId → record */
    this.records = new Map();
    /**
     * 已挂起（timed 超时）但仍可续答的提问，id → record。
     * DSH 的 `askTimed` 在前台等待超时后会以 `{pending:true}` 放行 agent，
     * 用户的迟答需要经 `userQuestions.answer(agent, callId, answers)` 送回
     * （user-questions/src/index.ts:165）。若此处直接删记录，迟答就永远找不到入口
     * —— round-2 发现 5。
     * @type {Map<string, object>}
     */
    this.continued = new Map();
    this._seq = 0;
    this._dispose = [];
  }

  /** 挂接 waterfall。注册即 effect（cordis 约定）。 */
  mount() {
    if (!this.cfg.enabled) {
      this.logLine('answerer NOT mounted: userQuestions.enabled=false');
      return;
    }
    this._dispose.push(
      // prepend: true 是关键。GUI 桥（api/remotes）也注册了
      // 'user-questions/request' waterfall 监听，把请求转发给浏览器。waterfall 中
      // **先返回者认领**、先注册者先执行——若 GUI 转发器排在前面，它会认领**所有**
      // 请求（含 IM 会话的），IM 应答者永远不被调用，表现就是「钉钉收不到选项」；
      // 而 GUI 侧等不到浏览器回应，几十秒后返回空答案（实测 46s + answers 全空）。
      // 故 IM 应答者必须 prepend：IM 会话自己应答，非 IM 会话 `next()` 委托回去，
      // GUI 行为不变。
      this.ctx.on('user-questions/request', (request, next) => this.answer(request, next), {
        prepend: true,
      }),
    );
    // 挂载即留痕：没有这条日志 = init() 没跑到这里（而非「请求没来」）。
    this.logLine('answerer mounted: listening on user-questions/request');
  }

  dispose() {
    for (const d of this._dispose.splice(0)) {
      try { d(); } catch { /* 卸载失败不影响其他 */ }
    }
    // 卸载时按应答契约（`{answers}`）结算，而不是把状态字符串当结果返回
    // （此前 `rec.resolve(STATE.CANCELLED)` 会把字符串 'cancelled' 交给调用方，
    // 破坏 `AskUserQuestionAnswer` 契约）；同时清掉计时器与 abort 监听，
    // 避免卸载后仍发超时消息或持有旧请求。
    for (const rec of [...this.records.values()]) {
      this._cleanup(rec);
      rec.state = STATE.CANCELLED;
      rec.resolve({ answers: [] });
    }
    this.records.clear();
    this.continued.clear();
  }

  /** 清理一条记录的计时器与 abort 监听（幂等）。 */
  _cleanup(rec) {
    if (rec.timer) { clearTimeout(rec.timer); rec.timer = null; }
    if (rec.onAbort && rec.signal?.removeEventListener) {
      rec.signal.removeEventListener('abort', rec.onAbort);
    }
    rec.onAbort = null;
  }

  /**
   * waterfall 监听：把提问发到 IM 并等待应答。
   * 不能应答时**必须**调用 next() 委托，不能吞掉请求。
   * @param {object} request AskUserQuestionRequestEvent
   * @param {() => Promise<object>} next
   */
  async answer(request, next) {
    const agent = request.agent ?? this.ctx.get('agents')?.currentInitiator?.();
    const sessionId = agent?.id ?? agent?.session?.id;
    const binding = sessionId ? this.map.bySessionId?.(sessionId) : null;
    // 诊断日志：确认监听是否被调用、binding 是否命中（排查「钉钉收不到提问」）
    this.logLine(
      `request received: session=${sessionId ?? '(none)'} questions=${request.questions?.length ?? 0} `
      + `binding=${binding ? `${binding.platform}:${binding.chatId.slice(0, 12)}…` : 'NONE→delegate'}`,
    );
    // 非 IM 会话（GUI/CLI 等）：交给别的应答者
    if (!binding) return next();

    const questions = request.questions ?? [];
    if (questions.length === 0) return next();

    const id = this._nextId();
    const record = this._createRecord(id, request, binding, questions);
    this.records.set(id, record);

    // 🔴 abort 监听必须在 `await this.send(...)` **之前**注册：
    // 若信号在发送期间就已 abort，之后再 addEventListener 不会补发事件，
    // 记录会永久停在 waiting（默认 timeoutSec=0 时无人能救），agent 侧也拿不到结果。
    //
    // 🔴 abort 必须 **reject**（抛错）而不是返回空答案：
    // DSH 的 `askTimed`（user-questions/src/index.ts:249-263）靠「signal.aborted ⇒
    // throw wait.signal.reason」把前台等待超时转成 `{pending:true}`，让用户稍后的答复
    // 经 `user-question-reply` 消息继续送达（continued 路径）。若我们在这里**成功**
    // 返回 `{answers:[]}`，DSH 会把它当成真实答案，late answer 通道随之关闭。
    // 非 timed 的 `ask()` 也会把 abort 转成 abortedQuestion，语义同样正确。
    const onAbort = () => {
      const err = new Error(
        request.wait?.timed
          ? 'ask_user_question timed out before the user answered'
          : 'user question aborted',
      );
      err.code = request.wait?.timed ? 'ASK_TIMED_OUT' : 'ABORTED';
      if (request.wait?.timed) {
        // timed：前台等待结束，但问题转为可续答（DSH 的 continued 语义）。
        // 保留记录，用户稍后作答时经 userQuestions.answer() 送回。
        this._continue(id, err);
        this.logLine(`question #${id} continued (前台超时，仍可作答)`);
      } else {
        this._fail(id, STATE.CANCELLED, err);
      }
    };
    record.signal = request.signal;
    record.onAbort = onAbort;
    request.signal?.addEventListener?.('abort', onAbort, { once: true });
    // 已经 abort 过（注册前就发生）→ 不要再推送卡片
    if (request.signal?.aborted) {
      onAbort();
      return record.promise;
    }

    // 🔴 卡片推送**不能 await**：若在这里 `await send`，waterfall 监听器本身就被
    // 卡在发送上，abort/取消只能等 send 返回后才被处理 —— 发送一旦挂起就是永久等待。
    // 改为「发起发送、立即返回 record.promise」，让 abort 能即时生效。
    const card = this.renderCard(record);
    void this.send({ platform: binding.platform, chatId: binding.chatId }, card)
      .catch((err) => {
        // 推送失败 → fail closed，不阻塞 agent
        this.logLine(`question ${id} push failed: ${err?.message ?? err}`);
        this._settle(id, STATE.UNAVAILABLE, { answers: [] });
      });

    if (this.cfg.timeoutSec > 0) {
      record.timer = setTimeout(() => {
        if (record.state !== STATE.WAITING) return;
        record.state = STATE.TIMEOUT;
        this.records.delete(id);
        void this.send(
          { platform: binding.platform, chatId: binding.chatId },
          { text: `⏰ 提问 #${id} 已超时（${this.cfg.timeoutSec}s），agent 将继续。` },
        ).catch(() => {});
        this._cleanup(record);
        record.resolve({ answers: [] });
      }, this.cfg.timeoutSec * 1000);
      record.timer.unref?.();
    }

    return record.promise;
  }

  _nextId() {
    // 短、可读、可手打（钉钉里要用户回复编号）。
    // 🔴 不能只用 `% 999` 回绕：回绕会复用**仍在等待**的 id，`records.set` 覆盖旧记录，
    // 旧 promise 永不结算，而旧卡片的按钮/文字命令会答到新问题上（串答）；旧 abort
    // 监听也会误取消新问题。改为单调递增，并在 id 被占用时跳过。
    for (let i = 0; i < 1000; i++) {
      this._seq += 1;
      const id = String(this._seq).padStart(2, '0');
      if (!this.records.has(id)) return id;
    }
    // 极端：1000 个并发等待。退化为带时间戳的唯一 id（仍可复制，只是长一些）。
    return `${this._seq + 1}x${Date.now().toString(36).slice(-4)}`;
  }

  _createRecord(id, request, binding, questions) {
    let resolve;
    let reject;
    const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
    // 未处理的 rejection 不应让进程崩溃：调用方（waterfall 监听）一定会 await，
    // 但 dispose 等路径可能不再 await，这里兜一个空 catch。
    promise.catch(() => {});
    return {
      id,
      callId: request.wait?.callId ?? request.callId,
      // 保留 agent：DSH 继续协议 `userQuestions.answer(agent, callId, answers)` 需要它
      agent: request.agent,
      binding,
      questions,
      state: STATE.WAITING,
      /** 逐题草稿：qIndex → 已选标签[]。按钮应答按题累计，避免"点一题就结题"。 */
      draft: new Map(),
      resolve: (v) => {
        if (this.records.has(id)) this.records.delete(id);
        resolve(v);
      },
      reject: (e) => {
        if (this.records.has(id)) this.records.delete(id);
        reject(e);
      },
      promise,
    };
  }

  /** 渲染提问卡片：问题 + 编号选项 + 按钮（按钮 id 复用 handleCallback 的中性载荷）。 */
  renderCard(record) {
    const lines = [`❓ **需要你的确认**  #${record.id}`];
    const buttons = [];

    record.questions.forEach((q, qi) => {
      if (record.questions.length > 1) lines.push('', `**${qi + 1}. ${q.question}**`);
      else lines.push('', q.question);
      if (q.header) lines.push(`_${q.header}_`);
      if (q.detail) lines.push(q.detail);
      if (q.multiSelect) lines.push('（可多选）');

      const opts = q.options ?? [];
      opts.forEach((o, oi) => {
        const n = oi + 1;
        // 编号按问题内序号；多问题时前缀问题序号，便于 /answer 手打
        const label = record.questions.length > 1 ? `${qi + 1}.${n}` : String(n);
        lines.push(`  ${label}. ${o.label}${o.description ? ` — ${o.description}` : ''}`);
        // 按钮载荷：q:<questionId>:<questionIndex>:<optionIndex>（适配器原样回传）
        // command 供渲染不了按钮的渠道（钉钉 markdown）降级为可直接发送的文字命令。
        buttons.push({
          id: `q:${record.id}:${qi}:${oi}`,
          label: `${label}. ${o.label}`.slice(0, 20),
          style: oi === 0 ? 'primary' : 'default',
          command: `/answer ${record.id} ${label}`,
        });
      });
      if (opts.length === 0) lines.push('  （无预设选项，请直接回复文字）');
    });

    lines.push(
      '',
      `回复方式：**直接回数字**即可（如 \`2\`；多选逗号分隔，如 \`1,3\`）`
      + `，也可发 \`/answer ${record.id} 2\`。`,
      `不想选：回 \`skip\`（或 \`/answer ${record.id} skip\`），agent 会自行决定后继续。`,
      record.questions.length > 1
        ? `按钮可逐题点选：答全会自动提交；也可发 \`/answer ${record.id} done\` 提前提交（未答的题按跳过）。`
        : '',
    );

    return { text: lines.join('\n'), buttons };
  }

  /**
   * 跳过提问：不选任何选项，让 agent 自行决定。
   *
   * 语义上等价于「用户未作答」——返回每问 `selected: []`。ask_user_question 的契约是
   * 「空选择 = 没拿到用户意见」，模型据此自行判断或改用默认方案继续，不会卡住。
   * （对比超时：超时也返回空答案，但用户明确 skip 更即时、意图更清楚。）
   * @param {string} questionId
   * @param {{platform?:string, chatId?:string}} [from] 应答来源；给出则校验归属
   * @returns {'skipped'|'not-found'|'forbidden'}
   */
  skip(questionId, from) {
    const rec = this.records.get(questionId);
    if (!rec) {
      // 已挂起可续答：skip 同样经继续协议送回空答案
      const cont = this.continued.get(questionId);
      if (!cont) return 'not-found';
      if (from && !this._owns(cont, from)) return 'forbidden';
      const answers = cont.questions.map((q) => ({ id: q.id, selected: [] }));
      const delivered = this.deliverContinued(cont, answers);
      if (delivered === 'delivered') { this.continued.delete(questionId); return 'continued'; }
      return delivered === 'unavailable' ? 'no-continuation' : 'invalid';
    }
    if (rec.state !== STATE.WAITING) return 'not-found';
    if (from && !this._owns(rec, from)) return 'forbidden';
    this._settle(
      questionId,
      STATE.ANSWERED,
      { answers: rec.questions.map((q) => ({ id: q.id, selected: [] })) },
      { skipped: true },
    );
    return 'skipped';
  }

  /**
   * 用户应答（按钮回调 / 文字命令）。
   * @param {string} questionId
   * @param {Array<{qIndex:number, oIndex:number}>} picks
   * @param {string} [custom] 自定义文字
   * @param {{platform?:string, chatId?:string}} [from] 应答来源；给出则校验归属
   * @param {{allowPartial?:boolean}} [opts] `allowPartial`：允许只答一部分
   *        （**按钮路径**必须开：一次点击只携带一个 (q,o)，多问题卡片否则永远
   *        `invalid`、用户无法完成作答 —— round-2 发现 2）；文字命令路径则应答全，
   *        以便用户能明确表达"哪些题我不选"。
   * @returns {'answered'|'not-found'|'invalid'|'forbidden'}
   */
  respond(questionId, picks, custom, from, opts = {}) {
    const rec = this.records.get(questionId);
    if (!rec) {
      // 已挂起但可续答（timed 前台超时后）：转为经 DSH 继续协议送回
      const cont = this.continued.get(questionId);
      if (!cont) return 'not-found';
      if (from && !this._owns(cont, from)) return 'forbidden';
      const built = this._buildAnswers(cont, picks, custom, opts);
      if (!built.ok) return 'invalid';
      const delivered = this.deliverContinued(cont, built.answers);
      if (delivered === 'delivered') {
        this.continued.delete(questionId);
        return 'continued';
      }
      return delivered === 'unavailable' ? 'no-continuation' : 'invalid';
    }
    if (rec.state !== STATE.WAITING) return 'not-found';
    // 归属校验：只有提问所在会话才能作答（否则另一个聊天里的授权用户可跨会话代答）
    if (from && !this._owns(rec, from)) return 'forbidden';
    const built = this._buildAnswers(rec, picks, custom, opts);
    if (!built.ok) return 'invalid';
    const unanswered = built.answers.filter((a) => a.selected.length === 0).length;

    this._settle(questionId, STATE.ANSWERED, { answers: built.answers }, {
      picks: picks.length, custom: !!custom, ...(unanswered ? { unanswered } : {}),
    });
    return 'answered';
  }

  /**
   * 按钮应答：把本次选择**累计**进草稿，而不是立刻结题。
   *
   * 为什么必须累计（round-3 发现）：一次按钮点击只携带一个 (q,o)，若立刻结题，
   * 多问题卡片点第一题就会把其余题当成"空答案"提交，且余下按钮全部失效。
   * 累计后：答全 → 自动提交；未答全 → 保持等待并回报进度，用户可继续点，
   * 或发 `/answer <id> done` 提前提交（未答的题按跳过处理）。
   *
   * @returns {{status:string, answered?:number, total?:number, label?:string}}
   */
  accumulate(questionId, picks, from) {
    const rec = this.records.get(questionId);
    if (!rec || rec.state !== STATE.WAITING) return { status: 'not-found' };
    if (from && !this._owns(rec, from)) return { status: 'forbidden' };

    for (const p of picks) {
      if (!Number.isInteger(p.qIndex) || p.qIndex < 0 || p.qIndex >= rec.questions.length) {
        return { status: 'invalid' };
      }
      const opts = rec.questions[p.qIndex].options ?? [];
      if (!Number.isInteger(p.oIndex) || p.oIndex < 0 || p.oIndex >= opts.length) {
        return { status: 'invalid' };
      }
      if (!rec.questions[p.qIndex].multiSelect) rec.draft.set(p.qIndex, []);
      if (!rec.draft.has(p.qIndex)) rec.draft.set(p.qIndex, []);
      const label = opts[p.oIndex].label;
      if (!rec.draft.get(p.qIndex).includes(label)) rec.draft.get(p.qIndex).push(label);
    }

    const withOptions = rec.questions
      .map((q, qi) => ((q.options ?? []).length ? qi : -1))
      .filter((qi) => qi >= 0);
    const answered = withOptions.filter((qi) => rec.draft.has(qi)).length;

    // 🔴 含**自由文本题**（无 options）时不能由按钮自动结题：
    // 那种题按钮根本无法作答，自动结题会把它的答案伪造为空
    // （round-4 发现 1，独立复验：混合题型只点有选项那题即 submitted，自由文本题 selected:[]）。
    // 此时必须由用户显式 `/answer <id> done` 或直接回复文字。
    const allHaveOptions = withOptions.length === rec.questions.length;
    const lastQ = picks.length ? picks[picks.length - 1].qIndex : 0;
    const label = rec.draft.get(lastQ)?.join('、') ?? '';

    if (allHaveOptions && answered >= withOptions.length) {
      const answers = rec.questions.map((q, qi) => ({ id: q.id, selected: rec.draft.get(qi) ?? [] }));
      this._settle(questionId, STATE.ANSWERED, { answers }, { accumulated: true });
      return { status: 'answered', answered, total: withOptions.length, label };
    }
    // 未答全：保持等待，回报进度
    this.logLine(`question #${questionId} partial ${answered}/${withOptions.length} (${label})`);
    return {
      status: 'partial',
      answered,
      total: withOptions.length,
      label,
      // 含自由文本题：告诉用户按钮无法完成，需要显式提交或打字
      needsText: !allHaveOptions,
    };
  }

  /**
   * 提交草稿（`/answer <id> done`）：未答的题按跳过（空选择）处理。
   * @returns {'answered'|'not-found'|'forbidden'|'empty'}
   */
  commitDraft(questionId, from) {
    const rec = this.records.get(questionId);
    if (!rec || rec.state !== STATE.WAITING) return 'not-found';
    if (from && !this._owns(rec, from)) return 'forbidden';
    if (rec.draft.size === 0) return 'empty';
    const answers = rec.questions.map((q, qi) => ({ id: q.id, selected: rec.draft.get(qi) ?? [] }));
    this._settle(questionId, STATE.ANSWERED, { answers }, { committed: true });
    return 'answered';
  }

  /**
   * 校验并组装答案（不结算）。越界编号**不得**静默丢弃后照样结题——那会把
   * 「答错了」变成「已作答但空选择」，用户以为选上了、agent 却什么都没拿到。
   * @returns {{ok:true, answers:Array}|{ok:false, reason:string}}
   */
  _buildAnswers(rec, picks, custom, opts = {}) {
    // 无选项的问题：允许纯文字作答（见 renderCard 的「请直接回复文字」提示）
    const noOptions = rec.questions.every((q) => !(q.options ?? []).length);
    if (picks.length === 0 && !(custom && noOptions)) return { ok: false, reason: 'no-picks' };

    const byQ = new Map();
    for (const p of picks) {
      if (!Number.isInteger(p.qIndex) || p.qIndex < 0 || p.qIndex >= rec.questions.length) {
        return { ok: false, reason: 'qIndex-out-of-range' };
      }
      const opts_ = rec.questions[p.qIndex].options ?? [];
      if (!Number.isInteger(p.oIndex) || p.oIndex < 0 || p.oIndex >= opts_.length) {
        return { ok: false, reason: 'oIndex-out-of-range' };
      }
      if (!byQ.has(p.qIndex)) byQ.set(p.qIndex, []);
      const label = opts_[p.oIndex].label;
      if (!byQ.get(p.qIndex).includes(label)) byQ.get(p.qIndex).push(label);
    }
    // 单问题必须给出一个选择；多问题在文字路径下要求答全，按钮路径允许部分
    if (!opts.allowPartial) {
      for (let qi = 0; qi < rec.questions.length; qi++) {
        if ((rec.questions[qi].options ?? []).length && !byQ.has(qi)) {
          return { ok: false, reason: 'unanswered' };
        }
      }
    }
    for (const [qi, labels] of byQ) {
      if (!rec.questions[qi].multiSelect && labels.length > 1) {
        return { ok: false, reason: 'multi-select-on-single' };
      }
    }
    return {
      ok: true,
      answers: rec.questions.map((q, qi) => {
        const item = { id: q.id, selected: byQ.get(qi) ?? [] };
        if (custom && qi === 0) item.custom = custom;
        return item;
      }),
    };
  }

  /** 该记录是否属于给定会话。 */
  _owns(rec, { platform, chatId }) {
    return rec.binding?.platform === platform && rec.binding?.chatId === chatId;
  }

  /**
   * timed 前台等待结束：reject 前台 promise（让 DSH 得到 `{pending:true}` 并放行 agent），
   * 但**记录转入 continued**，保留 callId/agent 供迟答转发。
   */
  _continue(id, error) {
    const rec = this.records.get(id);
    if (!rec || rec.state !== STATE.WAITING) return;
    rec.state = STATE.CONTINUED;
    this._cleanup(rec);
    this.records.delete(id);
    rec.reject(error);
    this.continued.set(id, rec);
    // 防无限增长：只保留最近 50 条
    if (this.continued.size > 50) {
      const oldest = this.continued.keys().next().value;
      this.continued.delete(oldest);
    }
  }

  /** 在「等待中」与「可续答」两处查找记录。 */
  findRecord(id) {
    return this.records.get(id) ?? this.continued.get(id) ?? null;
  }

  /**
   * 把迟答转交给 DSH 的继续协议（`userQuestions.answer`）。
   * @returns {'delivered'|'unavailable'|'error'}
   */
  deliverContinued(rec, answers) {
    const svc = this.ctx.get?.('userQuestions');
    if (!svc || typeof svc.answer !== 'function' || !rec.callId || !rec.agent) return 'unavailable';
    try {
      const ok = svc.answer(rec.agent, rec.callId, { answers });
      return ok ? 'delivered' : 'error';
    } catch (err) {
      this.logLine(`continued answer failed: ${err?.message ?? err}`);
      return 'error';
    }
  }

  _settle(id, state, value, extra) {
    const rec = this.records.get(id);
    if (!rec || rec.state !== STATE.WAITING) return;
    rec.state = state;
    this._cleanup(rec);
    this.records.delete(id);
    this.logLine(
      `question #${id} ${state}${extra ? ' ' + JSON.stringify(extra) : ''}`,
    );
    rec.resolve(value);
  }

  /**
   * 以**异常**结束（用于 abort：DSH 依赖异常区分「超时挂起/取消」与「用户真的答了空」）。
   * @param {string} id
   * @param {string} state
   * @param {Error} error
   */
  _fail(id, state, error) {
    const rec = this.records.get(id);
    if (!rec || rec.state !== STATE.WAITING) return;
    rec.state = state;
    this._cleanup(rec);
    this.records.delete(id);
    this.logLine(`question #${id} ${state} (${error.code ?? 'error'})`);
    rec.reject(error);
  }

  /** 该会话是否有待答提问（有则返回最近一个），供「直接回数字即作答」用。 */
  pendingFor(platform, chatId) {
    let found = null;
    for (const rec of this.records.values()) {
      if (rec.state !== STATE.WAITING) continue;
      if (rec.binding?.platform === platform && rec.binding?.chatId === chatId) found = rec;
    }
    return found;
  }

  /** 待回答的提问（/status 用）。 */
  pendingList() {
    return [...this.records.values()].map((r) => ({
      id: r.id,
      count: r.questions.length,
      ageSec: Math.round((Date.now() - (r.createdAt ?? Date.now())) / 1000),
    }));
  }
}