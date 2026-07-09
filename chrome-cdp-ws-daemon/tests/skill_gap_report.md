# chrome-cdp-ws-daemon Skill 能力缺失报告

> 生成时间：2026-05-17
> 发现场景：对 bdp-ueaz 任务 73590 执行"CDP 页面侦察 → 接口逆向 → 纯 Python 编辑"完整流程

---

## 缺失点汇总（按优先级）

### 🔴 阻塞型（无法直接完成任务，被迫绕道）

| # | 问题 | 复现步骤 | 当前 workaround | 建议新增 |
|---|------|----------|-----------------|---------|
| 3 | `press` 不支持组合键（Ctrl+S / Meta+S） | `daemon.py press "s"` 无法触发保存 | 用 SDK `page_call("Input.dispatchKeyEvent", {modifiers:4, key:"s"})` 手写 | `press "Meta+S"` / `press "Ctrl+S"` 组合键语法 |
| 4 | CLI 无 `eval-js` / `inject-js` 命令 | 需要在页面执行任意 JS 时（如读 meta 标签、触发事件）只能用 SDK | 用 `cdp_client.page_call("Runtime.evaluate", {...})` | `daemon.py eval-js "JS表达式" [--target]` |
| 6 | 无"实时 header 监听"能力 | 要拿页面 XHR 动态注入的 `uid` header，fetch/XHR 拦截都失败 | 用 `network-capture start/stop` 抓包后手工提取 | `daemon.py capture-headers --url-filter "xxx" --wait 5s` 持续监听直到命中 |

---

### 🟠 高成本型（可完成但步骤繁琐）

| # | 问题 | 影响 | 当前步骤数 | 建议 |
|---|------|------|-----------|------|
| 1 | `snapshot` 无文字 button 无法区分功能 | 工具栏 `@e8~@e25` 全是匿名 button，不知道哪个是保存/提交 | 必须额外调 `scan-tooltips`（耗时 ~10s）才能识别 | `snapshot` 输出里自动合并 tooltip/aria-label，或增加 `-T` 参数自动触发 hover 扫描 |
| 2 | `scan-tooltips` 扫不到"保存"（快捷键保存场景） | 保存仅靠 Ctrl+S，没有可见按钮，只能靠组合键 | 无法通过 scan-tooltips 发现 | 增加"快捷键映射扫描"：读页面注册的 keydown listener，输出 `{key: "Meta+S", action: "save"}` |
| 5 | `network-capture export` 生成的 Python 代码不可直接运行 | 导出代码缺少 cookie 注入、uid header、csrf-token 逻辑 | 手动补 3 处认证逻辑 | `export --python-client` 自动调 `cookies get` + 从页面读 csrf-token，生成可直接运行的完整 client |
| 7 | `network-capture stop` 没有 `--body` 时 responseBody 全空 | 第二次抓包忘加 `--body`，50 条请求全部 body=0B | 手动每次都记得加 `--body` | 改为**默认带 body**（或增加 `--no-body` 选项反向），对 API 请求不影响性能 |

---

### 🟡 体验型（效率低但可接受）

| # | 问题 | 影响 | 建议 |
|---|------|------|------|
| 8 | `cookies get` 返回过滤后 cookie，但 `get_all_cookies()` 返回 915 个（全域名）导致 nginx 400 | 脚本里误用 `get_all_cookies()` 直接塞入 session 会触发 `400 Request Header Too Large` | 在 `get_all_cookies()` 文档注释 / SKILL.md 里明确警告：不要直接用于请求，必须先用 `get_cookies(url)` 过滤 |
| 9 | `network fetch` CLI 不支持 `--headers` 参数 | 无法附加自定义 header（如 TUYA-WKID），调试时需要用 SDK 绕道 | 增加 `--headers '{"key":"val"}'` 参数 |
| 10 | 无"接口候选过滤"能力 | 50 条抓包请求需人工扫描找核心 PUT 保存接口（夹在大量 `biz/catalogue` 轮询噪音中） | 增加 `network-capture filter --method PUT,POST --exclude-domains sentry,static` 快速定位业务接口 |
| 11 | `network-capture export --python-client` 未包含 `uid` header 逻辑 | 导出代码里 uid 字段注释掉了，实际请求需要 uid 才能通过鉴权 | 从抓包的请求 header 里自动提取所有非浏览器标准 header，统一放入导出代码 |

---

## 发现路径（按顺序）

```
缺失#1 ← snapshot 返回 @e8~@e25 匿名 button（需要 scan-tooltips 补充）
缺失#2 ← scan-tooltips 找不到"保存"（保存只有 Ctrl+S，没有 tooltip 按钮）
缺失#3 ← press 不支持组合键，无法触发 Meta+S
缺失#4 ← CLI 没有 eval-js，只能改用 SDK page_call 注入 JS
缺失#5 ← network-capture export 生成代码缺少 cookie + uid + csrf 注入
缺失#6 ← 无法在不主动操作的情况下被动捕获页面 XHR/fetch 的动态注入 header（uid 来源）
缺失#7 ← 第二次 network-capture stop 忘加 --body，全部响应 body=0B
缺失#8 ← get_all_cookies() vs get_cookies(url) 的使用风险（全域名 915 个 cookie → nginx 400）
缺失#9 ← network fetch CLI 无 --headers 参数
缺失#10 ← 无请求噪音过滤，50条中需人工找2条有效接口
缺失#11 ← 导出代码 uid header 丢失
```

---

## 最高价值改进项（建议优先落地）

1. **`press "Meta+S"`** — 组合键支持，影响所有依赖快捷键的 Web 应用
2. **`eval-js` CLI 命令** — 通用 JS 执行能力，覆盖大量页面状态读取场景
3. **`network-capture stop` 默认带 body** — 减少用户失误，body 默认开启
4. **`network-capture export` 自动注入认证** — 导出即可运行的完整 client 代码
5. **`capture-headers` 实时监听命令** — 解决动态注入 header 的获取难题

---

## 交付物

- 纯 Python 编辑脚本：`tests/edit_task_code.py`
- 任务备份：`tests/backup/73590_*.json`（含修改前备份）
- 本报告：`tests/skill_gap_report.md`
