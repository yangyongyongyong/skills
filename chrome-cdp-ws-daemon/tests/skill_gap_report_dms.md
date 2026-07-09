# CDP Skill 缺失点报告 —— DMS MySQL/StarRocks 查询平台场景

**页面**：`https://starrocks-dms-cn.tuya-inc.com:7799/#/MySQL/sqlQuery`  
**日期**：2026-05-17  
**场景**：通过 CDP 读取 Ace Editor SQL、调用后台 API 实现不依赖浏览器页面的静默查询

---

## 缺失点 #19：editor-get/set 不支持 Ace Editor

**现象**：DMS 编辑器类型为 Ace Editor（`.ace_editor`），`editor-get` 报 "No editor found"  
**原因**：原实现只探测 Monaco textarea、CodeMirror 5/6，未处理 Ace Editor  
**影响**：无法读取或写入 DMS、许多旧版 SQL 平台的编辑器内容  

**修复**：
- `editor_get()`：新增 Ace 探测路径，优先 `aceEl.env.editor`，回退 `ace.edit(aceEl)`
- `editor_set()`：对 Ace 优先走 JS API（`editor.setValue()` / `editor.insert()`），避免 `insertText` 不触发 Ace onChange 的问题
- 返回的 `type` 字段新增 `"ace"` 值，`language` 字段填入 `session.$modeId`

**已修复文件**：`scripts/page_manager.py`（`editor_get` / `editor_set`），`SKILL.md`

---

## 缺失点 #20：stop-daemon 命令缺失，导致绕路 kill 误杀 Chrome

**现象**：需要重启 daemon 时，只能手动 `kill -9 $(cat cdp.pid)` 或 `lsof -ti :9222 | xargs kill -9`，后者直接把 Chrome 进程杀掉  
**原因**：daemon CLI 没有提供 `stop-daemon` / `restart-daemon` 命令  
**影响**：重启 daemon 操作危险，`lsof -ti :9222` 会匹配到 Chrome 本身

**建议修复**：
```bash
# 新增命令（daemon.py dispatch 入口）
$DAEMON $SCRIPT stop-daemon    # 只杀 daemon 进程（读 cdp.pid），不触碰 Chrome
$DAEMON $SCRIPT restart-daemon # 先 stop 再 start --daemon
```

**状态**：待实现

---

## 缺失点 #21：Python 直接请求被 SSO 拦截，cookies get --json 误导用户

**现象**：`dms_mysql_query.py` 初版只传 `Authorization: TUYA <JWT>` header，被服务端返回 SSO 登录 HTML  
**原因**：服务端对直接 HTTP 请求验证 `SSO_USER_TOKEN` / `COOKICE_USER_TOKEN_PC`（httpOnly cookie），但 `--json` 格式实际已包含这两个 cookie，脚本没有把 cookie 一起发出去  
**影响**：用户误以为 JWT 就足够，实际还需要完整 cookie 字符串

**修复**：`dms_mysql_query.py` 改为同时把全部 cookie 拼成 `Cookie: ...` header 一起发送  
**结论**：`cookies get --json` 已能返回 httpOnly cookie，无需改 skill，属于使用姿势问题  
**建议**：SKILL.md 补充说明"Python 脚本直接调 API 时，需把 `cookies get --json` 拿到的全部 cookie 拼成 Cookie header，包括 httpOnly 的 SSO token"

**状态**：`dms_mysql_query.py` 已修复并验证通过（`SELECT 1+1` 返回 2，真实表查询 3 行正常）

---

## 场景产物

| 文件 | 说明 |
|------|------|
| `tests/dms_mysql_query.py` | DMS MySQL/StarRocks 静默查询脚本（含 cookie 修复） |
| `scripts/page_manager.py` | editor_get/editor_set 新增 Ace Editor 支持 |
| `SKILL.md` | editor 章节更新为 Monaco/CM/Ace/textarea |

---

## 本次场景总结

| 目标 | 状态 |
|------|------|
| 读取 Ace Editor 内容 | ✅ `editor-get` 修复后正常 |
| 写入 Ace Editor 内容 | ✅ `editor-set` JS API 路径 |
| 静默调用 DMS API | ✅ Cookie + JWT 双 header 绕过 SSO |
| 发现并记录新缺失点 | ✅ #19 已修复，#20/#21 已记录 |
