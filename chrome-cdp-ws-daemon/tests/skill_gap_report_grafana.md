# CDP Skill 缺失点分析 — Grafana Loki 日志抓取场景

生成时间：2026-05-17  
场景：复用 Chrome 登录态，从 Grafana Explore 页面提取 Loki 日志

---

## 整体流程回顾

```
1. active-page      → 确认页面 URL / target_id
2. cookies get      → 获取登录 cookie（SSO_USER_TOKEN 等）
3. network-capture start → 开始抓包
4. snapshot -i      → 找到 "Run query" 按钮 @e17
5. click @e17       → 触发查询，Grafana 发出 POST /api/ds/query
6. network-capture stop → 停止，拿到完整请求（含 responseBody）
7. 分析 postData + headers → 提炼 x-grafana-device-id / x-datasource-uid 等非标 header
8. 写独立脚本 grafana_loki_logs.py → 验证成功，获取到 197 条日志
```

---

## 发现的缺失点（本次新增）

### #12 network-capture 响应 body 截断问题
**现象**：`responseBody` 字段被 daemon 截断为约 192KB，实际 Grafana 日志响应约 170KB JSON 没问题，但如果日志量大（>500 条）会截断。  
**根因**：`page_manager.py` 中 `Network.getResponseBody` 回来的 body 存入内存无大小控制；`_send` 的 `MAX_RECV_SIZE` 限制了 socket 传输大小。  
**建议**：
- `network-capture stop` 的 responseBody 超过阈值（如 500KB）时自动写到临时文件，返回 `{"bodyFile": "/tmp/xxx.bin"}` 路径引用
- 或支持 `--max-body-size` 参数截断（当前行为无提示地静默截断）

### #13 `network-capture stop` 耗时无进度显示
**现象**：本次查询响应 170KB，`stop` 等了约 8s，CLI 无任何输出，用户不知道是否卡死。  
**建议**：`stop` 期间每 2s 打印一个 `"."` 进度点到 stderr，或输出 `"fetching body for N requests..."` 提示。

### #14 `cookies get` 返回的 cookie 中缺少"非 cookie"认证信息提示
**现象**：本次 Grafana 请求需要 `x-grafana-device-id`（固定值，存在 localStorage）、`x-grafana-org-id`（来自 URL 参数），这些**不是 cookie**，`cookies get` 拿不到。  
**根因**：skill 只能通过 `Browser.getCookies` 获取 cookie，localStorage / sessionStorage / URL 参数不在 cookie 范围内。  
**建议**：增加 `eval-js 'localStorage.getItem("grafanaUserPreferences")'` 的使用提示；或提供 `local-storage get <key>` CLI 命令。  

### #15 没有"从 URL 自动提取关键参数"能力
**现象**：Grafana 的 datasource UID、org ID 等都编码在 URL 的 `panes` JSON 参数中，需要手动 decode URL + parse JSON 才能拿到。整个过程纯靠人肉，无 skill 辅助。  
**建议**：增加 `get-url --json-decode` 参数，或 `eval-js 'new URL(location.href).searchParams.get("panes")'`（后者已通过 eval-js 可实现，但无文档示例）。

### #16 `network-capture export --python-client` 缺少 HTTPS 证书验证跳过处理
**现象**：Grafana 用自签或内部 CA 证书，直接用 `requests.get()` 会报 SSLError。生成的 client 代码没有 `verify=False` 或自定义 CA。  
**建议**：`_export_python_client` 生成代码时，对内网域名自动加 `session.verify = False`（并加 `urllib3.disable_warnings()` 注释）。

### #17 `network fetch` 在页面上下文执行，无法跳过 CORS，但也无法设置自定义 header（已修复 #9）
**现象**：本次 Grafana API 不存在 CORS 问题（同域），但 `network fetch` 如果用于跨域场景，仍可能被浏览器 CORS 拦截。  
**根因**：浏览器的 `fetch()` 受 CORS 控制；对比 `requests.Session` 在 Python 中不受 CORS 限制。  
**建议**：文档中明确说明"network fetch 适合同域 API；跨域场景请用 cookies get + Python requests"，避免用户踩坑。

### #18 responseBody 对 base64 编码内容无自动解码
**现象**：`base64Encoded: false` 时正常，若是压缩（gzip）响应，body 会以 base64 返回，当前代码没有自动 `base64.b64decode`。  
**建议**：在 `network_capture_stop` 的 body 处理中判断 `base64Encoded`，自动解码并尝试 gzip decompress。

---

## 本次流程中 Skill 表现良好的部分

| 步骤 | 结果 |
|------|------|
| `active-page` 确认活跃页 | ✓ 一次成功，URL 精确 |
| `cookies get --json` | ✓ 9 个 cookie，直接可用 |
| `network-capture start` | ✓ 即时响应 |
| `snapshot -i` 找到 Run query 按钮 | ✓ @e17 精确定位 |
| `click @e17` 触发查询 | ✓ 成功点击，抓到 3 个 API 请求 |
| `network-capture stop`（带 body）| ✓ 完整 responseBody（170KB 内正常） |
| 非标 header 保留（x-grafana-*） | ✓ 上次修复 #11 生效 |
| 独立脚本验证 | ✓ 197 条日志，首次运行即成功 |

---

## 总结

本次流程**无需多次尝试**，一次性成功（从抓包到脚本可运行共约 15 分钟）。主要瓶颈在于：

1. **body 截断无提示**（#12）—— 如果日志量更大会悄悄丢数据
2. **cookie 之外的认证信息需要人肉分析**（#14）—— x-grafana-device-id 来自 localStorage，无法通过 cookies get 获取
3. **生成代码的 SSL 问题**（#16）—— 内网证书场景下生成代码跑不起来

建议优先实现 #12（body 截断安全处理）和 #16（SSL 处理）。
