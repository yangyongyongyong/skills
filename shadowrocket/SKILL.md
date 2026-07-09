---
name: shadowrocket
description: >-
  通过 URL Scheme 控制本机 macOS Shadowrocket（小火箭）VPN 开关与全局路由；
  用 scutil 查询连接状态。用户提到 Shadowrocket、小火箭、代理开关、VPN 开/关/切换时使用。
---

# Shadowrocket 开关

## 何时使用

- 用户要求开启 / 关闭 / 切换 Shadowrocket（小火箭）VPN
- 查询当前是否已连接
- 切换全局路由：代理 / 配置 / 直连 / 场景

## 前提

- macOS 已安装 `/Applications/Shadowrocket.app`
- 首次使用 VPN 需用户在系统里授权过网络扩展

## 标准调用（绝对路径）

```bash
python3 /Users/thomas990p/.cursor/skills/shadowrocket/scripts/shadowrocket.py status
python3 /Users/thomas990p/.cursor/skills/shadowrocket/scripts/shadowrocket.py on
python3 /Users/thomas990p/.cursor/skills/shadowrocket/scripts/shadowrocket.py off
python3 /Users/thomas990p/.cursor/skills/shadowrocket/scripts/shadowrocket.py toggle
python3 /Users/thomas990p/.cursor/skills/shadowrocket/scripts/shadowrocket.py route proxy
```

加 `--json` 可拿结构化结果。

## 命令说明

| 命令 | 行为 |
|------|------|
| `status` | 读 `scutil --nc list`，解析 `com.liguangming.Shadowrocket` |
| `on` | 已连接则跳过；否则 `shadowrocket://connect?autoclose=true` 并等待 Connected |
| `off` | 已断开则跳过；否则 `shadowrocket://disconnect?autoclose=true` 并等待 Disconnected |
| `toggle` | `shadowrocket://toggle?autoclose=true`，再核对状态是否翻转 |
| `route <mode>` | `mode` ∈ `proxy` / `config` / `direct` / `scene` |

`on` / `off` 可用 `--force` 强制重发 URL；`--timeout` 控制等待秒数（默认 8）。

## Agent 行为约定

1. 用户说「开 / 关 / 切换」时，直接跑对应 CLI，不要只口述 `open` 命令。
2. 操作后用 CLI 输出确认最终 `state`（connected / disconnected）。
3. 不要用 UI 自动化点开关；优先 URL Scheme。
4. 本 skill 只管 Shadowrocket；ClashX / Surge 等不在范围。

## 底层 URL Scheme（参考）

```text
shadowrocket://connect?autoclose=true
shadowrocket://disconnect?autoclose=true
shadowrocket://toggle?autoclose=true
shadowrocket://route/proxy|config|direct|scene?autoclose=true
```
