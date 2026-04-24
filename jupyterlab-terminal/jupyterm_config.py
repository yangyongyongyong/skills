"""
jupyterm_config — 配置读写模块。
"""

import json
import os
import sys

CONFIG_FILE = os.path.expanduser("~/.jupyterm.json")


def load_config() -> dict:
    """读取保存的 JupyterLab 连接配置。

    @return 配置字典（含 base_url / ws_base / token）
    """
    if not os.path.exists(CONFIG_FILE):
        sys.exit(
            f"[jupyterm] 未找到配置 {CONFIG_FILE}，请先运行: jupyterm setup"
        )
    with open(CONFIG_FILE) as f:
        return json.load(f)


def save_config(cfg: dict):
    """保存配置到 ~/.jupyterm.json。

    @param[in] cfg 配置字典
    """
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)
    print(f"[jupyterm] 配置已保存: {CONFIG_FILE}")
    print(json.dumps(cfg, indent=2))
