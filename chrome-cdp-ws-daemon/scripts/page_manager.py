"""页面级高级操作管理器。

职责：
  - Page Session 管理（targetId <-> sessionId，flatten 模式）
  - Target 解析（"active" / targetId / "url:keyword"）
  - 元素引用缓存（@e1, @e2, ... 带 TTL）
  - 高级动作执行（snapshot / click / fill / select / wait / get_text / press）

依赖 CdpConnection 提供底层 WebSocket 通信，但不直接操作 WS。
本模块所有方法均线程安全。
"""

from __future__ import annotations

import json
import os
import base64
import re
import struct
import subprocess
import sys
import threading
import time
import tempfile
import zlib
import urllib.parse
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    # 避免循环导入，仅用于类型提示
    pass

# ---------------------------------------------------------------------------
# JS 代码片段（独立常量，方便单独维护）
# ---------------------------------------------------------------------------

JS_SNAPSHOT = r"""
(function(scope, includeCursor) {
    const root = scope ? document.querySelector(scope) : document;
    if (!root) return JSON.stringify({error: 'scope not found: ' + scope});
    const interactiveSelectors = [
        'a[href]', 'button', 'input', 'textarea', 'select',
        '[role="button"]', '[role="link"]', '[role="tab"]',
        '[role="menuitem"]', '[role="checkbox"]', '[role="radio"]',
        '[role="switch"]', '[role="combobox"]', '[role="searchbox"]',
        '[role="textbox"]', '[contenteditable="true"]',
        '[tabindex]:not([tabindex="-1"])',
    ];
    if (includeCursor) {
        interactiveSelectors.push('[onclick]', '[data-action]');
    }
    const seen = new Set();
    const elements = [];

    /* 生成唯一 CSS 选择器（多级回退） */
    function buildSelector(el) {
        const tag = el.tagName.toLowerCase();

        // 1. id（最可靠）
        if (el.id) {
            const idSel = '#' + CSS.escape(el.id);
            if (document.querySelectorAll(idSel).length === 1) return idSel;
        }

        // 2. data-testid / data-cy / data-test
        for (const attr of ['data-testid', 'data-cy', 'data-test']) {
            const v = el.getAttribute(attr);
            if (v) return tag + '[' + attr + '=' + JSON.stringify(v) + ']';
        }

        // 3. name 属性
        const name = el.getAttribute('name');
        if (name) return tag + '[name=' + JSON.stringify(name) + ']';

        // 4. aria-label
        const ariaLabel = el.getAttribute('aria-label');
        if (ariaLabel) return tag + '[aria-label=' + JSON.stringify(ariaLabel) + ']';

        // 5. placeholder
        const placeholder = el.getAttribute('placeholder');
        if (placeholder) return tag + '[placeholder=' + JSON.stringify(placeholder) + ']';

        // 6. <a> 带 href：用 href 构建（去掉域名前缀，用 $= 或 *= 匹配）
        if (tag === 'a') {
            const href = el.getAttribute('href');
            if (href && href !== '#' && href !== 'javascript:void(0)') {
                // 优先精确匹配（相对路径或短路径）
                if (href.startsWith('/') && href.length < 200) {
                    const sel = 'a[href=' + JSON.stringify(href) + ']';
                    if (document.querySelectorAll(sel).length === 1) return sel;
                }
                // 回退到 contains 匹配
                const path = href.replace(/^https?:\/\/[^\/]+/, '');
                if (path && path.length < 150) {
                    const sel = 'a[href*=' + JSON.stringify(path) + ']';
                    if (document.querySelectorAll(sel).length === 1) return sel;
                }
            }
        }

        // 7. data-component / data-variant 组合（GitHub Primer 等框架）
        const dc = el.getAttribute('data-component');
        const dv = el.getAttribute('data-variant');
        if (dc) {
            let sel = tag + '[data-component=' + JSON.stringify(dc) + ']';
            if (dv) sel += '[data-variant=' + JSON.stringify(dv) + ']';
            if (document.querySelectorAll(sel).length === 1) return sel;
        }

        // 8. role 属性
        const role = el.getAttribute('role');
        if (role) {
            const sel = tag + '[role=' + JSON.stringify(role) + ']';
            if (document.querySelectorAll(sel).length === 1) return sel;
        }

        // 9. type 属性（button/input）
        const type = el.getAttribute('type');
        if (type) {
            const sel = tag + '[type=' + JSON.stringify(type) + ']';
            if (document.querySelectorAll(sel).length === 1) return sel;
        }

        // 10. nth-of-type 路径（比 nth-child 更稳定）
        function stablePath(node) {
            if (!node.parentElement) return tag;
            const siblings = Array.from(node.parentElement.children).filter(
                c => c.tagName === node.tagName
            );
            const t = node.tagName.toLowerCase();
            if (siblings.length === 1) return t;
            const idx = siblings.indexOf(node) + 1;
            return t + ':nth-of-type(' + idx + ')';
        }
        const parts = [];
        let cur = el;
        for (let i = 0; i < 4 && cur && cur !== document.body; i++) {
            // 如果父级有 id，到此为止
            if (cur.parentElement && cur.parentElement.id) {
                parts.unshift(stablePath(cur));
                parts.unshift('#' + CSS.escape(cur.parentElement.id));
                break;
            }
            parts.unshift(stablePath(cur));
            cur = cur.parentElement;
        }
        return parts.join(' > ');
    }

    for (const sel of interactiveSelectors) {
        try {
            root.querySelectorAll(sel).forEach(el => {
                if (seen.has(el)) return;
                // 跳过不可见元素
                const rect = el.getBoundingClientRect();
                if (rect.width === 0 && rect.height === 0) return;
                const style = getComputedStyle(el);
                if (style.display === 'none' || style.visibility === 'hidden') return;
                if (parseFloat(style.opacity) === 0) return;
                seen.add(el);

                const tag = el.tagName.toLowerCase();
                const type = el.getAttribute('type') || '';
                const role = el.getAttribute('role') || '';
                const id = el.id || '';
                const name = el.getAttribute('name') || '';
                const placeholder = el.getAttribute('placeholder') || '';
                const ariaLabel = el.getAttribute('aria-label') || '';
                const href = (tag === 'a' ? el.getAttribute('href') : '') || '';
                const text = (el.textContent || '').trim().substring(0, 80);
                const value = el.value !== undefined ? String(el.value).substring(0, 60) : '';
                const checked = el.checked;
                const selector = buildSelector(el);

                // 额外标签来源（#25：匿名 button 补充）
                const titleAttr   = el.getAttribute('title') || '';
                const nzTooltip   = el.getAttribute('nz-tooltip') || el.getAttribute('nzTooltipTitle') || '';
                const dataTestId  = el.getAttribute('data-testid') || el.getAttribute('data-test-id') || '';
                const ariaDesc    = (() => {
                    const by = el.getAttribute('aria-describedby');
                    if (!by) return '';
                    const d = document.getElementById(by);
                    return d ? (d.textContent || '').trim().substring(0, 40) : '';
                })();
                // SVG 内嵌 <title>（图标按钮最常用）
                const svgTitle = (() => {
                    const t = el.querySelector('svg > title, svg title');
                    return t ? (t.textContent || '').trim().substring(0, 40) : '';
                })();
                // anticon class 名称（Ant Design 图标，如 anticon-close → "close"）
                const anticonName = (() => {
                    const icon = el.querySelector('[class*="anticon-"]') || (el.className && el.className.includes && el.className.includes('anticon-') ? el : null);
                    if (!icon) return '';
                    const m = (icon.className || '').match(/anticon-([a-z0-9\-]+)/);
                    return m ? m[1] : '';
                })();

                // 查找关联 label（input/select/textarea）
                let labelText = '';
                if (tag === 'input' || tag === 'select' || tag === 'textarea') {
                    // 方法1: <label for="id">
                    if (el.id) {
                        const lbl = document.querySelector('label[for=' + JSON.stringify(el.id) + ']');
                        if (lbl) labelText = (lbl.textContent || '').trim().substring(0, 40);
                    }
                    // 方法2: 祖先 <label> 包裹
                    if (!labelText) {
                        const parentLabel = el.closest('label');
                        if (parentLabel) {
                            // 取 label 自身文字，排除子 input 的文字
                            const clone = parentLabel.cloneNode(true);
                            clone.querySelectorAll('input,select,textarea').forEach(c => c.remove());
                            labelText = (clone.textContent || '').trim().substring(0, 40);
                        }
                    }
                    // 方法3: aria-labelledby
                    if (!labelText) {
                        const lblBy = el.getAttribute('aria-labelledby');
                        if (lblBy) {
                            const lblEl = document.getElementById(lblBy);
                            if (lblEl) labelText = (lblEl.textContent || '').trim().substring(0, 40);
                        }
                    }
                    // 方法4: Ant Design .ant-form-item-label 同级
                    if (!labelText) {
                        const formItem = el.closest('.ant-form-item, .el-form-item, .arco-form-item');
                        if (formItem) {
                            const lblEl = formItem.querySelector('.ant-form-item-label, .el-form-item__label, .arco-form-item-label');
                            if (lblEl) labelText = (lblEl.textContent || '').trim().substring(0, 40);
                        }
                    }
                }

                // 可读描述：按优先级取最优标签
                // 优先级：ariaLabel > text > titleAttr > nzTooltip > svgTitle > anticonName > ariaDesc > dataTestId
                const bestLabel = ariaLabel || (text.length <= 40 ? text : '') || titleAttr || nzTooltip || svgTitle || anticonName || ariaDesc || dataTestId;

                let desc = '[' + tag;
                if (type) desc += ' type="' + type + '"';
                if (role) desc += ' role="' + role + '"';
                desc += ']';
                if (labelText) desc += ' label="' + labelText + '"';
                if (placeholder) desc += ' "' + placeholder + '"';
                else if (bestLabel) desc += ' "' + bestLabel + '"';

                let domDepth = 0;
                let depthNode = el;
                while (depthNode && depthNode !== root && depthNode !== document) {
                    domDepth++;
                    depthNode = depthNode.parentElement;
                }

                elements.push({
                    tag, type, role, id, name, placeholder, ariaLabel,
                    text, value, checked, selector, desc, href, labelText,
                    titleAttr, nzTooltip, svgTitle, anticonName, dataTestId,
                    depth: domDepth,
                    rect: {x: rect.x, y: rect.y, w: rect.width, h: rect.height},
                });
            });
        } catch(e) {}
    }
    return JSON.stringify({elements});
})
"""

JS_FILL = r"""
(function(selector, text, clearFirst) {
    const el = document.querySelector(selector);
    if (!el) return JSON.stringify({error: 'element not found: ' + selector});
    el.scrollIntoView({block: 'center', behavior: 'instant'});
    el.focus();
    const tag = el.tagName.toLowerCase();
    if (tag === 'input' || tag === 'textarea') {
        const proto = tag === 'input'
            ? HTMLInputElement.prototype
            : HTMLTextAreaElement.prototype;
        const setter = Object.getOwnPropertyDescriptor(proto, 'value').set;
        if (clearFirst) setter.call(el, '');
        // React 16+ _valueTracker：重置为旧值，让 React 认为值变了
        const tracker = el._valueTracker;
        if (tracker) tracker.setValue(clearFirst ? '' : (el.value || ''));
        setter.call(el, text);
        el.dispatchEvent(new Event('input', {bubbles: true}));
        el.dispatchEvent(new Event('change', {bubbles: true}));
    } else if (el.contentEditable === 'true') {
        if (clearFirst) el.textContent = '';
        el.textContent = text;
        el.dispatchEvent(new Event('input', {bubbles: true}));
    } else {
        return JSON.stringify({error: 'element is not fillable', tag});
    }
    return JSON.stringify({ok: true, tag, value: text.substring(0, 60)});
})
"""

JS_SELECT = r"""
(function(selector, value, byLabel) {
    const el = document.querySelector(selector);
    if (!el) return JSON.stringify({error: 'element not found: ' + selector});
    if (el.tagName.toLowerCase() !== 'select') {
        return JSON.stringify({error: 'not_native_select', tag: el.tagName});
    }
    el.scrollIntoView({block: 'center', behavior: 'instant'});
    let found = false;
    for (const opt of el.options) {
        const match = byLabel
            ? opt.textContent.trim() === value
            : opt.value === value;
        if (match) {
            el.value = opt.value;
            found = true;
            break;
        }
    }
    if (!found) {
        const options = Array.from(el.options).map(o => ({
            value: o.value,
            label: o.textContent.trim()
        }));
        return JSON.stringify({error: 'option not found', value, options});
    }
    el.dispatchEvent(new Event('change', {bubbles: true}));
    return JSON.stringify({ok: true, selected: el.value});
})
"""

# 通用自定义下拉框选择（Ant Design / Element UI / Arco 等）
# 策略：点击触发元素 → 等弹出层出现 → 按文本匹配选项 → 点击选中
JS_CUSTOM_SELECT = r"""
(function(selector, label, searchText) {
    const el = document.querySelector(selector);
    if (!el) return JSON.stringify({error: 'element not found: ' + selector});

    // 1. 找到 Select 容器（向上查找 .ant-select / .el-select / [class*="select"] 等）
    const container = el.closest('.ant-select, .el-select, .arco-select, [class*="select-wrapper"]') || el;

    function normalizeText(text) {
        return String(text || '').replace(/\s+/g, ' ').trim();
    }

    function getVisibleDropdowns() {
        const dropdowns = document.querySelectorAll(
            '.ant-select-dropdown, .el-select-dropdown, .arco-select-popup, ' +
            '.ant-cascader-dropdown, [class*="select-dropdown"], [class*="dropdown-menu"], ' +
            '[role="listbox"]'
        );
        return Array.from(dropdowns).filter(dd => {
            const style = getComputedStyle(dd);
            if (style.display === 'none' || style.visibility === 'hidden') return false;
            if (dd.classList.contains('ant-select-dropdown-hidden')) return false;
            const rect = dd.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
        });
    }

    function getOptionNodes(dropdown) {
        return dropdown.querySelectorAll(
            '.ant-select-item-option, .el-select-dropdown__item, .arco-select-option, ' +
            '[class*="option"], li[role="option"], div[role="option"]'
        );
    }

    function getSelectedText(fallbackText) {
        const selItem = container.querySelector(
            '.ant-select-selection-item, .el-input__inner, .arco-select-view-value, ' +
            '[class*="selection-item"], [class*="select-view-value"]'
        );
        return normalizeText(selItem ? selItem.textContent : fallbackText);
    }

    function findSearchInput() {
        const candidates = [];

        function resolveEditable(node) {
            if (!node) return null;
            if (node.matches && node.matches('input, textarea, [contenteditable="true"]')) return node;
            return node.querySelector
                ? node.querySelector('input, textarea, [contenteditable="true"]')
                : null;
        }

        function pushCandidate(node) {
            const editable = resolveEditable(node);
            if (!editable || candidates.includes(editable)) return;
            if (editable.disabled || editable.readOnly) return;
            const rect = editable.getBoundingClientRect();
            if (rect.width === 0 && rect.height === 0) return;
            candidates.push(editable);
        }

        if (document.activeElement && (document.activeElement.matches('input, textarea, [contenteditable="true"]') || document.activeElement.getAttribute('role') === 'combobox')) {
            pushCandidate(document.activeElement);
        }

        container.querySelectorAll(
            'input, textarea, [contenteditable="true"], [role="combobox"], ' +
            '.ant-select-selection-search-input, .el-select__input, .arco-select-view-input'
        ).forEach(pushCandidate);

        getVisibleDropdowns().forEach(dd => {
            dd.querySelectorAll(
                'input, textarea, [contenteditable="true"], [role="combobox"], ' +
                '.ant-select-selection-search-input, .el-select__input, .arco-select-view-input'
            ).forEach(pushCandidate);
        });

        return candidates[0] || null;
    }

    function setSearchValue(input, text) {
        const normalized = String(text || '');
        input.focus();
        input.dispatchEvent(new MouseEvent('mousedown', {bubbles: true, cancelable: true}));
        if (typeof input.click === 'function') input.click();

        if (input.isContentEditable) {
            input.textContent = normalized;
            input.dispatchEvent(new Event('input', {bubbles: true}));
            input.dispatchEvent(new Event('change', {bubbles: true}));
            return;
        }

        const tag = (input.tagName || '').toLowerCase();
        const proto = tag === 'textarea'
            ? HTMLTextAreaElement.prototype
            : HTMLInputElement.prototype;
        const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
        if (setter) setter.call(input, '');
        else input.value = '';
        const tracker = input._valueTracker;
        if (tracker) tracker.setValue('');
        if (setter) setter.call(input, normalized);
        else input.value = normalized;
        input.dispatchEvent(new KeyboardEvent('keydown', {
            key: normalized.slice(-1) || ' ',
            bubbles: true,
            cancelable: true,
        }));
        input.dispatchEvent(new Event('input', {bubbles: true}));
        input.dispatchEvent(new Event('change', {bubbles: true}));
        input.dispatchEvent(new KeyboardEvent('keyup', {
            key: normalized.slice(-1) || ' ',
            bubbles: true,
            cancelable: true,
        }));
    }

    function tryPickOption() {
        const wanted = normalizeText(label);
        const dropdowns = getVisibleDropdowns();
        for (const dd of dropdowns) {
            const options = getOptionNodes(dd);
            for (const opt of options) {
                const text = normalizeText(opt.textContent);
                if (text === wanted) return {opt, text, method: 'custom_select'};
            }
            for (const opt of options) {
                const text = normalizeText(opt.textContent);
                if (text.includes(wanted) || wanted.includes(text)) {
                    return {opt, text, method: 'custom_select_fuzzy'};
                }
            }
        }
        return null;
    }

    // 2. 点击触发下拉（Ant Design 监听 mousedown）
    el.focus();
    el.dispatchEvent(new MouseEvent('mousedown', {bubbles: true, cancelable: true}));
    el.click();

    // 3. 等下拉弹出并查找选项（异步，最多等 500ms）
    return new Promise(function(resolve) {
        let attempts = 0;
        let searched = false;
        const query = normalizeText(searchText || label);

        function trySelect() {
            attempts++;
            const matched = tryPickOption();
            if (matched) {
                matched.opt.scrollIntoView({block: 'center', behavior: 'instant'});
                matched.opt.click();
                setTimeout(function() {
                    resolve(JSON.stringify({
                        ok: true,
                        selected: getSelectedText(matched.text),
                        method: searched ? matched.method + '_search' : matched.method,
                        searched: searched,
                        searchText: searched ? query : '',
                    }));
                }, 100);
                return;
            }

            if (!searched && query) {
                const searchInput = findSearchInput();
                if (searchInput) {
                    searched = true;
                    setSearchValue(searchInput, query);
                    setTimeout(trySelect, 180);
                    return;
                }
            }

            if (attempts < 7) {
                setTimeout(trySelect, 120);
            } else {
                // 收集可用选项返回
                const allOpts = [];
                for (const dd of getVisibleDropdowns()) {
                    const options = getOptionNodes(dd);
                    for (const opt of options) {
                        const text = normalizeText(opt.textContent);
                        if (text) allOpts.push(text);
                    }
                }
                resolve(JSON.stringify({
                    error: 'option not found',
                    label: label,
                    searched: searched,
                    searchText: searched ? query : '',
                    available: allOpts.slice(0, 20),
                }));
            }
        }
        setTimeout(trySelect, 100);
    });
})
"""

JS_WAIT_FOR = r"""
(function(selector, text) {
    if (selector) {
        const el = document.querySelector(selector);
        if (!el) return JSON.stringify({found: false, type: 'selector'});
        return JSON.stringify({found: true, type: 'selector', tag: el.tagName.toLowerCase()});
    }
    if (text) {
        const found = document.body.innerText.includes(text);
        return JSON.stringify({found, type: 'text'});
    }
    return JSON.stringify({found: true, type: 'none'});
})
"""

JS_GET_TEXT = r"""
(function(selector) {
    if (!selector || selector === 'body') {
        return document.body.innerText.substring(0, 50000);
    }
    const el = document.querySelector(selector);
    if (!el) return JSON.stringify({error: 'element not found: ' + selector});
    return el.innerText || el.textContent || '';
})
"""

JS_PRESS = r"""
(function(selector, key) {
    const target = selector ? document.querySelector(selector) : document.activeElement;
    if (!target) return JSON.stringify({error: selector ? 'element not found: ' + selector : 'no active element'});
    const opts = {key: key, code: key, bubbles: true, cancelable: true};
    target.dispatchEvent(new KeyboardEvent('keydown', opts));
    target.dispatchEvent(new KeyboardEvent('keypress', opts));
    target.dispatchEvent(new KeyboardEvent('keyup', opts));
    return JSON.stringify({ok: true, key: key});
})
"""

JS_CHECK = r"""
(function(selector, forceState) {
    const el = document.querySelector(selector);
    if (!el) return JSON.stringify({error: 'element not found: ' + selector});
    if (el.type !== 'checkbox' && el.type !== 'radio') {
        return JSON.stringify({error: 'element is not checkable', type: el.type});
    }
    if (forceState !== null) {
        if (el.checked !== forceState) el.click();
    } else {
        el.click();
    }
    el.dispatchEvent(new Event('change', {bubbles: true}));
    return JSON.stringify({ok: true, checked: el.checked});
})
"""

JS_SCROLL = r"""
(function(selector, direction, amount) {
    const target = selector ? document.querySelector(selector) : window;
    if (selector && !target) return JSON.stringify({error: 'element not found: ' + selector});
    const opts = {behavior: 'instant'};
    const px = amount || 500;
    switch(direction) {
        case 'up': opts.top = -px; break;
        case 'down': opts.top = px; break;
        case 'left': opts.left = -px; break;
        case 'right': opts.left = px; break;
    }
    if (target === window) window.scrollBy(opts);
    else target.scrollBy(opts);
    return JSON.stringify({ok: true, direction, amount: px});
})
"""

JS_LIVE_PAGE_PROBE = r"""
(() => JSON.stringify({
    title: document.title || '',
    url: location.href || '',
    hasFocus: !!document.hasFocus(),
    visibilityState: document.visibilityState || '',
    hidden: !!document.hidden,
}))()
"""

# 兜底查找：当 CSS selector 失效时，用 tag + text + href 重新定位元素
JS_FIND_FALLBACK = r"""
(function(tag, text, href) {
    const candidates = document.querySelectorAll(tag || '*');
    for (const el of candidates) {
        // 跳过不可见元素
        const rect = el.getBoundingClientRect();
        if (rect.width === 0 && rect.height === 0) continue;
        const style = getComputedStyle(el);
        if (style.display === 'none' || style.visibility === 'hidden') continue;

        // href 精确匹配（优先级最高）
        if (href && el.getAttribute('href')) {
            if (el.getAttribute('href') === href) {
                return JSON.stringify({found: true, method: 'href'});
            }
        }
        // text 精确匹配
        if (text) {
            const elText = (el.textContent || '').trim();
            if (elText === text) {
                // 找到了，执行操作前先 focus + scrollIntoView
                el.scrollIntoView({block: 'center', behavior: 'instant'});
                el.focus();
                return JSON.stringify({found: true, method: 'text_exact'});
            }
        }
    }
    // text 前缀匹配
    if (text) {
        for (const el of candidates) {
            const rect = el.getBoundingClientRect();
            if (rect.width === 0 && rect.height === 0) continue;
            const elText = (el.textContent || '').trim();
            if (elText.startsWith(text.substring(0, 20)) && elText.length < text.length * 3) {
                el.scrollIntoView({block: 'center', behavior: 'instant'});
                el.focus();
                return JSON.stringify({found: true, method: 'text_prefix'});
            }
        }
    }
    return JSON.stringify({found: false});
})
"""

# 兜底填充
JS_FILL_FALLBACK = r"""
(function(tag, text, href, fillText, clearFirst) {
    const candidates = document.querySelectorAll(tag || 'input,textarea,[contenteditable]');
    // 按 placeholder 或 aria-label 匹配
    for (const el of candidates) {
        const rect = el.getBoundingClientRect();
        if (rect.width === 0 && rect.height === 0) continue;
        const ph = el.getAttribute('placeholder') || '';
        const al = el.getAttribute('aria-label') || '';
        const elText = (el.textContent || '').trim();
        if ((text && (ph === text || al === text || elText === text)) ||
            (href && el.getAttribute('href') === href)) {
            el.scrollIntoView({block: 'center', behavior: 'instant'});
            el.focus();
            const t = el.tagName.toLowerCase();
            if (t === 'input' || t === 'textarea') {
                const proto = t === 'input' ? HTMLInputElement.prototype : HTMLTextAreaElement.prototype;
                const setter = Object.getOwnPropertyDescriptor(proto, 'value').set;
                if (clearFirst) setter.call(el, '');
                setter.call(el, fillText);
                el.dispatchEvent(new Event('input', {bubbles: true}));
                el.dispatchEvent(new Event('change', {bubbles: true}));
            } else if (el.contentEditable === 'true') {
                if (clearFirst) el.textContent = '';
                el.textContent = fillText;
                el.dispatchEvent(new Event('input', {bubbles: true}));
            }
            return JSON.stringify({ok: true, tag: t, value: fillText.substring(0, 60), method: 'fallback'});
        }
    }
    return JSON.stringify({error: 'fallback: fillable element not found', tag, text});
})
"""


# ---------------------------------------------------------------------------
# PageManager
# ---------------------------------------------------------------------------

class PageManager:
    """页面级高级操作管理器，与 CdpConnection 配合使用。

    初始化时不会连接 Chrome 或启动 daemon。
    所有操作通过 cdp（CdpConnection）实例的锁进行并发保护。
    """

    def __init__(self, cdp: Any):
        """
        :param cdp: daemon.CdpConnection 实例
        """
        self._cdp = cdp
        # page session 缓存：targetId -> sessionId（flatten 模式）
        self._sessions: dict[str, str] = {}
        self._sessions_lock = threading.Lock()
        # 元素引用缓存：ref(@eN) -> {selector, target_id, created_at}
        self._refs: dict[str, dict] = {}
        self._refs_lock = threading.Lock()
        self._ref_counter = 0
        self._ref_ttl = 600  # 10 分钟

        # ---- 网络抓包状态 ----
        # sessionId -> 是否正在抓包
        self._net_capture_active: dict[str, bool] = {}
        # sessionId -> [request_info, ...]  已完成的请求
        self._net_capture_buffer: dict[str, list] = {}
        # requestId -> {url, method, headers, postData, type, timestamp, ...}
        self._net_request_map: dict[str, dict] = {}
        # sessionId -> 最近一次网络事件时间戳，用于 idle 判定
        self._net_last_event_at: dict[str, float] = {}
        # sessionId -> 本轮抓包开始时间
        self._net_capture_started_at: dict[str, float] = {}
        self._net_lock = threading.Lock()

        # ---- follow 模式（跟踪新 tab）----
        # 当前抓包的"主" targetId（start 时设定）
        self._net_follow_origin: str = ""
        # follow 模式开启时记录的已有 targetId 集合（区分新旧）
        self._net_follow_known: set[str] = set()
        # 待处理的新 tab targetId 队列
        self._net_follow_queue: list[dict] = []
        # 已自动 attach 的新 tab: targetId -> {sessionId, url, title}
        self._net_follow_tabs: dict[str, dict] = {}

        # 注册网络事件回调到 CdpConnection，
        # 这样 _call_locked（心跳、ensure_connected 等）读 WS 时也能捕获 Network 事件
        self._cdp._network_event_handler = self._handle_network_event
        # 注册 Target 事件回调（用于 follow 模式跟踪新 tab）
        self._cdp._target_event_handler = self._handle_target_for_follow

        # ---- 标签页群组 ----
        # name -> {"color": str, "targets": [targetId, ...], "chrome_group_id": int|None}
        self._tab_groups: dict[str, dict] = {}
        # Chrome 扩展 service worker targetId 缓存（用于调用 chrome.tabs/tabGroups API）
        self._ext_sw_tabs: str = ""       # 有 chrome.tabs.group 的 SW targetId
        self._ext_sw_tabgroups: str = ""  # 有 chrome.tabGroups.update 的 SW targetId

        # CDP 新开页面统一进入固定自动化分组，方便用户识别这些页面来自自动化工具
        self._automation_group_name = "CDP自动化"
        self._automation_group_color = "purple"

        # ---- 标签页别名 ----
        # name -> targetId；用于避免按标题/URL 模糊匹配误命中
        self._tab_aliases: dict[str, str] = {}
        self._tab_aliases_lock = threading.Lock()
        self._tab_aliases_path = Path.home() / ".chrome-cdp-daemon" / "tab_aliases.json"
        self._load_tab_aliases()

    def _load_tab_aliases(self) -> None:
        try:
            if not self._tab_aliases_path.exists():
                return
            data = json.loads(self._tab_aliases_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                with self._tab_aliases_lock:
                    self._tab_aliases = {
                        str(k): str(v)
                        for k, v in data.items()
                        if isinstance(k, str) and isinstance(v, str)
                    }
        except Exception:
            pass

    def _save_tab_aliases(self) -> None:
        try:
            self._tab_aliases_path.parent.mkdir(parents=True, exist_ok=True)
            with self._tab_aliases_lock:
                payload = dict(self._tab_aliases)
            self._tab_aliases_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _validate_tab_alias(self, name: str) -> str:
        alias = (name or "").strip()
        if not alias:
            raise RuntimeError("alias 不能为空")
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", alias):
            raise RuntimeError("alias 仅支持字母、数字、点、下划线、中划线，长度 1-64")
        return alias

    def _get_bound_target(self, alias: str) -> str:
        alias = self._validate_tab_alias(alias)
        self._load_tab_aliases()
        with self._tab_aliases_lock:
            target_id = self._tab_aliases.get(alias, "")
        if not target_id:
            raise RuntimeError(f"tab alias 不存在: {alias}")

        pages = self._cdp.get_pages()
        if any(p.get("targetId") == target_id for p in pages):
            return target_id

        with self._tab_aliases_lock:
            self._tab_aliases.pop(alias, None)
        self._save_tab_aliases()
        raise RuntimeError(f"tab alias 已失效: {alias}，对应页面可能已关闭，请重新绑定")

    def _remove_aliases_for_target(self, target_id: str) -> list[str]:
        removed: list[str] = []
        with self._tab_aliases_lock:
            for name, tid in list(self._tab_aliases.items()):
                if tid == target_id:
                    removed.append(name)
                    del self._tab_aliases[name]
        if removed:
            self._save_tab_aliases()
        return removed

    def bind_tab(self, name: str, target: str = "active") -> dict:
        alias = self._validate_tab_alias(name)
        target_id = self.resolve_target(target)
        with self._tab_aliases_lock:
            self._tab_aliases[alias] = target_id
        self._save_tab_aliases()
        info = self._get_page_info(target_id)
        return {"ok": True, "alias": alias, **info}

    def get_tab_binding(self, name: str) -> dict:
        alias = self._validate_tab_alias(name)
        target_id = self._get_bound_target(alias)
        info = self._get_page_info(target_id)
        return {"ok": True, "alias": alias, **info}

    def list_tab_bindings(self) -> dict:
        self._load_tab_aliases()
        with self._tab_aliases_lock:
            items = list(self._tab_aliases.keys())
        bindings = []
        for alias in items:
            try:
                bindings.append(self.get_tab_binding(alias))
            except Exception:
                continue
        bindings.sort(key=lambda item: item.get("alias", ""))
        return {"ok": True, "bindings": bindings, "count": len(bindings)}

    def remove_tab_binding(self, name: str) -> dict:
        alias = self._validate_tab_alias(name)
        with self._tab_aliases_lock:
            existed = alias in self._tab_aliases
            target_id = self._tab_aliases.pop(alias, "")
        if existed:
            self._save_tab_aliases()
        return {"ok": True, "alias": alias, "removed": existed, "targetId": target_id}

    # =================================================================
    # Session 管理
    # =================================================================

    def _get_or_attach(self, target_id: str) -> str:
        """获取或创建到指定 page 的 CDP session（flatten 模式）。"""
        with self._sessions_lock:
            sid = self._sessions.get(target_id)
            if sid:
                return sid
        # attach（需要在 cdp._lock 内部调用）
        result = self._cdp.call("Target.attachToTarget", {
            "targetId": target_id,
            "flatten": True,
        })
        sid = result.get("sessionId", "")
        if not sid:
            raise RuntimeError(f"attach to {target_id} failed: no sessionId")
        with self._sessions_lock:
            self._sessions[target_id] = sid
        return sid

    def _invalidate_session(self, target_id: str) -> None:
        with self._sessions_lock:
            self._sessions.pop(target_id, None)

    def _handle_target_for_follow(self, method: str, params: dict) -> None:
        """处理 Target 事件用于 follow 模式。

        注意：此方法在 cdp._lock 持有状态下被调用（从 _handle_target_event），
        不能做 page_call，只能把新 tab 入队，由 _process_follow_queue 异步处理。
        """
        if method != "Target.targetCreated":
            return
        with self._net_lock:
            if not self._net_follow_origin:
                return  # follow 模式未启用
        info = params.get("targetInfo", {})
        tid = info.get("targetId", "")
        ttype = info.get("type", "")
        if not tid or ttype != "page":
            return
        with self._net_lock:
            if tid in self._net_follow_known:
                return  # 不是新 tab
            self._net_follow_queue.append({
                "targetId": tid,
                "url": info.get("url", ""),
                "title": info.get("title", ""),
            })

    def _process_follow_queue(self, origin_target_id: str) -> None:
        """处理 follow 队列：attach 新 tab + Network.enable + reload 补抓。

        必须在 cdp._lock 外调用（page_call 自己会获取锁）。
        """
        while True:
            with self._net_lock:
                if not self._net_follow_queue:
                    break
                item = self._net_follow_queue.pop(0)
            tid = item["targetId"]
            try:
                sid = self._get_or_attach(tid)
                self.page_call(tid, "Network.enable", {})
                started_at = time.time()
                with self._net_lock:
                    self._net_capture_active[sid] = True
                    self._net_capture_buffer.setdefault(sid, [])
                    self._net_last_event_at[sid] = started_at
                    self._net_capture_started_at[sid] = started_at
                    self._net_follow_tabs[tid] = {
                        "sessionId": sid,
                        "url": item.get("url", ""),
                        "title": item.get("title", ""),
                    }
                # reload 新 tab 以补抓首屏 API 请求
                # （Network.enable 在 reload 之前已生效）
                try:
                    self.page_call(tid, "Page.reload", {})
                except Exception:
                    pass
            except Exception:
                pass  # 新 tab 可能还在加载中，忽略

    def _session_to_target(self, session_id: str) -> str | None:
        """通过 sessionId 反查 targetId。"""
        with self._sessions_lock:
            for tid, sid in self._sessions.items():
                if sid == session_id:
                    return tid
        return None

    def _handle_network_event(self, event_method: str, params: dict, session_id: str) -> None:
        """处理 Network.* 事件，缓冲到抓包 buffer。"""
        with self._net_lock:
            if not self._net_capture_active.get(session_id):
                return
            self._net_last_event_at[session_id] = time.time()

            if event_method == "Network.requestWillBeSent":
                req = params.get("request", {})
                request_id = params.get("requestId", "")
                self._net_request_map[request_id] = {
                    "requestId": request_id,
                    "url": req.get("url", ""),
                    "method": req.get("method", ""),
                    "headers": req.get("headers", {}),
                    "postData": req.get("postData"),
                    "resourceType": params.get("type", ""),
                    "timestamp": params.get("timestamp", 0),
                    "sessionId": session_id,
                }

            elif event_method == "Network.responseReceived":
                request_id = params.get("requestId", "")
                response = params.get("response", {})
                info = self._net_request_map.get(request_id)
                if info and info.get("sessionId") == session_id:
                    info["status"] = response.get("status", 0)
                    info["statusText"] = response.get("statusText", "")
                    info["responseHeaders"] = response.get("headers", {})
                    info["mimeType"] = response.get("mimeType", "")
                    # 自动过滤后加入 buffer
                    if self._is_api_request(info):
                        self._net_capture_buffer.setdefault(session_id, []).append(info)

            elif event_method == "Network.loadingFinished":
                request_id = params.get("requestId", "")
                info = self._net_request_map.get(request_id)
                if info and info.get("sessionId") == session_id:
                    info["encodedDataLength"] = params.get("encodedDataLength", 0)

    @staticmethod
    def _is_api_request(info: dict) -> bool:
        """过滤静态资源，只保留 API 请求。"""
        url = info.get("url", "").lower().split("?")[0]
        # 跳过静态资源
        static_exts = (
            ".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico",
            ".woff", ".woff2", ".ttf", ".eot", ".map", ".webp", ".avif",
        )
        if any(url.endswith(ext) for ext in static_exts):
            return False
        # 跳过已知静态 MIME
        mime = info.get("mimeType", "").lower()
        static_mimes = (
            "text/css", "text/javascript", "application/javascript",
            "image/", "font/", "audio/", "video/",
        )
        if any(mime.startswith(m) for m in static_mimes):
            return False
        # 跳过 data: / chrome-extension: / blob:
        raw_url = info.get("url", "")
        if raw_url.startswith(("data:", "chrome-extension:", "blob:", "chrome:")):
            return False
        return True

    _PAGE_CALL_TIMEOUT = 30  # 单次 page_call 最大等待秒数

    def page_call(
        self, target_id: str, method: str, params: dict | None = None
    ) -> dict:
        """在指定 page session 上执行 CDP 命令（线程安全）。

        使用 flatten 模式：消息中带 sessionId 直接发送到 browser WS。
        读取 WS 响应时同时捕获 Network.* 事件用于网络抓包。
        内置超时保护，避免卡死的 target 阻塞所有后续请求。
        """
        sid = self._get_or_attach(target_id)
        with self._cdp._lock:
            if not self._cdp._ws:
                raise RuntimeError("not connected")
            self._cdp._msg_id += 1
            mid = self._cdp._msg_id
            payload: dict[str, Any] = {
                "id": mid,
                "method": method,
                "sessionId": sid,
            }
            if params:
                payload["params"] = params
            self._cdp._inflight_requests += 1
            # 临时设置 recv 超时
            old_timeout = self._cdp._ws.gettimeout()
            self._cdp._ws.settimeout(self._PAGE_CALL_TIMEOUT)
            try:
                self._cdp._ws.send(json.dumps(payload))
                deadline = time.time() + self._PAGE_CALL_TIMEOUT
                while True:
                    if time.time() > deadline:
                        self._invalidate_session(target_id)
                        raise RuntimeError(
                            f"page_call timeout ({self._PAGE_CALL_TIMEOUT}s): "
                            f"target {target_id[:8]} 可能无响应，"
                            f"尝试刷新页面或 close/reopen"
                        )
                    raw = self._cdp._ws.recv()
                    msg = json.loads(raw if isinstance(raw, str) else raw.decode())
                    # 事件帧：交给 cdp 处理 Target 事件，或捕获 Network 事件
                    if "method" in msg and "id" not in msg:
                        evt = msg.get("method", "")
                        if evt.startswith("Target."):
                            self._cdp._handle_target_event(evt, msg.get("params", {}))
                        elif evt.startswith("Network."):
                            evt_sid = msg.get("sessionId", "")
                            self._handle_network_event(evt, msg.get("params", {}), evt_sid)
                        continue
                    if msg.get("id") == mid:
                        if "error" in msg:
                            err = msg["error"]
                            # session 过期则清除缓存
                            if "session" in str(err).lower():
                                self._invalidate_session(target_id)
                            raise RuntimeError(f"CDP page {method}: {err}")
                        self._cdp._last_ok_at = time.time()
                        self._cdp._last_error = ""
                        return msg.get("result", {})
            except (TimeoutError, Exception) as exc:
                if "timeout" in str(exc).lower() or isinstance(exc, TimeoutError):
                    self._invalidate_session(target_id)
                    raise RuntimeError(
                        f"page_call timeout: target {target_id[:8]} 无响应，"
                        f"可能页面卡死。尝试: close {target_id[:8]} 后重新打开"
                    ) from exc
                raise
            finally:
                self._cdp._inflight_requests = max(0, self._cdp._inflight_requests - 1)
                # 恢复原来的 timeout
                try:
                    self._cdp._ws.settimeout(old_timeout)
                except Exception:
                    pass

    # =================================================================
    # Target 解析
    # =================================================================

    _LIVE_PROBE_TIMEOUT = 0.5
    _MUTATING_TARGET_ACTIONS = {
        "click", "click_text", "fill", "select", "check", "hover", "press", "scroll", "drag",
        "editor_set", "editor_type", "eval_js", "close_tab",
    }

    def _page_call_with_timeout(
        self,
        target_id: str,
        method: str,
        params: dict | None = None,
        timeout_sec: float | None = None,
    ) -> dict:
        """在指定 page session 上执行 CDP 命令，允许覆盖较短超时。"""
        sid = self._get_or_attach(target_id)
        timeout_val = timeout_sec or self._PAGE_CALL_TIMEOUT
        with self._cdp._lock:
            if not self._cdp._ws:
                raise RuntimeError("not connected")
            self._cdp._msg_id += 1
            mid = self._cdp._msg_id
            payload: dict[str, Any] = {
                "id": mid,
                "method": method,
                "sessionId": sid,
            }
            if params:
                payload["params"] = params
            self._cdp._inflight_requests += 1
            old_timeout = self._cdp._ws.gettimeout()
            self._cdp._ws.settimeout(timeout_val)
            try:
                self._cdp._ws.send(json.dumps(payload))
                deadline = time.time() + timeout_val
                while True:
                    if time.time() > deadline:
                        self._invalidate_session(target_id)
                        raise RuntimeError(
                            f"page_call timeout ({timeout_val}s): target {target_id[:8]} 可能无响应"
                        )
                    raw = self._cdp._ws.recv()
                    msg = json.loads(raw if isinstance(raw, str) else raw.decode())
                    if "method" in msg and "id" not in msg:
                        evt = msg.get("method", "")
                        if evt.startswith("Target."):
                            self._cdp._handle_target_event(evt, msg.get("params", {}))
                        elif evt.startswith("Network."):
                            evt_sid = msg.get("sessionId", "")
                            self._handle_network_event(evt, msg.get("params", {}), evt_sid)
                        continue
                    if msg.get("id") == mid:
                        if "error" in msg:
                            err = msg["error"]
                            if "session" in str(err).lower():
                                self._invalidate_session(target_id)
                            raise RuntimeError(f"CDP page {method}: {err}")
                        self._cdp._last_ok_at = time.time()
                        self._cdp._last_error = ""
                        return msg.get("result", {})
            finally:
                self._cdp._inflight_requests = max(0, self._cdp._inflight_requests - 1)
                try:
                    self._cdp._ws.settimeout(old_timeout)
                except Exception:
                    pass

    def _probe_live_page(self, page: dict) -> dict | None:
        """实时探测单个页面的焦点/可见性状态。"""
        target_id = page.get("targetId", "")
        if not target_id:
            return None
        try:
            result = self._page_call_with_timeout(target_id, "Runtime.evaluate", {
                "expression": JS_LIVE_PAGE_PROBE,
                "returnByValue": True,
                "awaitPromise": False,
            }, timeout_sec=self._LIVE_PROBE_TIMEOUT)
            raw = result.get("result", {}).get("value", "")
            data = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(data, dict):
                return None
            return {
                "targetId": target_id,
                "url": data.get("url") or page.get("url", ""),
                "title": data.get("title") or page.get("title", ""),
                "hasFocus": bool(data.get("hasFocus")),
                "visibilityState": str(data.get("visibilityState", "")),
                "hidden": bool(data.get("hidden")),
            }
        except Exception:
            return None

    def detect_active_page(self) -> dict:
        """通过实时焦点探测识别当前活动页；不依赖历史缓存。"""
        try:
            with self._cdp._lock:
                self._cdp._rebuild_pages_cache_locked()
        except Exception:
            pass

        pages = self._cdp.get_pages()
        live_pages: list[dict] = []
        visible_pages: list[dict] = []
        for page in pages:
            item = self._probe_live_page(page)
            if not item:
                continue
            live_pages.append(item)
            if item.get("hasFocus"):
                return {"ok": True, "page": item, "source": "live_focus"}
            if item.get("visibilityState") == "visible" and not item.get("hidden", False):
                visible_pages.append(item)

        if not live_pages:
            return {"ok": False, "error": "active_page not found by live probe", "source": "live_probe"}

        if len(visible_pages) == 1:
            return {"ok": True, "page": visible_pages[0], "source": "live_visible"}
        if len(visible_pages) > 1:
            return {
                "ok": False,
                "error": "active_page ambiguous: multiple visible pages",
                "source": "live_visible",
                "candidates": visible_pages,
            }

        return {
            "ok": False,
            "error": "active_page not found by live probe",
            "source": "live_probe",
            "candidates": live_pages[:10],
        }

    @staticmethod
    def _target_candidate_brief(page: dict) -> dict:
        """压缩 target 信息，供 CLI 错误提示和 JSON 输出使用。"""
        return {
            "targetId": page.get("targetId", ""),
            "title": page.get("title", ""),
            "url": page.get("url", ""),
            "type": page.get("type", ""),
        }

    @staticmethod
    def _target_summary(pages: list[dict]) -> str:
        """生成一行候选摘要，避免 target 解析失败时只给模糊错误。"""
        return ", ".join(
            f"{p.get('targetId', '')[:8]} {p.get('url', '')}" for p in pages[:8]
        )

    def _matching_pages_for_selector(self, selector: str) -> list[dict]:
        """按通用 selector 返回所有候选页面。"""
        pages = self._cdp.get_pages()
        if selector.startswith("host:"):
            expected = selector[5:].strip().lower()
            result = []
            for page in pages:
                host = (urllib.parse.urlparse(str(page.get("url", ""))).hostname or "").lower()
                if host == expected:
                    result.append(page)
            return result
        if selector.startswith("title:"):
            keyword = selector[6:].strip().lower()
            return [p for p in pages if keyword and keyword in str(p.get("title", "")).lower()]
        if selector.startswith("url-strict:"):
            keywords = [k.strip().lower() for k in selector[11:].split(",") if k.strip()]
            return [
                p for p in pages
                if keywords and all(kw in str(p.get("url", "")).lower() for kw in keywords)
            ]
        if selector.startswith("url:"):
            keywords = [k.strip().lower() for k in selector[4:].split(",") if k.strip()]
            return [
                p for p in pages
                if keywords and all(kw in str(p.get("url", "")).lower() for kw in keywords)
            ]
        if selector and len(selector) < 32:
            return [
                p for p in pages
                if str(p.get("targetId", "")).upper().startswith(selector.upper())
            ]
        return []

    def _resolve_unique_selector(self, selector: str, candidates: list[dict]) -> str:
        """严格 selector 必须唯一命中，否则要求用户绑定 alias 或指定 targetId。"""
        if not candidates:
            raise RuntimeError(f"no page matching {selector}")
        if len(candidates) > 1:
            summary = self._target_summary(candidates)
            raise RuntimeError(
                f"multiple pages matching {selector}; candidates: {summary}; "
                "use: tab bind <name> --target <targetId>"
            )
        return str(candidates[0].get("targetId", ""))

    def resolve_target_info(self, target: str = "active") -> dict:
        """解析 target 并返回结构化页面信息，供 target resolve CLI 使用。"""
        try:
            target_id = self.resolve_target(target)
            pages = self._cdp.get_pages()
            page = next((p for p in pages if p.get("targetId") == target_id), {"targetId": target_id})
            return {"ok": True, **self._target_candidate_brief(page)}
        except Exception as exc:
            candidates = [self._target_candidate_brief(p) for p in self._matching_pages_for_selector(target)]
            result: dict[str, Any] = {"ok": False, "error": str(exc)}
            if candidates:
                result["candidates"] = candidates
                result["suggestion"] = "tab bind <name> --target <targetId>"
            return result

    def resolve_target(self, target: str = "active") -> str:
        """解析 target 标识为 targetId。

        支持：
          - "active"       macOS 上优先用前台 Chrome 窗口识别当前 tab，
                           再回退 Target.targetActivated 缓存，最后回退最后一个 page
          - "tab:name"     已绑定的标签页别名
          - "url:keyword"  URL 包含 keyword 的第一个 page（支持多关键词用逗号分隔）
          - "url-strict:keyword" URL 唯一命中才返回，否则列出候选
          - "host:hostname" URL host 唯一命中才返回，否则列出候选
          - "title:keyword" title 唯一命中才返回，否则列出候选
          - targetId       完整或前缀匹配
        """
        if target.startswith("tab:"):
            return self._get_bound_target(target[4:])

        if not target or target == "active":
            result = self.detect_active_page()
            if result.get("ok"):
                return result.get("page", {}).get("targetId", "")
            candidates = result.get("candidates", [])
            if candidates:
                summary = ", ".join(
                    f"{p.get('targetId','')[:8]} {p.get('url','')}" for p in candidates[:5]
                )
                raise RuntimeError(f"{result.get('error', 'no active page found')}；候选: {summary}")
            raise RuntimeError(result.get("error", "no active page found"))

        if target.startswith("url:"):
            # 支持多关键词（逗号分隔），所有词都命中才算匹配
            for p in self._matching_pages_for_selector(target):
                return p["targetId"]
            keywords = [k.strip().lower() for k in target[4:].split(",") if k.strip()]
            raise RuntimeError(f"no page matching url:{','.join(keywords)}")

        if target.startswith(("host:", "title:", "url-strict:")):
            return self._resolve_unique_selector(target, self._matching_pages_for_selector(target))

        # targetId（支持短前缀）
        if len(target) < 32:
            candidates = self._matching_pages_for_selector(target)
            if len(candidates) == 1:
                return candidates[0]["targetId"]
            if len(candidates) > 1:
                summary = self._target_summary(candidates)
                raise RuntimeError(
                    f"multiple pages matching targetId prefix: {target}; candidates: {summary}; "
                    "use: tab bind <name> --target <targetId>"
                )
            raise RuntimeError(f"no page matching targetId prefix: {target}")
        return target

    def get_cookies_for_url(self, url: str, target: str = "active") -> dict:
        """在页面 session 中按 URL 获取 cookie，避免读取全域 cookie。"""
        target_url = str(url or "").strip()
        parsed = urllib.parse.urlparse(target_url)
        if not parsed.scheme or not parsed.hostname:
            return {"ok": False, "error": f"invalid url: {url}"}
        target_id = self.resolve_target(target)
        result = self.page_call(target_id, "Network.getCookies", {"urls": [target_url]})
        return {
            "ok": True,
            "url": target_url,
            "target_id": target_id,
            "cookies": result.get("cookies", []),
        }

    # =================================================================
    # 元素引用管理（@e1, @e2, ...）
    # =================================================================

    def _gc_refs(self) -> None:
        now = time.time()
        with self._refs_lock:
            expired = [r for r, v in self._refs.items()
                       if now - v.get("created_at", 0) > self._ref_ttl]
            for r in expired:
                del self._refs[r]

    def _store_refs(self, elements: list[dict], target_id: str) -> list[dict]:
        """存储元素引用，每次 snapshot 重置该 target 的编号。"""
        self._gc_refs()
        now = time.time()
        result = []
        with self._refs_lock:
            # 清除该 target 的旧引用
            old = [k for k, v in self._refs.items() if v.get("target_id") == target_id]
            for k in old:
                del self._refs[k]
            self._ref_counter = 0
            for el in elements:
                self._ref_counter += 1
                ref = f"@e{self._ref_counter}"
                self._refs[ref] = {
                    "selector": el.get("selector", ""),
                    "text": el.get("text", ""),
                    "href": el.get("href", ""),
                    "tag": el.get("tag", ""),
                    "target_id": target_id,
                    "created_at": now,
                }
                el["ref"] = ref
                result.append(el)
        return result

    def resolve_ref(self, ref_or_selector: str) -> dict:
        """解析 @eN 引用或原始 CSS 选择器，返回 {selector, target_id?, text?, href?, tag?}。"""
        if ref_or_selector.startswith("@e"):
            with self._refs_lock:
                info = self._refs.get(ref_or_selector)
                if not info:
                    raise RuntimeError(f"引用 {ref_or_selector} 不存在，请重新 snapshot")
                if time.time() - info.get("created_at", 0) > self._ref_ttl:
                    del self._refs[ref_or_selector]
                    raise RuntimeError(f"引用 {ref_or_selector} 已过期，请重新 snapshot")
                return dict(info)  # 返回副本
        return {"selector": ref_or_selector}

    def _refine_selector(self, target_id: str, selector: str) -> str:
        """将 selector 解析到当前页面中最合适的可见节点，规避隐藏实例和重复 id。"""
        expr = f"""
        (function(selector) {{
            function isVisible(el) {{
                if (!el) return false;
                const rect = el.getBoundingClientRect();
                const style = getComputedStyle(el);
                return rect.width > 0 && rect.height > 0 &&
                    style.display !== 'none' && style.visibility !== 'hidden' &&
                    parseFloat(style.opacity || '1') !== 0;
            }}
            function buildSelector(el) {{
                if (el.id) {{
                    const idSel = '#' + CSS.escape(el.id);
                    if (document.querySelectorAll(idSel).length === 1) return idSel;
                }}
                const parts = [];
                let cur = el;
                for (let i = 0; i < 8 && cur && cur !== document.body; i++) {{
                    let part = cur.tagName.toLowerCase();
                    const name = cur.getAttribute && cur.getAttribute('name');
                    if (name) {{
                        const nameSel = part + '[name=' + JSON.stringify(name) + ']';
                        if (document.querySelectorAll(nameSel).length === 1) {{
                            parts.unshift(nameSel);
                            return parts.join(' > ');
                        }}
                    }}
                    const role = cur.getAttribute && cur.getAttribute('role');
                    if (role) {{
                        const roleSel = part + '[role=' + JSON.stringify(role) + ']';
                        if (document.querySelectorAll(roleSel).length === 1) {{
                            parts.unshift(roleSel);
                            return parts.join(' > ');
                        }}
                    }}
                    const siblings = cur.parentElement
                        ? Array.from(cur.parentElement.children).filter(c => c.tagName === cur.tagName)
                        : [];
                    if (siblings.length > 1) {{
                        part += ':nth-of-type(' + (siblings.indexOf(cur) + 1) + ')';
                    }}
                    parts.unshift(part);
                    cur = cur.parentElement;
                }}
                return parts.join(' > ');
            }}

            let matches = [];
            try {{
                matches = Array.from(document.querySelectorAll(selector));
            }} catch (e) {{
                return JSON.stringify({{ok: false, error: 'invalid selector'}});
            }}
            if (!matches.length) return JSON.stringify({{ok: false, error: 'not found'}});

            const visible = matches.filter(isVisible);
            const visibleInDialog = visible.filter(el => el.closest('.ant-modal-root, .ant-modal-wrap, .ant-modal, [role="dialog"]'));
            const pool = visibleInDialog.length ? visibleInDialog : (visible.length ? visible : matches);
            pool.sort((a, b) => {{
                const ra = a.getBoundingClientRect();
                const rb = b.getBoundingClientRect();
                return (rb.width * rb.height) - (ra.width * ra.height);
            }});
            const picked = pool[0];
            return JSON.stringify({{
                ok: true,
                selector: buildSelector(picked),
                matchedCount: matches.length,
                visibleCount: visible.length,
                visibleDialogCount: visibleInDialog.length,
            }});
        }})({json.dumps(selector)})
        """
        try:
            data = self._evaluate_json(target_id, expr)
        except Exception:
            return selector
        if data.get("ok") and data.get("selector"):
            return str(data["selector"])
        return selector

    # =================================================================
    # 页面 JS 执行
    # =================================================================

    def _evaluate(self, target_id: str, expression: str) -> Any:
        """在指定页面执行 JS 并返回值。"""
        result = self.page_call(target_id, "Runtime.evaluate", {
            "expression": expression,
            "returnByValue": True,
            "awaitPromise": False,
        })
        val = result.get("result", {})
        if val.get("type") == "undefined":
            return None
        if "value" in val:
            return val["value"]
        if val.get("subtype") == "error":
            raise RuntimeError(f"JS error: {val.get('description', '')}")
        return val

    def _evaluate_json(self, target_id: str, expression: str) -> dict:
        """执行 JS 并解析 JSON 结果。"""
        raw = self._evaluate(target_id, expression)
        if isinstance(raw, str):
            return json.loads(raw)
        return raw if isinstance(raw, dict) else {}

    # =================================================================
    # 高级动作
    # =================================================================

    _SNAPSHOT_MAX_ELEMENTS = 5000  # DOM 元素超过此数量时自动限定 scope

    def snapshot(
        self,
        target: str = "active",
        scope: str | None = None,
        include_cursor: bool = False,
        compact: bool = False,
        depth: int | None = None,
        include_urls: bool = False,
    ) -> dict:
        """获取页面可交互元素快照，返回带 @eN 引用的元素列表。

        大 DOM 保护策略（按优先级）：
        1. 用户指定了 scope → 直接用
        2. DOM 未超限 → 全页扫描
        3. DOM 超限但 scope 候选**覆盖整个视口** → 用最佳候选
        4. DOM 超限且无合适候选 → 不裁剪 scope，让 JS_SNAPSHOT 内部的
           "仅取可见元素"过滤来限流（避免把右侧面板整体漏掉）
        """
        target_id = self.resolve_target(target)

        # ---- 大 DOM 保护：预检元素数量 ----
        if not scope:
            try:
                count = self._evaluate(target_id, "document.querySelectorAll('*').length")
                if isinstance(count, (int, float)) and count > self._SNAPSHOT_MAX_ELEMENTS:
                    # 找一个覆盖最多视口面积的单容器（而不是取第一个元素数够小的）
                    auto_scope = self._evaluate(target_id, r"""
                        (() => {
                            const vw = window.innerWidth, vh = window.innerHeight;
                            const vpArea = vw * vh;
                            const candidates = [
                                'body', '#app', '#root', 'main', '[role=main]',
                                '.main-content', '.content', '.container',
                                '[class*=layout]', '[class*=Layout]',
                            ];
                            let bestSel = '', bestScore = 0;
                            for (const s of candidates) {
                                const el = document.querySelector(s);
                                if (!el) continue;
                                const elCount = el.querySelectorAll('*').length;
                                if (elCount > 8000) continue;  // 太大跳过
                                const r = el.getBoundingClientRect();
                                const visArea = Math.min(r.right, vw) * Math.min(r.bottom, vh)
                                              - Math.max(r.left, 0) * Math.max(r.top, 0);
                                const coverage = visArea / vpArea;
                                // 覆盖率 > 80% 才考虑，取元素数最多（内容最丰富）的那个
                                if (coverage > 0.8 && elCount > bestScore) {
                                    bestScore = elCount;
                                    bestSel = s;
                                }
                            }
                            return bestSel;
                        })()
                    """)
                    # 只有候选覆盖率足够高才采用，否则保持全页（让 JS_SNAPSHOT 内部过滤）
                    if auto_scope and auto_scope != "body":
                        scope = auto_scope
            except Exception:
                pass

        scope_arg = json.dumps(scope) if scope else "null"
        cursor_arg = "true" if include_cursor else "false"
        expr = f"({JS_SNAPSHOT})({scope_arg}, {cursor_arg})"
        data = self._evaluate_json(target_id, expr)
        if "error" in data:
            raise RuntimeError(data["error"])
        raw_elements = data.get("elements", [])
        if isinstance(depth, int) and depth >= 0:
            raw_elements = [
                el for el in raw_elements
                if not isinstance(el, dict) or int(el.get("depth", 0) or 0) <= depth
            ]
        elements = self._store_refs(raw_elements, target_id)
        if compact:
            compact_keys = {"ref", "desc", "value", "checked", "depth"}
            if include_urls:
                compact_keys.add("href")
            elements = [
                {k: v for k, v in el.items() if k in compact_keys and v not in ("", None, False)}
                for el in elements
            ]
        result: dict[str, Any] = {"elements": elements, "target_id": target_id, "count": len(elements)}
        if scope:
            result["scope"] = scope
        if compact:
            result["compact"] = True
        if depth is not None:
            result["depth"] = depth
        if include_urls:
            result["include_urls"] = True
        try:
            result["url"] = self._evaluate(target_id, "location.href") or ""
        except Exception:
            pass
        return result

    def _get_element_center(self, target_id: str, selector: str) -> dict:
        """获取元素中心坐标（scrollIntoView + getBoundingClientRect）。

        返回 {"x": int, "y": int, "tag": str} 或抛异常。
        """
        rect_data = self._evaluate(target_id, f"""
            (function() {{
                var el = document.querySelector({json.dumps(selector)});
                if (!el) return JSON.stringify({{error: 'not found: ' + {json.dumps(selector)}}});
                el.scrollIntoView({{block: 'center', behavior: 'instant'}});
                var r = el.getBoundingClientRect();
                return JSON.stringify({{x: Math.round(r.x + r.width/2), y: Math.round(r.y + r.height/2), tag: el.tagName.toLowerCase()}});
            }})()
        """)
        rect = json.loads(rect_data) if isinstance(rect_data, str) else rect_data
        if "error" in rect:
            raise RuntimeError(rect["error"])
        return rect

    def _cdp_mouse_click(
        self, target_id: str, x: int, y: int,
        button: str = "left", click_count: int = 1,
    ) -> None:
        """CDP 原生鼠标点击序列：mouseMoved → mousePressed → mouseReleased。"""
        self.page_call(target_id, "Input.dispatchMouseEvent",
            {"type": "mouseMoved", "x": x, "y": y})
        self.page_call(target_id, "Input.dispatchMouseEvent",
            {"type": "mousePressed", "x": x, "y": y,
             "button": button, "clickCount": click_count})
        self.page_call(target_id, "Input.dispatchMouseEvent",
            {"type": "mouseReleased", "x": x, "y": y,
             "button": button, "clickCount": click_count})

    def _js_click(self, target_id: str, selector: str) -> bool:
        """通过 JS el.click() 触发点击，穿透 Angular Zone / Vue 响应式。
        返回 True 表示找到元素并 click()，False 表示未找到。
        """
        result = self._evaluate(target_id, f"""
            (() => {{
                var el = document.querySelector({json.dumps(selector)});
                if (!el) return false;
                el.click();
                return true;
            }})()
        """)
        return bool(result)

    @staticmethod
    def _is_framework_element(attrs: dict) -> bool:
        """检测元素是否由 Angular / Vue / Svelte 等框架渲染。
        这些框架的 click 事件绑定在 Zone/响应式系统上，纯 CDP 鼠标事件无法触发。
        """
        cls = attrs.get("className", "") or ""
        outer = attrs.get("outerHTML", "") or ""
        # Angular: _ngcontent-xxx、ng-star-inserted、[role=tab] 在 nz-tabs 里
        if "_ngcontent" in outer or "ng-star-inserted" in cls:
            return True
        # Vue: __vue__ 属性或 v- 指令
        if "__vue__" in outer or " v-" in outer:
            return True
        return False

    def click(
        self,
        ref_or_selector: str,
        target: str = "active",
        dblclick: bool = False,
        right: bool = False,
        at: tuple[int, int] | None = None,
        force_js: bool = False,
    ) -> dict:
        """点击元素。CDP 原生鼠标事件 + Angular/Vue Zone 自动兼容。

        自动检测 Angular/Vue 渲染的元素，先发 CDP 鼠标事件，再追加
        JS el.click() 确保 Zone 事件触发。
        :param force_js: True 时强制只走 JS click（跳过 CDP 鼠标事件）
        :param dblclick: True 时双击（clickCount=2）
        :param right: True 时右键点击（触发右键菜单）
        :param at: 指定坐标点击，忽略 ref_or_selector
        """
        target_id = self.resolve_target(target)
        btn = "right" if right else "left"

        # --at 坐标点击（仅 CDP，不含 JS click）
        if at:
            x, y = at
            if not force_js:
                self._cdp_mouse_click(target_id, x, y, button=btn)
                if dblclick and not right:
                    self._cdp_mouse_click(target_id, x, y, click_count=2)
            return {"ok": True, "at": [x, y], "dblclick": dblclick, "right": right}

        ref_info = self.resolve_ref(ref_or_selector)
        target_id = ref_info.get("target_id") or target_id
        selector = self._refine_selector(target_id, ref_info["selector"])
        if not selector:
            raise RuntimeError(f"引用 {ref_or_selector} 无可用选择器")

        # 获取元素中心坐标 + 框架检测属性
        try:
            rect = self._get_element_center(target_id, selector)
        except RuntimeError:
            fallback_text = ref_info.get("text", "")
            fallback_href = ref_info.get("href", "")
            fallback_tag = ref_info.get("tag", "")
            if fallback_text or fallback_href:
                fb_data = self._evaluate(target_id, f"""
                    (function() {{
                        var tag = {json.dumps(fallback_tag or '*')};
                        var text = {json.dumps(fallback_text)};
                        var href = {json.dumps(fallback_href)};
                        var candidates = document.querySelectorAll(tag);
                        for (var el of candidates) {{
                            var r = el.getBoundingClientRect();
                            if (r.width === 0 && r.height === 0) continue;
                            if (href && el.getAttribute('href') === href) {{
                                el.scrollIntoView({{block: 'center', behavior: 'instant'}});
                                r = el.getBoundingClientRect();
                                return JSON.stringify({{x: Math.round(r.x+r.width/2), y: Math.round(r.y+r.height/2), tag: el.tagName.toLowerCase(), method: 'href_fallback'}});
                            }}
                            if (text && (el.textContent||'').trim() === text) {{
                                el.scrollIntoView({{block: 'center', behavior: 'instant'}});
                                r = el.getBoundingClientRect();
                                return JSON.stringify({{x: Math.round(r.x+r.width/2), y: Math.round(r.y+r.height/2), tag: el.tagName.toLowerCase(), method: 'text_fallback'}});
                            }}
                        }}
                        return JSON.stringify({{error: 'fallback not found', tag: tag, text: text, href: href}});
                    }})()
                """)
                rect = json.loads(fb_data) if isinstance(fb_data, str) else fb_data
                if "error" in rect:
                    raise RuntimeError(
                        f"selector 和 fallback 均失败: selector={selector}, "
                        f"text={fallback_text!r}, href={fallback_href!r}"
                    )
            else:
                raise

        x, y = rect["x"], rect["y"]

        # 检测是否为 Angular/Vue 框架元素
        framework_el = self._evaluate(target_id, f"""
            (() => {{
                var el = document.querySelector({json.dumps(selector)});
                if (!el) return false;
                var html = el.outerHTML.slice(0, 300);
                var cls = el.className || '';
                var attrs = el.getAttributeNames ? el.getAttributeNames() : [];
                // Angular: _ngcontent-xxx、ng-star-inserted、CDK 属性
                if (html.includes('_ngcontent') || cls.includes('ng-star')) return true;
                // Angular CDK 特征属性（nztabnavitem、cdkmonitor、cdk开头）
                if (attrs.some(a => a.startsWith('nz') || a.startsWith('cdk') || a === '_nghost' || a.startsWith('_ng'))) return true;
                // Vue
                if (typeof el.__vue__ !== 'undefined' || typeof el.__vueParentComponent !== 'undefined') return true;
                // Angular Zone click listener（__zone_symbol__clickfalse）
                if (el.__zone_symbol__clickfalse && el.__zone_symbol__clickfalse.length) return true;
                return false;
            }})()
        """)
        is_framework = bool(framework_el)

        js_click_done = False
        if force_js or (is_framework and not right):
            # 框架元素：JS click() 优先（穿透 Zone），再叠加 CDP 鼠标（触发 ripple 等视觉反馈）
            js_click_done = self._js_click(target_id, selector)

        if not force_js:
            self._cdp_mouse_click(target_id, x, y, button=btn)
            if dblclick and not right:
                self._cdp_mouse_click(target_id, x, y, click_count=2)

        # 框架元素且 CDP 鼠标后仍需保证 Zone 触发（double-fire 幂等）
        if is_framework and not js_click_done and not right:
            self._js_click(target_id, selector)
            js_click_done = True

        return {
            "ok": True,
            "tag": rect.get("tag", ""),
            "dblclick": dblclick,
            "right": right,
            "at": [x, y],
            "js_click": js_click_done,
            "framework": is_framework,
        }

    def fill(
        self,
        ref_or_selector: str,
        text: str,
        clear: bool = True,
        target: str = "active",
        native: bool = False,
        submit: bool = False,
    ) -> dict:
        """填充 input/textarea/contenteditable。

        :param native: True 时使用 CDP Input.insertText 模拟真实键入，
                       兼容 Vue/React 等框架的响应式绑定。
        :param submit: True 时填充后自动按 Enter 键提交。
        """
        ref_info = self.resolve_ref(ref_or_selector)
        target_id = ref_info.get("target_id") or self.resolve_target(target)
        selector = self._refine_selector(target_id, ref_info["selector"])
        if not selector:
            raise RuntimeError(f"引用 {ref_or_selector} 无可用选择器")

        if native:
            # ---- CDP 原生输入模式 ----
            # 1. 聚焦 + 清空（含 React _valueTracker 重置）
            self._evaluate(target_id, f"""
                (function() {{
                    var el = document.querySelector({json.dumps(selector)});
                    if (!el) return;
                    el.focus();
                    el.click();
                    if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {{
                        var tracker = el._valueTracker;
                        if (tracker) tracker.setValue(el.value || '');
                        el.value = '';
                        el.dispatchEvent(new Event('input', {{bubbles: true}}));
                    }} else if (el.contentEditable === 'true') {{
                        el.textContent = '';
                    }}
                }})()
            """)
            # 2. 用 Input.insertText 逐段插入（触发框架响应式）
            self.page_call(target_id, "Input.insertText", {"text": text})
            result = {"ok": True, "tag": "input", "value": text[:60], "method": "native"}
        else:
            # ---- 原有 JS setter 路径 ----
            clear_arg = "true" if clear else "false"
            expr = f'({JS_FILL})({json.dumps(selector)}, {json.dumps(text)}, {clear_arg})'
            result = self._evaluate_json(target_id, expr)
            if "error" in result:
                # 兜底
                fallback_text = ref_info.get("text", "")
                fallback_href = ref_info.get("href", "")
                fallback_tag = ref_info.get("tag", "")
                if fallback_text or fallback_href:
                    expr2 = (
                        f'({JS_FILL_FALLBACK})'
                        f'({json.dumps(fallback_tag or "input,textarea")},'
                        f' {json.dumps(fallback_text)},'
                        f' {json.dumps(fallback_href)},'
                        f' {json.dumps(text)}, {clear_arg})'
                    )
                    result = self._evaluate_json(target_id, expr2)
                    if "error" in result:
                        raise RuntimeError(result["error"])
                else:
                    raise RuntimeError(result["error"])

        # ---- submit: 填充后自动按 Enter ----
        if submit:
            self.press("Enter", ref_or_selector=None, target=target)
            result["submitted"] = True

        return result

    def select(
        self,
        ref_or_selector: str,
        value: str,
        by_label: bool = False,
        search_text: str | None = None,
        target: str = "active",
    ) -> dict:
        """选择下拉框选项。

        自动兼容原生 <select> 和自定义下拉组件（Ant Design / Element UI / Arco 等）。
        """
        ref_info = self.resolve_ref(ref_or_selector)
        target_id = ref_info.get("target_id") or self.resolve_target(target)
        selector = self._refine_selector(target_id, ref_info["selector"])
        if not selector:
            raise RuntimeError(f"引用 {ref_or_selector} 无可用选择器")

        # 1. 先尝试原生 <select>
        label_arg = "true" if by_label else "false"
        expr = f'({JS_SELECT})({json.dumps(selector)}, {json.dumps(value)}, {label_arg})'
        data = self._evaluate_json(target_id, expr)
        if "error" not in data:
            return data

        # 2. 不是原生 select → 走自定义下拉框路径（awaitPromise）
        if data.get("error") == "not_native_select" or "not a <select>" in str(data.get("error", "")):
            expr2 = f'({JS_CUSTOM_SELECT})({json.dumps(selector)}, {json.dumps(value)}, {json.dumps(search_text)})'
            result = self.page_call(target_id, "Runtime.evaluate", {
                "expression": expr2,
                "returnByValue": True,
                "awaitPromise": True,
            })
            val = result.get("result", {})
            if "value" in val:
                parsed = json.loads(val["value"]) if isinstance(val["value"], str) else val["value"]
                if "error" in parsed:
                    raise RuntimeError(f"选项未找到: {parsed}")
                return parsed

        raise RuntimeError(data.get("error", "select failed"))

    def check(
        self,
        ref_or_selector: str,
        checked: bool | None = None,
        target: str = "active",
    ) -> dict:
        """勾选/取消勾选 checkbox 或 radio。"""
        ref_info = self.resolve_ref(ref_or_selector)
        target_id = ref_info.get("target_id") or self.resolve_target(target)
        selector = self._refine_selector(target_id, ref_info["selector"])
        if not selector:
            raise RuntimeError(f"引用 {ref_or_selector} 无可用选择器")
        state_arg = json.dumps(checked) if checked is not None else "null"
        expr = f'({JS_CHECK})({json.dumps(selector)}, {state_arg})'
        data = self._evaluate_json(target_id, expr)
        if "error" in data:
            raise RuntimeError(data["error"])
        return data

    # 常用键名 → {key, code, keyCode, text} 映射
    # text 字段用于 char 事件，模拟真实键盘的完整事件序列
    _KEY_MAP: dict[str, dict[str, Any]] = {
        "Enter":     {"key": "Enter",     "code": "Enter",     "keyCode": 13, "text": "\r"},
        "Tab":       {"key": "Tab",       "code": "Tab",       "keyCode": 9,  "text": ""},
        "Escape":    {"key": "Escape",    "code": "Escape",    "keyCode": 27, "text": ""},
        "Backspace": {"key": "Backspace", "code": "Backspace", "keyCode": 8,  "text": ""},
        "Delete":    {"key": "Delete",    "code": "Delete",    "keyCode": 46, "text": ""},
        "ArrowUp":   {"key": "ArrowUp",   "code": "ArrowUp",   "keyCode": 38, "text": ""},
        "ArrowDown": {"key": "ArrowDown", "code": "ArrowDown", "keyCode": 40, "text": ""},
        "ArrowLeft": {"key": "ArrowLeft", "code": "ArrowLeft", "keyCode": 37, "text": ""},
        "ArrowRight":{"key": "ArrowRight","code": "ArrowRight","keyCode": 39, "text": ""},
        "Space":     {"key": " ",         "code": "Space",     "keyCode": 32, "text": " "},
    }

    def hover(
        self,
        ref_or_selector: str | None = None,
        at: tuple[int, int] | None = None,
        target: str = "active",
    ) -> dict:
        """鼠标悬浮（CDP Input.dispatchMouseEvent mouseMoved）。

        用于触发 hover 下拉菜单、tooltip 等。
        """
        target_id = self.resolve_target(target)

        if at:
            x, y = at
        elif ref_or_selector:
            ref_info = self.resolve_ref(ref_or_selector)
            sel = self._refine_selector(target_id, ref_info["selector"])
            rect_data = self._evaluate(target_id, f"""
                (function() {{
                    var el = document.querySelector({json.dumps(sel)});
                    if (!el) return JSON.stringify({{error: 'not found'}});
                    var r = el.getBoundingClientRect();
                    return JSON.stringify({{x: Math.round(r.x + r.width/2), y: Math.round(r.y + r.height/2)}});
                }})()
            """)
            rect = json.loads(rect_data) if isinstance(rect_data, str) else rect_data
            if "error" in rect:
                raise RuntimeError(rect["error"])
            x, y = rect["x"], rect["y"]
        else:
            raise RuntimeError("hover 需要 @ref 或 --at x,y")

        self.page_call(target_id, "Input.dispatchMouseEvent", {
            "type": "mouseMoved",
            "x": x,
            "y": y,
        })
        return {"ok": True, "at": [x, y]}

    # 组合键修饰符名称 → CDP modifiers bitmask
    _MODIFIER_MAP: dict[str, int] = {
        "alt": 1, "option": 1,
        "ctrl": 2, "control": 2,
        "meta": 4, "cmd": 4, "command": 4, "win": 4,
        "shift": 8,
    }

    @classmethod
    def _parse_key_combo(cls, key_str: str) -> tuple[int, str]:
        """解析组合键字符串，返回 (modifiers_bitmask, main_key)。

        示例：
          "Meta+S"       → (4,  "s")
          "Ctrl+Shift+P" → (10, "p")
          "Enter"        → (0,  "Enter")
        """
        parts = key_str.split("+")
        modifiers = 0
        main_key = parts[-1]
        for mod in parts[:-1]:
            modifiers |= cls._MODIFIER_MAP.get(mod.lower(), 0)
        return modifiers, main_key

    def press(
        self,
        key: str,
        ref_or_selector: str | None = None,
        target: str = "active",
        modifiers: int = 0,
    ) -> dict:
        """发送按键事件（使用 CDP Input.dispatchKeyEvent，兼容 Vue/React）。

        支持组合键语法，例如 "Meta+S"、"Ctrl+Shift+P"、"Alt+F4"。
        模拟真实键盘的完整事件序列：rawKeyDown → char（若有文字） → keyUp
        """
        # 解析组合键（优先级：字符串中的 "+" 语法 > modifiers 参数）
        if "+" in key:
            extra_mods, key = self._parse_key_combo(key)
            modifiers |= extra_mods

        if ref_or_selector:
            ref_info = self.resolve_ref(ref_or_selector)
            target_id = ref_info.get("target_id") or self.resolve_target(target)
            selector = ref_info["selector"]
        else:
            target_id = self.resolve_target(target)
            selector = None

        # 如果指定了元素，先聚焦
        if selector:
            self._evaluate(target_id, f"""
                (function() {{
                    var el = document.querySelector({json.dumps(selector)});
                    if (el) {{ el.focus(); }}
                }})()
            """)

        # 解析键名
        km = self._KEY_MAP.get(key)
        if km:
            k, code, kc, text = km["key"], km["code"], km["keyCode"], km["text"]
        else:
            # 单字符按键
            k = key
            code = f"Key{key.upper()}" if len(key) == 1 else key
            kc = ord(key.upper()) if len(key) == 1 else 0
            text = key if len(key) == 1 else ""

        # 组合键按下修饰符时，普通字母不发送 char 事件（防止意外输入）
        if modifiers and text and len(text) == 1:
            text = ""

        # 真实键盘事件序列：rawKeyDown → char → keyUp
        base_params = {
            "type": "rawKeyDown",
            "key": k,
            "code": code,
            "windowsVirtualKeyCode": kc,
            "nativeVirtualKeyCode": kc,
            "modifiers": modifiers,
        }
        self.page_call(target_id, "Input.dispatchKeyEvent", base_params)
        if text:
            self.page_call(target_id, "Input.dispatchKeyEvent", {
                "type": "char",
                "key": k,
                "code": code,
                "text": text,
                "unmodifiedText": text,
                "windowsVirtualKeyCode": kc,
                "nativeVirtualKeyCode": kc,
                "modifiers": modifiers,
            })
        self.page_call(target_id, "Input.dispatchKeyEvent", {
            "type": "keyUp",
            "key": k,
            "code": code,
            "windowsVirtualKeyCode": kc,
            "nativeVirtualKeyCode": kc,
            "modifiers": modifiers,
        })
        orig_key = "+".join([m for m in ["Meta" if modifiers & 4 else "",
                                          "Ctrl" if modifiers & 2 else "",
                                          "Alt" if modifiers & 1 else "",
                                          "Shift" if modifiers & 8 else ""]
                              if m] + [key]) if modifiers else key
        return {"ok": True, "key": orig_key, "modifiers": modifiers}

    def scroll(
        self,
        direction: str = "down",
        amount: int = 500,
        ref_or_selector: str | None = None,
        at: tuple[int, int] | None = None,
        target: str = "active",
    ) -> dict:
        """滚动页面或指定区域。

        使用 CDP Input.dispatchMouseEvent mouseWheel 模拟真实鼠标滚轮，
        浏览器根据鼠标坐标自动判定滚动哪个容器。

        定位策略（优先级从高到低）：
          1. at=(x, y)       — 在指定坐标滚动
          2. @ref / selector  — 在元素中心位置滚动
          3. 默认              — 在视口中心滚动（主内容区域）
        """
        target_id = self.resolve_target(target)

        # 确定鼠标坐标
        if at:
            x, y = at
        elif ref_or_selector:
            ref_info = self.resolve_ref(ref_or_selector)
            sel = ref_info["selector"]
            # 获取元素中心坐标
            rect_data = self._evaluate(target_id, f"""
                (function() {{
                    var el = document.querySelector({json.dumps(sel)});
                    if (!el) return JSON.stringify({{error: 'not found'}});
                    var r = el.getBoundingClientRect();
                    return JSON.stringify({{x: Math.round(r.x + r.width/2), y: Math.round(r.y + r.height/2)}});
                }})()
            """)
            rect = json.loads(rect_data) if isinstance(rect_data, str) else rect_data
            if "error" in rect:
                raise RuntimeError(rect["error"])
            x, y = rect["x"], rect["y"]
        else:
            # 默认视口中心
            vp = self._evaluate(target_id,
                "JSON.stringify({w: window.innerWidth, h: window.innerHeight})")
            vp = json.loads(vp) if isinstance(vp, str) else vp
            x, y = vp["w"] // 2, vp["h"] // 2

        # 方向 → deltaX/deltaY
        dx, dy = 0, 0
        if direction == "down":
            dy = amount
        elif direction == "up":
            dy = -amount
        elif direction == "right":
            dx = amount
        elif direction == "left":
            dx = -amount

        # CDP 鼠标滚轮事件
        self.page_call(target_id, "Input.dispatchMouseEvent", {
            "type": "mouseWheel",
            "x": x,
            "y": y,
            "deltaX": dx,
            "deltaY": dy,
        })
        return {"ok": True, "direction": direction, "amount": amount,
                "at": [x, y]}

    def drag(
        self,
        start_x: int,
        start_y: int,
        end_x: int,
        end_y: int,
        target: str = "active",
        steps: int = 10,
        hold_ms: int = 100,
    ) -> dict:
        """鼠标拖拽（mousePressed → mouseMoved*N → mouseReleased）。

        :param start_x, start_y: 起点坐标
        :param end_x, end_y: 终点坐标
        :param steps: 移动分几步完成（步数越多越平滑）
        :param hold_ms: 按下后等待的毫秒数（某些拖拽库需要长按识别）
        """
        import time as _time

        target_id = self.resolve_target(target)

        def _dispatch(evt_type: str, x: int, y: int, **kw: Any) -> None:
            self.page_call(target_id, "Input.dispatchMouseEvent", {
                "type": evt_type, "x": x, "y": y, "button": "left", **kw,
            })

        # 1. 移到起点
        _dispatch("mouseMoved", start_x, start_y)
        _time.sleep(0.05)

        # 2. 按下
        _dispatch("mousePressed", start_x, start_y, clickCount=1)
        _time.sleep(hold_ms / 1000.0)

        # 3. 分步移动
        for i in range(1, steps + 1):
            cx = start_x + (end_x - start_x) * i // steps
            cy = start_y + (end_y - start_y) * i // steps
            _dispatch("mouseMoved", cx, cy)
            _time.sleep(0.02)

        # 4. 释放
        _dispatch("mouseReleased", end_x, end_y, clickCount=1)

        return {
            "ok": True,
            "from": [start_x, start_y],
            "to": [end_x, end_y],
            "steps": steps,
        }

    def wait_for(
        self,
        selector: str | None = None,
        text: str | None = None,
        timeout_ms: int = 10000,
        target: str = "active",
    ) -> dict:
        """等待元素或文本出现，轮询模式。"""
        target_id = self.resolve_target(target)
        sel_arg = json.dumps(selector) if selector else "null"
        text_arg = json.dumps(text) if text else "null"
        expr = f'({JS_WAIT_FOR})({sel_arg}, {text_arg})'
        start = time.time()
        deadline = start + timeout_ms / 1000
        last_data: dict = {}
        while time.time() < deadline:
            data = self._evaluate_json(target_id, expr)
            if data.get("found"):
                return {**data, "waited_ms": int((time.time() - start) * 1000)}
            last_data = data
            time.sleep(0.3)
        return {**last_data, "timeout": True, "waited_ms": timeout_ms}

    def get_text(
        self,
        ref_or_selector: str | None = None,
        target: str = "active",
    ) -> str:
        """获取元素或整个页面文本。"""
        if ref_or_selector and ref_or_selector.startswith("@e"):
            ref_info = self.resolve_ref(ref_or_selector)
            target_id = ref_info.get("target_id") or self.resolve_target(target)
            selector = ref_info["selector"]
        else:
            target_id = self.resolve_target(target)
            selector = ref_or_selector
        sel_arg = json.dumps(selector) if selector else "'body'"
        expr = f'({JS_GET_TEXT})({sel_arg})'
        raw = self._evaluate(target_id, expr)
        if isinstance(raw, str):
            try:
                data = json.loads(raw)
                if isinstance(data, dict) and "error" in data:
                    raise RuntimeError(data["error"])
            except (json.JSONDecodeError, TypeError):
                pass
            return raw
        return str(raw) if raw else ""

    def get_url(self, target: str = "active") -> str:
        """获取页面 URL。"""
        target_id = self.resolve_target(target)
        return self._evaluate(target_id, "location.href") or ""

    def get_title(self, target: str = "active") -> str:
        """获取页面标题。"""
        target_id = self.resolve_target(target)
        return self._evaluate(target_id, "document.title") or ""

    def screenshot(
        self,
        target: str = "active",
        path: str = "",
        annotate: bool = False,
        full_page: bool = False,
    ) -> dict:
        """保存页面截图。

        annotate=True 时先扫描交互元素，在页面上临时叠加编号标签。
        标签编号与 @eN 引用一致，截图后可直接使用这些 ref 继续交互。
        """
        target_id = self.resolve_target(target)
        if not path:
            path = str(Path(tempfile.gettempdir()) / f"cdp_screenshot_{int(time.time() * 1000)}.png")
        out_path = Path(path).expanduser()
        if not out_path.is_absolute():
            out_path = Path.cwd() / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)

        legend: list[dict[str, Any]] = []
        overlay_id = "__cdp_daemon_annotate_overlay__"

        def _remove_overlay() -> None:
            try:
                self._evaluate(target_id, f"""
                    (() => {{
                        const old = document.getElementById({json.dumps(overlay_id)});
                        if (old) old.remove();
                    }})()
                """)
            except Exception:
                pass

        try:
            if annotate:
                snap = self.snapshot(target=target_id, include_cursor=True)
                elements = snap.get("elements", [])
                labels = []
                for idx, el in enumerate(elements, 1):
                    rect = el.get("rect") or {}
                    x = int(rect.get("x", 0) or 0)
                    y = int(rect.get("y", 0) or 0)
                    w = int(rect.get("w", 0) or 0)
                    h = int(rect.get("h", 0) or 0)
                    if w <= 0 and h <= 0:
                        continue
                    labels.append({"index": idx, "x": x, "y": y, "w": w, "h": h})
                    legend.append({"index": idx, "ref": el.get("ref"), "desc": el.get("desc", "")})
                self._evaluate(target_id, f"""
                    (() => {{
                        const old = document.getElementById({json.dumps(overlay_id)});
                        if (old) old.remove();
                        const root = document.createElement('div');
                        root.id = {json.dumps(overlay_id)};
                        root.style.cssText = 'position:fixed;left:0;top:0;z-index:2147483647;pointer-events:none;font-family:Arial,sans-serif;';
                        const labels = {json.dumps(labels)};
                        for (const item of labels) {{
                            const box = document.createElement('div');
                            box.textContent = '[' + item.index + ']';
                            box.style.cssText = [
                                'position:fixed',
                                'left:' + Math.max(0, item.x) + 'px',
                                'top:' + Math.max(0, item.y) + 'px',
                                'background:#ff3b30',
                                'color:#fff',
                                'border:1px solid #fff',
                                'border-radius:3px',
                                'padding:1px 4px',
                                'font-size:12px',
                                'font-weight:bold',
                                'line-height:16px',
                                'box-shadow:0 1px 3px rgba(0,0,0,.45)'
                            ].join(';');
                            const outline = document.createElement('div');
                            outline.style.cssText = [
                                'position:fixed',
                                'left:' + Math.max(0, item.x) + 'px',
                                'top:' + Math.max(0, item.y) + 'px',
                                'width:' + Math.max(1, item.w) + 'px',
                                'height:' + Math.max(1, item.h) + 'px',
                                'border:2px solid #ff3b30',
                                'box-sizing:border-box',
                                'border-radius:3px'
                            ].join(';');
                            root.appendChild(outline);
                            root.appendChild(box);
                        }}
                        document.documentElement.appendChild(root);
                    }})()
                """)

            params: dict[str, Any] = {"format": "png", "fromSurface": True}
            if full_page:
                metrics = self.page_call(target_id, "Page.getLayoutMetrics", {})
                content = metrics.get("contentSize", {})
                width = max(1, int(content.get("width", 0) or 0))
                height = max(1, int(content.get("height", 0) or 0))
                if width and height:
                    params["captureBeyondViewport"] = True
                    params["clip"] = {"x": 0, "y": 0, "width": width, "height": height, "scale": 1}
            captured = self.page_call(target_id, "Page.captureScreenshot", params)
            data = captured.get("data", "")
            if not data:
                raise RuntimeError("Page.captureScreenshot returned empty data")
            out_path.write_bytes(base64.b64decode(data))
            return {
                "ok": True,
                "path": str(out_path),
                "target_id": target_id,
                "annotate": annotate,
                "legend": legend,
            }
        finally:
            if annotate:
                _remove_overlay()

    @staticmethod
    def _png_pixel_stats(data: bytes, max_samples: int = 50000) -> dict[str, Any]:
        """解析 Chrome PNG 截图并采样统计非白像素比例。"""
        if not data.startswith(b"\x89PNG\r\n\x1a\n"):
            return {"ok": False, "error": "not a png"}

        pos = 8
        width = height = bit_depth = color_type = 0
        idat = bytearray()
        while pos + 8 <= len(data):
            length = struct.unpack(">I", data[pos:pos + 4])[0]
            ctype = data[pos + 4:pos + 8]
            chunk = data[pos + 8:pos + 8 + length]
            pos += 12 + length
            if ctype == b"IHDR":
                width, height, bit_depth, color_type, _, _, _ = struct.unpack(">IIBBBBB", chunk)
            elif ctype == b"IDAT":
                idat.extend(chunk)
            elif ctype == b"IEND":
                break

        if bit_depth != 8 or color_type not in (2, 6) or width <= 0 or height <= 0:
            return {"ok": False, "error": f"unsupported png: bit_depth={bit_depth} color_type={color_type}"}

        channels = 4 if color_type == 6 else 3
        stride = width * channels
        raw = zlib.decompress(bytes(idat))
        rows: list[bytearray] = []
        offset = 0
        prev = bytearray(stride)
        for _ in range(height):
            filter_type = raw[offset]
            offset += 1
            scan = bytearray(raw[offset:offset + stride])
            offset += stride
            for i in range(stride):
                left = scan[i - channels] if i >= channels else 0
                up = prev[i]
                up_left = prev[i - channels] if i >= channels else 0
                if filter_type == 1:
                    scan[i] = (scan[i] + left) & 0xFF
                elif filter_type == 2:
                    scan[i] = (scan[i] + up) & 0xFF
                elif filter_type == 3:
                    scan[i] = (scan[i] + ((left + up) // 2)) & 0xFF
                elif filter_type == 4:
                    p = left + up - up_left
                    pa, pb, pc = abs(p - left), abs(p - up), abs(p - up_left)
                    pr = left if pa <= pb and pa <= pc else (up if pb <= pc else up_left)
                    scan[i] = (scan[i] + pr) & 0xFF
            rows.append(scan)
            prev = scan

        total_pixels = width * height
        step = max(1, total_pixels // max_samples)
        sampled = non_white = transparent = 0
        colors: set[tuple[int, int, int]] = set()
        brightness_sum = 0.0
        idx = 0
        for y, row in enumerate(rows):
            for x in range(width):
                if idx % step != 0:
                    idx += 1
                    continue
                base = x * channels
                r, g, b = row[base], row[base + 1], row[base + 2]
                a = row[base + 3] if channels == 4 else 255
                sampled += 1
                if a == 0:
                    transparent += 1
                if a > 0 and not (r >= 248 and g >= 248 and b >= 248):
                    non_white += 1
                colors.add((r // 16, g // 16, b // 16))
                brightness_sum += (r + g + b) / 3
                idx += 1

        return {
            "ok": True,
            "width": width,
            "height": height,
            "sampled": sampled,
            "non_white_ratio": round(non_white / sampled, 6) if sampled else 0,
            "transparent_ratio": round(transparent / sampled, 6) if sampled else 0,
            "distinct_color_buckets": len(colors),
            "avg_brightness": round(brightness_sum / sampled, 2) if sampled else 0,
        }

    def diagnose_page(
        self,
        target: str = "active",
        path: str = "",
        full_page: bool = False,
        wait_ms: int = 0,
    ) -> dict:
        """诊断页面是否白屏、图表是否有可见 DOM，并保存截图。"""
        target_id = self.resolve_target(target)
        if wait_ms > 0:
            time.sleep(max(0, wait_ms) / 1000)

        dom = self._evaluate_json(target_id, r"""
            (() => {
              const visible = (el) => {
                if (!el) return false;
                const r = el.getBoundingClientRect();
                const s = getComputedStyle(el);
                return r.width > 0 && r.height > 0 && s.display !== 'none'
                  && s.visibility !== 'hidden' && Number(s.opacity || 1) > 0;
              };
              const rect = (el) => {
                const r = el.getBoundingClientRect();
                return {x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), h: Math.round(r.height)};
              };
              const chartSelectors = [
                'canvas', 'svg',
                '[class*="echart"]', '[class*="antv"]', '[class*="chart"]',
                '[class*="g2"]', '[class*="s2"]', '.wrapper-outer'
              ];
              const chartNodes = Array.from(document.querySelectorAll(chartSelectors.join(',')))
                .filter(visible)
                .slice(0, 80)
                .map(el => ({
                  tag: el.tagName,
                  className: String(el.className || '').slice(0, 120),
                  text: (el.innerText || el.textContent || '').trim().slice(0, 120),
                  rect: rect(el)
                }));
              const normalizeText = (text) => String(text || '').replace(/\s+/g, ' ').trim();
              const textBlocks = Array.from(document.querySelectorAll('h1,h2,h3,h4,h5,h6,p,span,div,label,td,th,button,a,[role="heading"],[role="status"],[role="alert"]'))
                .filter(visible)
                .map(el => ({el, text: normalizeText(el.innerText || el.textContent), box: rect(el)}))
                .filter(item => item.text && item.text.length <= 180 && item.box.w > 0 && item.box.h > 0 && item.box.h <= 220 && item.box.w <= innerWidth * 0.98)
                .slice(0, 120)
                .map(item => ({
                  tag: item.el.tagName,
                  role: item.el.getAttribute('role') || '',
                  className: String(item.el.className || '').slice(0, 100),
                  text: item.text.slice(0, 180),
                  rect: item.box
                }));
              const numericTextBlocks = textBlocks
                .filter(item => /[-+]?\d[\d,]*(\.\d+)?%?/.test(item.text))
                .slice(0, 80);
              const emptySelectors = [
                '[role="table"]', '[role="grid"]', '[role="list"]', '[role="tabpanel"]',
                '[class*="card"]', '[class*="panel"]', '[class*="content"]',
                '[class*="chart"]', '[class*="table"]', '[data-testid]'
              ];
              const emptyContentCandidates = Array.from(document.querySelectorAll(emptySelectors.join(',')))
                .filter(visible)
                .map(el => ({el, text: normalizeText(el.innerText || el.textContent), box: rect(el)}))
                .filter(item => item.box.w >= 80 && item.box.h >= 40 && !item.text
                  && !item.el.querySelector('canvas,svg,img,video,input,textarea,select'))
                .slice(0, 80)
                .map(item => ({
                  tag: item.el.tagName,
                  role: item.el.getAttribute('role') || '',
                  ariaLabel: item.el.getAttribute('aria-label') || '',
                  title: item.el.getAttribute('title') || '',
                  testId: item.el.getAttribute('data-testid') || '',
                  className: String(item.el.className || '').slice(0, 120),
                  rect: item.box
                }));
              const text = document.body ? (document.body.innerText || '') : '';
              const resources = performance.getEntriesByType('resource')
                .filter(e => e.initiatorType === 'script' || e.initiatorType === 'img' || e.initiatorType === 'fetch' || e.initiatorType === 'xmlhttprequest')
                .slice(-80)
                .map(e => ({name: e.name, type: e.initiatorType, duration: Math.round(e.duration || 0), transferSize: e.transferSize || 0}));
              return JSON.stringify({
                url: location.href,
                title: document.title,
                readyState: document.readyState,
                bodyTextLength: text.length,
                bodyTextSample: text.slice(0, 500),
                viewport: {w: innerWidth, h: innerHeight, dpr: devicePixelRatio},
                scroll: {x: scrollX, y: scrollY, w: document.documentElement.scrollWidth, h: document.documentElement.scrollHeight},
                visibleCanvas: Array.from(document.querySelectorAll('canvas')).filter(visible).length,
                visibleSvg: Array.from(document.querySelectorAll('svg')).filter(visible).length,
                chartCandidates: chartNodes,
                textBlocks,
                numericTextBlocks,
                emptyContentCandidates,
                resourceTail: resources
              });
            })()
        """)

        if not path:
            path = str(Path(tempfile.gettempdir()) / f"cdp_diagnose_{int(time.time() * 1000)}.png")
        out_path = Path(path).expanduser()
        if not out_path.is_absolute():
            out_path = Path.cwd() / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)

        params: dict[str, Any] = {"format": "png", "fromSurface": True}
        if full_page:
            metrics = self.page_call(target_id, "Page.getLayoutMetrics", {})
            content = metrics.get("contentSize", {})
            width = max(1, int(content.get("width", 0) or 0))
            height = max(1, int(content.get("height", 0) or 0))
            params["captureBeyondViewport"] = True
            params["clip"] = {"x": 0, "y": 0, "width": width, "height": height, "scale": 1}
        captured = self.page_call(target_id, "Page.captureScreenshot", params)
        png_bytes = base64.b64decode(captured.get("data", ""))
        out_path.write_bytes(png_bytes)
        pixel_stats = self._png_pixel_stats(png_bytes)

        conclusions: list[str] = []
        non_white = pixel_stats.get("non_white_ratio", 0) if pixel_stats.get("ok") else 0
        if non_white < 0.01:
            conclusions.append("截图几乎全白，疑似白屏或截图模式异常")
        if dom.get("bodyTextLength", 0) > 0 and non_white < 0.01:
            conclusions.append("DOM 有文本但截图白，优先检查 full-page 截图模式或遮罩层")
        if not dom.get("chartCandidates"):
            conclusions.append("未发现可见图表候选 DOM")
        elif dom.get("visibleCanvas", 0) + dom.get("visibleSvg", 0) == 0:
            conclusions.append("发现图表容器但没有可见 canvas/svg，可能仍在加载或图表渲染失败")
        else:
            conclusions.append("发现可见图表 DOM，截图像素可用于二次确认")
        if dom.get("emptyContentCandidates"):
            conclusions.append(f"发现 {len(dom.get('emptyContentCandidates') or [])} 个可见但内容为空的通用容器候选")
        if dom.get("bodyTextLength", 0) > 0 and not dom.get("textBlocks"):
            conclusions.append("页面有 body 文本但未提取到稳定可见文本块，可能存在遮罩、虚拟滚动或文本在 Shadow DOM 中")

        return {
            "ok": True,
            "target_id": target_id,
            "screenshot": str(out_path),
            "full_page": full_page,
            "dom": dom,
            "pixel_stats": pixel_stats,
            "conclusions": conclusions,
        }

    def activate(self, target: str = "active") -> dict:
        """将指定 tab 切换到前台（Target.activateTarget）。"""
        target_id = self.resolve_target(target)
        self._cdp.call("Target.activateTarget", {"targetId": target_id})
        title = self._evaluate(target_id, "document.title") or ""
        return {"ok": True, "targetId": target_id, "title": title}

    def open_tab(
        self,
        url: str,
        wait_ms: int = 3000,
        activate: bool = True,
        group: str = "",
        alias: str = "",
        requested_group: str = "",
    ) -> dict:
        """打开新标签页并等待加载。

        :param url: 要打开的 URL
        :param wait_ms: 等待页面加载的毫秒数
        :param activate: 是否切换到新 tab（默认 True）
        :param group: 保留兼容参数；CDP 新开页必须进入固定自动化分组
        :param requested_group: 用户传入但被忽略的自定义分组名
        """
        result = self._cdp.call("Target.createTarget", {"url": url})
        target_id = result.get("targetId", "")
        if not target_id:
            raise RuntimeError(f"createTarget 失败: {result}")

        # 等待页面加载
        import time as _time
        deadline = _time.time() + wait_ms / 1000
        title = ""
        url_final = url
        while _time.time() < deadline:
            _time.sleep(0.3)
            try:
                title = self._evaluate(target_id, "document.title") or ""
                current_url = self._evaluate(target_id, "location.href") or ""
                if current_url and (current_url != "about:blank" or url == "about:blank"):
                    url_final = current_url
                if title or (current_url and current_url != "about:blank"):
                    break
            except Exception:
                pass

        resp = {
            "ok": True,
            "targetId": target_id,
            "title": title,
            "url": url_final,
        }

        # CDP 新建页面必须移入固定自动化分组，失败则关闭页面，避免留下未分组自动化 tab。
        effective_group = self._automation_group_name
        if requested_group and requested_group != effective_group:
            resp["requested_group_ignored"] = requested_group
        elif group and group != effective_group:
            resp["requested_group_ignored"] = group
        try:
            move_result = self._ensure_automation_group(target_id)
            resp["group"] = effective_group
            resp["group_ok"] = move_result.get("ok", False)
            if not move_result.get("ok", False):
                group_error = move_result.get("error", "")
                try:
                    self._cdp.call("Target.closeTarget", {"targetId": target_id})
                except Exception:
                    pass
                return {
                    "ok": False,
                    "error": f"open_tab 被阻止：无法加入固定分组 {effective_group}: {group_error}",
                    "targetId": target_id,
                    "target_closed": True,
                    "group": effective_group,
                    "group_error": group_error,
                }
        except Exception as exc:
            try:
                self._cdp.call("Target.closeTarget", {"targetId": target_id})
            except Exception:
                pass
            return {
                "ok": False,
                "error": f"open_tab 被阻止：无法加入固定分组 {effective_group}: {exc}",
                "targetId": target_id,
                "target_closed": True,
                "group": effective_group,
                "group_error": str(exc),
            }

        if activate:
            try:
                self._cdp.call("Target.activateTarget", {"targetId": target_id})
            except Exception:
                pass

        if alias:
            bind_result = self.bind_tab(alias, target=target_id)
            resp["alias"] = bind_result.get("alias", "")

        return resp

    def close_tab(self, target: str = "active") -> dict:
        """关闭指定标签页。"""
        target_id = self.resolve_target(target)
        # 先获取信息
        try:
            title = self._evaluate(target_id, "document.title") or ""
        except Exception:
            title = ""
        self._cdp.call("Target.closeTarget", {"targetId": target_id})
        # 清理 session 缓存
        self._invalidate_session(target_id)
        removed_aliases = self._remove_aliases_for_target(target_id)
        for g in self._tab_groups.values():
            g["targets"] = [t for t in g.get("targets", []) if t != target_id]
        # 等待 targetDestroyed 事件刷新 pages 缓存
        import time as _time
        _time.sleep(0.2)
        try:
            self._cdp.call("Target.getTargets")
        except Exception:
            pass
        return {"ok": True, "targetId": target_id, "title": title, "removed_aliases": removed_aliases}

    # =================================================================
    # 标签页群组管理（Chrome 原生 Tab Groups）
    # =================================================================

    def _find_extension_sw(self) -> None:
        """查找可用的 Chrome 扩展 service worker（有 chrome.tabs.group 权限的）。

        缓存结果到 self._ext_sw_tabs / self._ext_sw_tabgroups。
        """
        if self._ext_sw_tabs:
            # 验证缓存是否还有效
            try:
                self.page_call(self._ext_sw_tabs, "Runtime.evaluate",
                    {"expression": "1", "returnByValue": True})
                return
            except Exception:
                self._ext_sw_tabs = ""
                self._ext_sw_tabgroups = ""

        all_targets = self._cdp.call("Target.getTargets")
        targets = all_targets.get("targetInfos", [])
        for t in targets:
            if t.get("type") != "service_worker":
                continue
            if "chrome-extension:" not in t.get("url", ""):
                continue
            tid = t["targetId"]
            try:
                resp = self.page_call(tid, "Runtime.evaluate", {
                    "expression": "JSON.stringify({tabs: typeof chrome.tabs?.group, tabGroups: typeof chrome.tabGroups?.update})",
                    "returnByValue": True,
                })
                val = json.loads(resp.get("result", {}).get("value", "{}"))
                if val.get("tabs") == "function" and not self._ext_sw_tabs:
                    self._ext_sw_tabs = tid
                if val.get("tabGroups") == "function" and not self._ext_sw_tabgroups:
                    self._ext_sw_tabgroups = tid
                if self._ext_sw_tabs and self._ext_sw_tabgroups:
                    break
            except Exception:
                continue
        # 如果 tabGroups 未找到但 tabs 有，也能用（只是无法设置名称/颜色）
        if not self._ext_sw_tabs:
            raise RuntimeError(
                "未找到可用的 Chrome 扩展（需要有 tabs 权限的扩展）。"
                "请确保至少安装了一个带 tabs 权限的 Chrome 扩展。"
            )

    def _ext_eval(self, target_id: str, expression: str) -> Any:
        """在扩展 service worker 上执行 JS 并返回解析后的值。"""
        resp = self.page_call(target_id, "Runtime.evaluate", {
            "expression": expression,
            "returnByValue": True,
            "awaitPromise": True,
        })
        val = resp.get("result", {}).get("value", "")
        if not val:
            return {}
        return json.loads(val) if isinstance(val, str) else val

    def _chrome_tab_ids_from_targets(self, target_ids: list[str]) -> list[int]:
        """通过扩展 API 将 CDP targetId 列表映射为 Chrome 内部 tab ID。"""
        self._find_extension_sw()
        # 获取所有 tabs，匹配 URL
        urls = []
        for tid in target_ids:
            try:
                url = self._evaluate(tid, "location.href") or ""
                if url:
                    urls.append((tid, url))
            except Exception:
                pass
        if not urls:
            return []

        all_tabs = self._ext_eval(self._ext_sw_tabs,
            "chrome.tabs.query({}).then(tabs => JSON.stringify(tabs.map(t => ({id: t.id, url: t.url}))))"
        )
        if not isinstance(all_tabs, list):
            return []

        # URL → chrome tab id 映射
        url_to_id: dict[str, int] = {}
        for t in all_tabs:
            url_to_id[t.get("url", "")] = t.get("id", 0)

        result = []
        for tid, url in urls:
            chrome_id = url_to_id.get(url, 0)
            if chrome_id:
                result.append(chrome_id)
        return result

    def _verify_target_in_chrome_group(self, target_id: str, group_name: str) -> dict:
        """反查 Chrome 原生标签组，确认目标 tab 确实进入指定组。"""
        self._find_extension_sw()
        tab_ids = self._chrome_tab_ids_from_targets([target_id])
        if not tab_ids:
            return {"ok": False, "error": "无法将 targetId 映射到 Chrome tab ID"}

        tab_id = tab_ids[0]
        tab_info = self._ext_eval(
            self._ext_sw_tabs,
            f"chrome.tabs.get({tab_id}).then(t => JSON.stringify({{id:t.id, groupId:t.groupId, url:t.url, title:t.title}}))",
        )
        group_id = tab_info.get("groupId")
        if group_id is None or int(group_id) < 0:
            return {"ok": False, "error": "Chrome tab 当前未加入任何原生标签组", "tab_id": tab_id}

        expected_group_id = (self._tab_groups.get(group_name) or {}).get("chrome_group_id")
        if expected_group_id is not None and int(group_id) != int(expected_group_id):
            return {
                "ok": False,
                "error": f"Chrome tab groupId={group_id} 与内存记录 groupId={expected_group_id} 不一致",
                "tab_id": tab_id,
                "chrome_group_id": group_id,
            }

        title = ""
        color = ""
        if self._ext_sw_tabgroups:
            try:
                group_info = self._ext_eval(
                    self._ext_sw_tabgroups,
                    f"chrome.tabGroups.get({group_id}).then(g => JSON.stringify({{id:g.id, title:g.title, color:g.color}}))",
                )
                title = str(group_info.get("title") or "")
                color = str(group_info.get("color") or "")
                if title != group_name:
                    return {
                        "ok": False,
                        "error": f"Chrome 原生标签组名称不匹配: actual={title!r}, expected={group_name!r}",
                        "tab_id": tab_id,
                        "chrome_group_id": group_id,
                    }
            except Exception as exc:
                return {"ok": False, "error": f"读取 Chrome 标签组信息失败: {exc}", "tab_id": tab_id}

        # 反查成功后同步内存中的 group id，避免 daemon 重启后复用已有组时信息漂移。
        if group_name in self._tab_groups:
            self._tab_groups[group_name]["chrome_group_id"] = int(group_id)
        return {
            "ok": True,
            "tab_id": tab_id,
            "chrome_group_id": int(group_id),
            "title": title,
            "color": color,
        }

    def _resolve_targets(self, targets: list[str]) -> list[str]:
        """批量解析 target 标识为 targetId 列表。

        url: 前缀支持匹配多个页面（去重）。
        """
        resolved = []
        seen = set()
        pages = self._cdp.get_pages()
        for t in targets:
            if t.startswith("url:"):
                keyword = t[4:].strip().lower()
                for p in pages:
                    tid = p.get("targetId", "")
                    if keyword in p.get("url", "").lower() and tid not in seen:
                        resolved.append(tid)
                        seen.add(tid)
            else:
                try:
                    tid = self.resolve_target(t)
                    if tid not in seen:
                        resolved.append(tid)
                        seen.add(tid)
                except Exception:
                    pass
        return resolved

    def _get_page_info(self, target_id: str) -> dict:
        """获取 page 的 title + url（容错）。"""
        try:
            title = self._evaluate(target_id, "document.title") or ""
        except Exception:
            title = ""
        try:
            url = self._evaluate(target_id, "location.href") or ""
        except Exception:
            url = ""
        return {"targetId": target_id, "title": title, "url": url}

    def _find_chrome_group_id_by_name(self, name: str) -> int | None:
        """按组名查找已有 Chrome 原生标签组，供 daemon 重启后复用。"""
        self._find_extension_sw()
        if not self._ext_sw_tabgroups:
            return None
        try:
            groups = self._ext_eval(
                self._ext_sw_tabgroups,
                f"chrome.tabGroups.query({{title: {json.dumps(name)}}}).then(gs => JSON.stringify(gs.map(g => ({{id: g.id, title: g.title, color: g.color}}))))",
            )
        except Exception:
            return None
        if isinstance(groups, list) and groups:
            gid = groups[0].get("id")
            try:
                return int(gid)
            except Exception:
                return None
        return None

    def _ensure_automation_group(self, target_id: str) -> dict:
        """确保 CDP 新开标签页进入固定自动化分组。"""
        name = self._automation_group_name
        color = self._automation_group_color
        last_result: dict[str, Any] = {"ok": False, "error": "unknown"}
        for attempt in range(4):
            if name in self._tab_groups:
                last_result = self.group_move(name, [target_id])
            else:
                existing_gid = self._find_chrome_group_id_by_name(name)
                if existing_gid is not None:
                    self._tab_groups[name] = {
                        "color": color,
                        "targets": [],
                        "chrome_group_id": existing_gid,
                    }
                    last_result = self.group_move(name, [target_id])
                else:
                    last_result = self.group_create(name, [target_id], color)

            if last_result.get("ok"):
                verify_result = self._verify_target_in_chrome_group(target_id, name)
                if verify_result.get("ok"):
                    last_result["verified"] = verify_result
                    return last_result
                last_result = {
                    "ok": False,
                    "error": f"固定分组反查失败: {verify_result.get('error', 'unknown')}",
                    "verify": verify_result,
                }
            if "无法将 targetId 映射到 Chrome tab ID" not in str(last_result.get("error", "")):
                return last_result
            time.sleep(0.25 * (attempt + 1))

        return last_result

    def group_create(self, name: str, targets: list[str] | None = None, color: str = "blue") -> dict:
        """创建 Chrome 原生标签页群组。

        :param name: 群组名称（显示在 Chrome 标签栏）
        :param targets: 要添加的 target 标识列表（targetId/前缀/url:关键词）
        :param color: 群组颜色（grey/blue/red/yellow/green/pink/purple/cyan/orange）
        """
        if name in self._tab_groups:
            return {"ok": False, "error": f"群组 '{name}' 已存在，用 group add 添加标签页"}
        resolved = self._resolve_targets(targets) if targets else []
        if not resolved:
            return {"ok": False, "error": "至少需要一个标签页"}

        # 通过扩展 API 创建 Chrome 原生 Tab Group
        self._find_extension_sw()
        chrome_tab_ids = self._chrome_tab_ids_from_targets(resolved)
        if not chrome_tab_ids:
            return {"ok": False, "error": "无法将 targetId 映射到 Chrome tab ID"}

        # chrome.tabs.group() 创建分组
        group_result = self._ext_eval(self._ext_sw_tabs,
            f"chrome.tabs.group({{tabIds: {json.dumps(chrome_tab_ids)}}}).then(gid => JSON.stringify({{groupId: gid}}))"
        )
        chrome_group_id = group_result.get("groupId")
        if chrome_group_id is None:
            return {"ok": False, "error": f"chrome.tabs.group 失败: {group_result}"}

        # chrome.tabGroups.update() 设置名称和颜色
        if self._ext_sw_tabgroups:
            try:
                self._ext_eval(self._ext_sw_tabgroups,
                    f"chrome.tabGroups.update({chrome_group_id}, {{title: {json.dumps(name)}, color: {json.dumps(color)}}}).then(() => JSON.stringify({{ok:true}}))"
                )
            except Exception:
                pass  # 设置名称失败不阻断

        self._tab_groups[name] = {"color": color, "targets": resolved, "chrome_group_id": chrome_group_id}
        tabs = [self._get_page_info(t) for t in resolved]
        return {"ok": True, "name": name, "color": color, "count": len(resolved),
                "chrome_group_id": chrome_group_id, "tabs": tabs}

    def group_add(self, name: str, targets: list[str]) -> dict:
        """向已有群组添加标签页。"""
        if name not in self._tab_groups:
            return {"ok": False, "error": f"群组 '{name}' 不存在，用 group create 先创建"}
        resolved = self._resolve_targets(targets)
        existing = set(self._tab_groups[name]["targets"])
        added = [t for t in resolved if t not in existing]
        if not added:
            return {"ok": True, "name": name, "added": 0, "total": len(existing)}

        # Chrome 原生 API: 添加到已有 group
        chrome_group_id = self._tab_groups[name].get("chrome_group_id")
        if chrome_group_id is not None:
            self._find_extension_sw()
            new_tab_ids = self._chrome_tab_ids_from_targets(added)
            if len(new_tab_ids) != len(added):
                return {"ok": False, "error": "无法将所有 targetId 映射到 Chrome tab ID"}
            self._ext_eval(self._ext_sw_tabs,
                f"chrome.tabs.group({{tabIds: {json.dumps(new_tab_ids)}, groupId: {chrome_group_id}}}).then(gid => JSON.stringify({{ok:true, groupId:gid}}))"
            )
            for tid in added:
                verify_result = self._verify_target_in_chrome_group(tid, name)
                if not verify_result.get("ok"):
                    return {"ok": False, "error": f"Chrome 原生分组反查失败: {verify_result.get('error')}", "verify": verify_result}

        self._tab_groups[name]["targets"].extend(added)
        tabs = [self._get_page_info(t) for t in added]
        total = len(self._tab_groups[name]["targets"])
        return {"ok": True, "name": name, "added": len(added), "total": total, "tabs": tabs}

    def group_remove_tab(self, name: str, targets: list[str]) -> dict:
        """从群组移除标签页（Chrome 里取消分组，不关闭标签页）。"""
        if name not in self._tab_groups:
            return {"ok": False, "error": f"群组 '{name}' 不存在"}
        resolved = set(self._resolve_targets(targets))

        # Chrome 原生 API: ungroup
        self._find_extension_sw()
        remove_tab_ids = self._chrome_tab_ids_from_targets(list(resolved))
        if remove_tab_ids:
            try:
                self._ext_eval(self._ext_sw_tabs,
                    f"chrome.tabs.ungroup({json.dumps(remove_tab_ids)}).then(() => JSON.stringify({{ok:true}}))"
                )
            except Exception:
                pass

        before = len(self._tab_groups[name]["targets"])
        self._tab_groups[name]["targets"] = [
            t for t in self._tab_groups[name]["targets"] if t not in resolved
        ]
        removed = before - len(self._tab_groups[name]["targets"])
        return {"ok": True, "name": name, "removed": removed,
                "remaining": len(self._tab_groups[name]["targets"])}

    def group_list(self, name: str = "") -> dict:
        """列出群组及其标签页。name 为空时列出所有群组。"""
        live_ids = {p.get("targetId", "") for p in self._cdp.get_pages()}
        if name:
            if name not in self._tab_groups:
                return {"ok": False, "error": f"群组 '{name}' 不存在"}
            g = self._tab_groups[name]
            g["targets"] = [t for t in g.get("targets", []) if t in live_ids]
            tabs = [self._get_page_info(t) for t in g["targets"]]
            return {"ok": True, "groups": [{
                "name": name, "color": g["color"], "count": len(tabs), "tabs": tabs,
            }]}
        groups = []
        for gname, g in self._tab_groups.items():
            g["targets"] = [t for t in g.get("targets", []) if t in live_ids]
            tabs = [self._get_page_info(t) for t in g["targets"]]
            groups.append({
                "name": gname, "color": g["color"], "count": len(tabs), "tabs": tabs,
            })
        return {"ok": True, "groups": groups}

    def group_close(self, name: str) -> dict:
        """关闭群组内所有标签页并删除群组（Chrome 里同时移除分组）。"""
        if name not in self._tab_groups:
            return {"ok": False, "error": f"群组 '{name}' 不存在"}

        # Chrome 原生 API: 通过扩展关闭 group 内的 tabs
        chrome_group_id = self._tab_groups[name].get("chrome_group_id")
        targets = self._tab_groups[name]["targets"][:]

        closed = []
        for tid in targets:
            try:
                title = ""
                try:
                    title = self._evaluate(tid, "document.title") or ""
                except Exception:
                    pass
                self._cdp.call("Target.closeTarget", {"targetId": tid})
                self._invalidate_session(tid)
                closed.append({"targetId": tid, "title": title})
            except Exception as e:
                closed.append({"targetId": tid, "error": str(e)})
        del self._tab_groups[name]
        # 等待 targetDestroyed 事件
        import time as _time
        _time.sleep(0.3)
        try:
            self._cdp.call("Target.getTargets")
        except Exception:
            pass
        return {"ok": True, "name": name, "closed": len(closed), "tabs": closed}

    def group_delete(self, name: str) -> dict:
        """删除群组定义（Chrome 里取消分组，不关闭标签页）。"""
        if name not in self._tab_groups:
            return {"ok": False, "error": f"群组 '{name}' 不存在"}

        # Chrome 原生 API: ungroup 所有 tabs
        targets = self._tab_groups[name]["targets"]
        if targets:
            try:
                self._find_extension_sw()
                tab_ids = self._chrome_tab_ids_from_targets(targets)
                if tab_ids:
                    self._ext_eval(self._ext_sw_tabs,
                        f"chrome.tabs.ungroup({json.dumps(tab_ids)}).then(() => JSON.stringify({{ok:true}}))"
                    )
            except Exception:
                pass

        count = len(targets)
        del self._tab_groups[name]
        return {"ok": True, "name": name, "released": count}

    def group_activate(self, name: str) -> dict:
        """将群组内第一个标签页切到前台。"""
        if name not in self._tab_groups:
            return {"ok": False, "error": f"群组 '{name}' 不存在"}
        targets = self._tab_groups[name]["targets"]
        if not targets:
            return {"ok": False, "error": f"群组 '{name}' 内没有标签页"}
        tid = targets[0]
        self._cdp.call("Target.activateTarget", {"targetId": tid})
        title = ""
        try:
            title = self._evaluate(tid, "document.title") or ""
        except Exception:
            pass
        return {"ok": True, "name": name, "targetId": tid, "title": title}

    def group_move(self, name: str, targets: list[str]) -> dict:
        """将标签页移入指定群组（先从其它群组移出）。

        与 group_add 的区别：group_move 会先把 tab 从所有其它群组移除，
        确保一个 tab 只属于一个群组。
        """
        if name not in self._tab_groups:
            return {"ok": False, "error": f"群组 '{name}' 不存在，请先 group create"}
        resolved = self._resolve_targets(targets)
        if not resolved:
            return {"ok": False, "error": "无法解析任何有效 target"}

        resolved_set = set(resolved)

        # 从其它群组移除这些 tab
        for gname, g in self._tab_groups.items():
            if gname == name:
                continue
            before = len(g["targets"])
            g["targets"] = [t for t in g["targets"] if t not in resolved_set]
            if len(g["targets"]) < before:
                # 同步 Chrome 原生 ungroup（从旧组移出）
                try:
                    self._find_extension_sw()
                    removed_tids = [t for t in resolved if t not in set(g["targets"])]
                    tab_ids = self._chrome_tab_ids_from_targets(removed_tids)
                    if tab_ids:
                        self._ext_eval(self._ext_sw_tabs,
                            f"chrome.tabs.ungroup({json.dumps(tab_ids)}).then(() => JSON.stringify({{ok:true}}))"
                        )
                except Exception:
                    pass

        # 加入目标群组（去重）
        existing = set(self._tab_groups[name]["targets"])
        added = [t for t in resolved if t not in existing]

        # Chrome 原生 API: 移入目标 group
        chrome_group_id = self._tab_groups[name].get("chrome_group_id")
        if chrome_group_id is not None and added:
            self._find_extension_sw()
            new_tab_ids = self._chrome_tab_ids_from_targets(added)
            if len(new_tab_ids) != len(added):
                return {"ok": False, "error": "无法将所有 targetId 映射到 Chrome tab ID"}
            self._ext_eval(self._ext_sw_tabs,
                f"chrome.tabs.group({{tabIds: {json.dumps(new_tab_ids)}, groupId: {chrome_group_id}}}).then(gid => JSON.stringify({{ok:true, groupId:gid}}))"
            )
            for tid in added:
                verify_result = self._verify_target_in_chrome_group(tid, name)
                if not verify_result.get("ok"):
                    return {"ok": False, "error": f"Chrome 原生分组反查失败: {verify_result.get('error')}", "verify": verify_result}

        self._tab_groups[name]["targets"].extend(added)

        tabs = [self._get_page_info(t) for t in added]
        total = len(self._tab_groups[name]["targets"])
        return {"ok": True, "name": name, "moved": len(added), "total": total, "tabs": tabs}

    def group_close_tabs(self, name: str, targets: list[str]) -> dict:
        """关闭群组内指定的标签页（从群组移除并关闭）。"""
        if name not in self._tab_groups:
            return {"ok": False, "error": f"群组 '{name}' 不存在"}
        resolved = set(self._resolve_targets(targets))
        group_targets = set(self._tab_groups[name]["targets"])
        to_close = resolved & group_targets
        if not to_close:
            return {"ok": False, "error": "指定的标签页不在该群组中"}

        closed = []
        for tid in to_close:
            try:
                title = ""
                try:
                    title = self._evaluate(tid, "document.title") or ""
                except Exception:
                    pass
                self._cdp.call("Target.closeTarget", {"targetId": tid})
                self._invalidate_session(tid)
                closed.append({"targetId": tid, "title": title})
            except Exception as e:
                closed.append({"targetId": tid, "error": str(e)})

        self._tab_groups[name]["targets"] = [
            t for t in self._tab_groups[name]["targets"] if t not in to_close
        ]
        import time as _time
        _time.sleep(0.2)
        try:
            self._cdp.call("Target.getTargets")
        except Exception:
            pass
        remaining = len(self._tab_groups[name]["targets"])
        return {"ok": True, "name": name, "closed": len(closed), "remaining": remaining, "tabs": closed}

    # 区域映射：将视口分为 3x3 九宫格
    _REGION_MAP = {
        "top-left", "top", "top-right",
        "left", "center", "right",
        "bottom-left", "bottom", "bottom-right",
    }

    def _find_elements_by_text_js(self, text: str, tag: str = "", region: str = "") -> str:
        """生成 JS 代码：按文本搜索可见元素，返回所有匹配项及坐标。

        支持 region 过滤（nine-grid: top-left, top, top-right, ...）。
        返回的坐标是视口相对坐标，附带 visible 标记区分是否在当前可视区。
        """
        region_filter = ""
        if region and region in self._REGION_MAP:
            region_filter = f"""
                var vw = window.innerWidth, vh = window.innerHeight;
                var xLo = 0, xHi = vw, yLo = 0, yHi = vh;
                var reg = {json.dumps(region)};
                if (reg.includes('left'))   {{ xHi = vw / 3; }}
                if (reg.includes('right'))  {{ xLo = vw * 2 / 3; }}
                if (reg.startsWith('top'))  {{ yHi = vh / 3; }}
                if (reg.startsWith('bottom') || reg === 'bottom') {{ yLo = vh * 2 / 3; }}
                if (reg === 'center') {{ xLo = vw/3; xHi = vw*2/3; yLo = vh/3; yHi = vh*2/3; }}
                if (reg === 'top')    {{ xLo = vw/3; xHi = vw*2/3; yHi = vh/3; }}
                if (reg === 'bottom') {{ xLo = vw/3; xHi = vw*2/3; yLo = vh*2/3; }}
                if (reg === 'left')   {{ xHi = vw/3; yLo = vh/3; yHi = vh*2/3; }}
                if (reg === 'right')  {{ xLo = vw*2/3; yLo = vh/3; yHi = vh*2/3; }}
                function inRegion(cx, cy) {{ return cx >= xLo && cx <= xHi && cy >= yLo && cy <= yHi; }}
            """
        else:
            region_filter = "function inRegion() { return true; }"

        return f"""
            (function() {{
                var tag = {json.dumps(tag or '*')};
                var text = {json.dumps(text)};
                var vw = window.innerWidth, vh = window.innerHeight;
                {region_filter}
                var candidates = document.querySelectorAll(tag);
                var results = [];
                function checkEl(el, method) {{
                    var r = el.getBoundingClientRect();
                    if (r.width === 0 && r.height === 0) return;
                    var cx = Math.round(r.x + r.width/2), cy = Math.round(r.y + r.height/2);
                    var visible = (r.bottom > 0 && r.top < vh && r.right > 0 && r.left < vw);
                    if (!inRegion(cx, cy)) return;
                    results.push({{
                        x: cx, y: cy, w: Math.round(r.width), h: Math.round(r.height),
                        tag: el.tagName.toLowerCase(),
                        text: (el.textContent || '').trim().substring(0, 60),
                        visible: visible,
                        pageX: Math.round(r.x + window.scrollX + r.width/2),
                        pageY: Math.round(r.y + window.scrollY + r.height/2),
                        method: method
                    }});
                }}
                // normalize: \u00a0 / &nbsp; / 全角空格 → 普通空格，合并连续空格
                function norm(s) {{ return s.replace(/[\u00a0\u3000\u2002\u2003\u2009\u200a]/g, ' ').replace(/\s+/g, ' ').trim(); }}
                var normText = norm(text);
                // 精确匹配
                for (var el of candidates) {{
                    var elText = norm((el.textContent || '').trim());
                    if (elText === normText) checkEl(el, 'exact');
                }}
                // 包含匹配（仅当精确匹配为空时）
                if (results.length === 0) {{
                    for (var el of candidates) {{
                        var elText = norm((el.textContent || '').trim());
                        if (elText.includes(normText) && elText.length < normText.length * 5) checkEl(el, 'contains');
                    }}
                }}
                return JSON.stringify(results);
            }})()
        """

    def find_text(
        self,
        text: str,
        target: str = "active",
        tag: str = "",
        region: str = "",
    ) -> dict:
        """按文本搜索页面元素，返回所有匹配项及坐标。

        :param text: 搜索文本
        :param tag: 限定标签类型（button, a, span, div...）
        :param region: 限定区域（top-left, top, top-right, left, center, right,
                       bottom-left, bottom, bottom-right）
        """
        target_id = self.resolve_target(target)
        js = self._find_elements_by_text_js(text, tag=tag, region=region)
        raw = self._evaluate(target_id, js)
        results = json.loads(raw) if isinstance(raw, str) else raw
        return {"ok": True, "count": len(results), "matches": results}

    def click_text(
        self,
        text: str,
        target: str = "active",
        tag: str = "",
        dblclick: bool = False,
        right: bool = False,
        nth: int = 1,
        region: str = "",
    ) -> dict:
        """通过文本内容查找元素并点击（一步完成，无需先 snapshot）。

        :param text: 按钮/链接的文本内容（精确匹配或包含匹配）
        :param tag: 限定标签类型（如 button, a, span），空表示任意
        :param nth: 第 N 个匹配（默认 1），用于同名元素
        :param region: 限定区域（top-right, top-left, ...），缩小搜索范围
        :param dblclick: 双击
        :param right: 右键
        """
        target_id = self.resolve_target(target)
        btn = "right" if right else "left"

        js = self._find_elements_by_text_js(text, tag=tag, region=region)
        raw = self._evaluate(target_id, js)
        matches = json.loads(raw) if isinstance(raw, str) else raw

        if not matches:
            region_hint = f" (region={region})" if region else ""
            raise RuntimeError(f"click-text 失败: 未找到 '{text}'{region_hint}")
        if len(matches) < nth:
            raise RuntimeError(
                f"click-text 失败: 找到 {len(matches)} 个匹配，但需要第 {nth} 个"
            )

        hit = matches[nth - 1]
        x, y = hit["x"], hit["y"]

        # scrollIntoView（如果不在可视区）
        self._evaluate(target_id, f"""
            (function() {{
                var el = document.elementFromPoint({x}, {y});
                if (el) el.scrollIntoView({{block: 'center', behavior: 'instant'}});
            }})()
        """)

        self._cdp_mouse_click(target_id, x, y, button=btn)
        if dblclick and not right:
            self._cdp_mouse_click(target_id, x, y, click_count=2)

        return {"ok": True, "tag": hit.get("tag", ""), "text": hit.get("text", ""),
                "at": [x, y], "dblclick": dblclick, "right": right}

    # =================================================================
    # 网络抓包（network capture）
    # =================================================================
    # Monaco / CodeMirror 编辑器操作
    # =================================================================

    def editor_get(self, target: str = "active") -> dict:
        """读取编辑器当前内容（自动探测 Monaco / CodeMirror / Ace / textarea）。

        返回 {"ok": True, "type": "monaco"|"codemirror6"|"codemirror5"|"ace"|"textarea",
               "value": "...", "language": ""}
        """
        target_id = self.resolve_target(target)
        js = r"""
        (() => {
            function isVisible(el) {
                if (!el) return false;
                const rect = el.getBoundingClientRect();
                const style = getComputedStyle(el);
                return rect.width > 0 && rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden' && parseFloat(style.opacity || '1') !== 0;
            }
            function dialogRoots() {
                return [...document.querySelectorAll('.ant-modal-root, .ant-modal-wrap, .ant-modal, [role="dialog"]')].filter(isVisible);
            }
            function pickInDialog(selector) {
                const roots = dialogRoots();
                for (const root of roots) {
                    const found = root.querySelector(selector);
                    if (found && isVisible(found)) return found;
                }
                return null;
            }
            function pickVisible(selector) {
                const els = [...document.querySelectorAll(selector)].filter(isVisible);
                if (!els.length) return null;
                els.sort((a, b) => {
                    const ra = a.getBoundingClientRect(), rb = b.getBoundingClientRect();
                    return (rb.width * rb.height) - (ra.width * ra.height);
                });
                return els[0];
            }

            // 1. Monaco
            const ta = pickInDialog('textarea.inputarea.monaco-mouse-cursor-text, textarea.inputarea')
                    || pickVisible('textarea.inputarea.monaco-mouse-cursor-text, textarea.inputarea');
            if (ta) {
                return JSON.stringify({ok: true, type: 'monaco', value: ta.value, language: ''});
            }
            // 2. CodeMirror 6
            const cm6 = pickInDialog('.cm-editor') || pickVisible('.cm-editor');
            if (cm6 && cm6.cmView && cm6.cmView.view) {
                const doc = cm6.cmView.view.state.doc.toString();
                return JSON.stringify({ok: true, type: 'codemirror6', value: doc, language: ''});
            }
            // 3. CodeMirror 5
            const cm5 = pickInDialog('.CodeMirror') || pickVisible('.CodeMirror');
            if (cm5 && cm5.CodeMirror) {
                const doc = cm5.CodeMirror.getValue();
                return JSON.stringify({ok: true, type: 'codemirror5', value: doc, language: ''});
            }
            // 4. Ace Editor（通过 DOM 元素上挂载的实例或全局 ace 对象）
            const aceEl = pickInDialog('.ace_editor') || pickVisible('.ace_editor');
            if (aceEl) {
                // 方式一：DOM 元素上的 env.editor 引用
                var editor = aceEl.env && aceEl.env.editor;
                // 方式二：全局 ace.edit()
                if (!editor && typeof ace !== 'undefined') {
                    try { editor = ace.edit(aceEl); } catch(e) {}
                }
                if (editor && typeof editor.getValue === 'function') {
                    const val = editor.getValue();
                    const mode = (editor.session && editor.session.$modeId) || '';
                    return JSON.stringify({ok: true, type: 'ace', value: val, language: mode});
                }
            }
            // 5. 普通 textarea（面积足够大的才认为是编辑器）
            const anyTa = pickInDialog('textarea') || pickVisible('textarea');
            if (anyTa && anyTa.getBoundingClientRect().width > 100) {
                return JSON.stringify({ok: true, type: 'textarea', value: anyTa.value, language: ''});
            }
            return JSON.stringify({ok: false, error: 'No editor found'});
        })()
        """
        raw = self._evaluate(target_id, js)
        return json.loads(raw) if isinstance(raw, str) else {"ok": False, "error": "unexpected"}

    def editor_set(self, text: str, target: str = "active", append: bool = False) -> dict:
        """设置编辑器内容（整段写入），自动探测 Monaco / CodeMirror / Ace / textarea。

        对 Ace Editor 优先通过 JS API 直接写入（避免 insertText 兼容问题）。
        对其他编辑器通过 CDP Input.insertText 实现。
        :param text: 要写入的完整文本
        :param append: True 时追加到末尾，False 时全选后替换
        """
        target_id = self.resolve_target(target)

        focus_js = r"""
        (() => {
            function isVisible(el) {
                if (!el) return false;
                const rect = el.getBoundingClientRect();
                const style = getComputedStyle(el);
                return rect.width > 0 && rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden' && parseFloat(style.opacity || '1') !== 0;
            }
            function dialogRoots() {
                return [...document.querySelectorAll('.ant-modal-root, .ant-modal-wrap, .ant-modal, [role="dialog"]')].filter(isVisible);
            }
            function pickInDialog(selector) {
                const roots = dialogRoots();
                for (const root of roots) {
                    const found = root.querySelector(selector);
                    if (found && isVisible(found)) return found;
                }
                return null;
            }
            function pickVisible(selector) {
                const els = [...document.querySelectorAll(selector)].filter(isVisible);
                if (!els.length) return null;
                els.sort((a, b) => {
                    const ra = a.getBoundingClientRect(), rb = b.getBoundingClientRect();
                    return (rb.width * rb.height) - (ra.width * ra.height);
                });
                return els[0];
            }

            const ta = pickInDialog('textarea.inputarea.monaco-mouse-cursor-text, textarea.inputarea')
                    || pickVisible('textarea.inputarea.monaco-mouse-cursor-text, textarea.inputarea');
            if (ta) {
                ta.focus();
                return 'monaco';
            }
            const cm6 = pickInDialog('.cm-editor') || pickVisible('.cm-editor');
            if (cm6) {
                cm6.focus();
                return 'codemirror6';
            }
            const cm5 = pickInDialog('.CodeMirror textarea') || pickVisible('.CodeMirror textarea');
            if (cm5) {
                cm5.focus();
                return 'codemirror5';
            }
            const anyTa = pickInDialog('textarea') || pickVisible('textarea');
            if (anyTa) {
                anyTa.focus();
                return 'textarea';
            }
            return 'none';
        })()
        """

        focused_editor = self._evaluate(target_id, focus_js)

        # --- 先尝试 Ace Editor JS 路径（最可靠）---
        escaped = json.dumps(text)
        ace_js = f"""
        (() => {{
            function isVisible(el) {{
                if (!el) return false;
                const rect = el.getBoundingClientRect();
                const style = getComputedStyle(el);
                return rect.width > 0 && rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden' && parseFloat(style.opacity || '1') !== 0;
            }}
            function dialogRoots() {{
                return [...document.querySelectorAll('.ant-modal-root, .ant-modal-wrap, .ant-modal, [role="dialog"]')].filter(isVisible);
            }}
            function pickInDialog(selector) {{
                const roots = dialogRoots();
                for (const root of roots) {{
                    const found = root.querySelector(selector);
                    if (found && isVisible(found)) return found;
                }}
                return null;
            }}
            function pickVisible(selector) {{
                const els = [...document.querySelectorAll(selector)].filter(isVisible);
                if (!els.length) return null;
                els.sort((a, b) => {{
                    const ra = a.getBoundingClientRect(), rb = b.getBoundingClientRect();
                    return (rb.width * rb.height) - (ra.width * ra.height);
                }});
                return els[0];
            }}
            var aceEl = pickInDialog('.ace_editor') || pickVisible('.ace_editor');
            if (!aceEl) return 'no_ace';
            var editor = (aceEl.env && aceEl.env.editor)
                      || (typeof ace !== 'undefined' && (() => {{ try {{ return ace.edit(aceEl); }} catch(e) {{}} }})());
            if (!editor || typeof editor.setValue !== 'function') return 'no_editor';
            if ({json.dumps(append)}) {{
                editor.navigateFileEnd();
                editor.insert({escaped});
            }} else {{
                editor.setValue({escaped}, -1);   // -1 = 光标置顶
            }}
            return 'ok';
        }})()
        """
        ace_result = self._evaluate(target_id, ace_js)
        if ace_result == "ok":
            return {"ok": True, "type": "ace", "length": len(text), "append": append}

        # --- 回退到 CDP Input 路径（Monaco / CodeMirror / textarea）---
        if focused_editor == 'none':
            raise RuntimeError("No visible editor found")
        _meta = 4 if sys.platform == "darwin" else 2  # Mac: Meta(Cmd), Others: Ctrl

        if append:
            self.page_call(target_id, "Input.dispatchKeyEvent", {
                "type": "rawKeyDown", "key": "End",
                "windowsVirtualKeyCode": 35,
                "modifiers": _meta,
            })
            self.page_call(target_id, "Input.dispatchKeyEvent", {
                "type": "keyUp", "key": "End",
                "windowsVirtualKeyCode": 35,
            })
        else:
            # Cmd+A / Ctrl+A 全选
            self.page_call(target_id, "Input.dispatchKeyEvent", {
                "type": "rawKeyDown", "key": "a",
                "code": "KeyA", "windowsVirtualKeyCode": 65,
                "modifiers": _meta,
            })
            self.page_call(target_id, "Input.dispatchKeyEvent", {
                "type": "keyUp", "key": "a",
                "code": "KeyA", "windowsVirtualKeyCode": 65,
            })

        self.page_call(target_id, "Input.insertText", {"text": text})
        return {"ok": True, "type": "input", "length": len(text), "append": append}

    def editor_type(self, text: str, target: str = "active") -> dict:
        """在编辑器中逐字符输入文本（模拟真实打字）。

        适用于需要触发 autocomplete / 语法高亮增量更新的场景。
        比 editor_set 慢但更真实。
        """
        target_id = self.resolve_target(target)
        for ch in text:
            if ch == "\n":
                self.page_call(target_id, "Input.dispatchKeyEvent", {
                    "type": "rawKeyDown", "key": "Enter",
                    "code": "Enter", "windowsVirtualKeyCode": 13,
                })
                self.page_call(target_id, "Input.dispatchKeyEvent", {
                    "type": "keyUp", "key": "Enter",
                    "code": "Enter", "windowsVirtualKeyCode": 13,
                })
            else:
                self.page_call(target_id, "Input.dispatchKeyEvent", {
                    "type": "keyDown", "key": ch, "text": ch,
                })
                self.page_call(target_id, "Input.dispatchKeyEvent", {
                    "type": "keyUp", "key": ch,
                })
        return {"ok": True, "length": len(text)}

    # =================================================================
    # find-icon / find-by-attr（图标按钮搜索）
    # =================================================================

    def find_icon(
        self,
        query: str,
        target: str = "active",
        region: str = "",
    ) -> list[dict]:
        """通过 title / aria-label / anticon class 搜索图标按钮。

        :param query: 搜索词（匹配 title、aria-label、anticon-{name} 中的 name）
        :param region: 限定区域（九宫格）
        :return: [{"tag", "title", "ariaLabel", "cls", "x", "y", "w", "h", "disabled"}, ...]
        """
        target_id = self.resolve_target(target)
        region_js = ""
        if region and region in self._REGION_MAP:
            region_js = f"""
            const vw = window.innerWidth, vh = window.innerHeight;
            const rr = '{region}'.split('-');
            const ry = rr[0] === 'top' ? [0, vh/3] : rr[0] === 'bottom' ? [vh*2/3, vh] : [vh/3, vh*2/3];
            const rx = (rr[1]||rr[0]) === 'left' ? [0, vw/3] : (rr[1]||rr[0]) === 'right' ? [vw*2/3, vw] : [vw/3, vw*2/3];
            """
        else:
            region_js = "const rx = [0, Infinity], ry = [0, Infinity];"

        js = f"""
        (() => {{
            {region_js}
            const q = {json.dumps(query)}.toLowerCase();
            const results = [];
            const els = document.querySelectorAll('button, [role=button], a, span, i, svg, [title], [aria-label]');
            const seen = new Set();
            for (const el of els) {{
                const rect = el.getBoundingClientRect();
                if (rect.width === 0 || rect.height === 0) continue;
                const cx = rect.left + rect.width/2, cy = rect.top + rect.height/2;
                if (cx < rx[0] || cx > rx[1] || cy < ry[0] || cy > ry[1]) continue;

                const title = (el.title || '').toLowerCase();
                const ariaLabel = (el.getAttribute('aria-label') || '').toLowerCase();
                const cls = (el.className || '').toString().toLowerCase();
                // anticon class: anticon-save → save
                const iconMatch = cls.match(/anticon-([\\w-]+)/);
                const iconName = iconMatch ? iconMatch[1] : '';
                // el-icon, iconfont 等其他图标框架
                const iconMatch2 = cls.match(/(?:el-icon|icon)-([\\w-]+)/);
                const iconName2 = iconMatch2 ? iconMatch2[1] : '';

                const matched = title.includes(q) || ariaLabel.includes(q) ||
                                iconName.includes(q) || iconName2.includes(q);
                if (!matched) continue;

                // 去重：按坐标
                const key = Math.round(cx) + '|' + Math.round(cy);
                if (seen.has(key)) continue;
                seen.add(key);

                results.push({{
                    tag: el.tagName.toLowerCase(),
                    title: el.title || '',
                    ariaLabel: el.getAttribute('aria-label') || '',
                    cls: (el.className || '').toString().slice(0, 60),
                    x: Math.round(cx), y: Math.round(cy),
                    w: Math.round(rect.width), h: Math.round(rect.height),
                    disabled: !!(el.disabled || el.getAttribute('disabled') !== null),
                    visible: rect.top >= 0 && rect.left >= 0 && rect.bottom <= window.innerHeight && rect.right <= window.innerWidth,
                }});
            }}
            return JSON.stringify(results);
        }})()
        """
        raw = self._evaluate(target_id, js)
        return json.loads(raw) if isinstance(raw, str) else []

    def click_icon(
        self,
        query: str,
        target: str = "active",
        region: str = "",
        nth: int = 1,
        dblclick: bool = False,
        right: bool = False,
    ) -> dict:
        """通过 title / aria-label / icon class 搜索并点击图标按钮。"""
        matches = self.find_icon(query, target=target, region=region)
        if not matches:
            region_hint = f" (region={region})" if region else ""
            raise RuntimeError(f"click-icon 失败: 未找到 '{query}'{region_hint}")
        # 过滤掉 disabled
        enabled = [m for m in matches if not m.get("disabled")]
        if not enabled:
            raise RuntimeError(f"click-icon 失败: '{query}' 按钮已禁用")
        if len(enabled) < nth:
            raise RuntimeError(f"click-icon 失败: 找到 {len(enabled)} 个可用匹配，但需要第 {nth} 个")

        hit = enabled[nth - 1]
        target_id = self.resolve_target(target)
        btn = "right" if right else "left"
        self._cdp_mouse_click(target_id, hit["x"], hit["y"], button=btn)
        if dblclick and not right:
            self._cdp_mouse_click(target_id, hit["x"], hit["y"], click_count=2)

        return {"ok": True, "tag": hit["tag"], "title": hit.get("title", ""),
                "at": [hit["x"], hit["y"]], "dblclick": dblclick, "right": right,
                "disabled": hit.get("disabled", False)}

    # =================================================================
    # scan-tooltips（悬浮提示按钮批量发现）
    # =================================================================

    def scan_tooltips(
        self,
        target: str = "active",
        region: str = "",
        scope: str = "",
    ) -> dict:
        """扫描区域内所有图标按钮，逐个 hover 收集 tooltip 文字。

        适用于只有鼠标悬浮才显示文字的按钮（Ant Design Tooltip / Element Tooltip 等）。
        :param region: 限定九宫格区域
        :param scope: CSS 选择器限定扫描范围
        :return: {"ok": True, "buttons": [{"tooltip", "icon", "tag", "x", "y", "w", "h", "disabled"}, ...]}
        """
        import time as _time

        target_id = self.resolve_target(target)

        # 1. 收集所有小型可点击元素（图标按钮）
        region_js = ""
        if region and region in self._REGION_MAP:
            region_js = f"""
            const vw = window.innerWidth, vh = window.innerHeight;
            const rr = '{region}'.split('-');
            const ry = rr[0] === 'top' ? [0, vh/3] : rr[0] === 'bottom' ? [vh*2/3, vh] : [vh/3, vh*2/3];
            const rx = (rr[1]||rr[0]) === 'left' ? [0, vw/3] : (rr[1]||rr[0]) === 'right' ? [vw*2/3, vw] : [vw/3, vw*2/3];
            function inRegion(cx, cy) {{ return cx >= rx[0] && cx <= rx[1] && cy >= ry[0] && cy <= ry[1]; }}
            """
        else:
            region_js = "function inRegion() { return true; }"

        scope_sel = json.dumps(scope) if scope else "null"
        js_collect = f"""
        (() => {{
            {region_js}
            const root = {scope_sel} ? document.querySelector({scope_sel}) : document;
            if (!root) return JSON.stringify([]);
            const els = root.querySelectorAll('button, [role=button], a, span.anticon, i.anticon, [class*=icon], svg');
            const results = [];
            const seen = new Set();
            for (const el of els) {{
                const rect = el.getBoundingClientRect();
                if (rect.width === 0 || rect.height === 0) continue;
                // 只要小型元素（图标按钮通常 < 100px 宽）
                if (rect.width > 120 || rect.height > 60) continue;
                const cx = Math.round(rect.left + rect.width/2);
                const cy = Math.round(rect.top + rect.height/2);
                if (!inRegion(cx, cy)) continue;
                // 有可见文字 > 4 字符的跳过（文字按钮不需要 tooltip 发现）
                const text = (el.textContent || '').trim();
                if (text.length > 4) continue;
                const key = cx + '|' + cy;
                if (seen.has(key)) continue;
                seen.add(key);
                const cls = (el.className || '').toString();
                const iconMatch = cls.match(/anticon-([\\w-]+)/);
                const iconMatch2 = cls.match(/(?:el-icon|icon)-([\\w-]+)/);
                results.push({{
                    x: cx, y: cy,
                    w: Math.round(rect.width), h: Math.round(rect.height),
                    tag: el.tagName.toLowerCase(),
                    icon: iconMatch ? iconMatch[1] : (iconMatch2 ? iconMatch2[1] : ''),
                    title: el.title || '',
                    ariaLabel: el.getAttribute('aria-label') || '',
                    disabled: !!(el.disabled || el.getAttribute('disabled') !== null),
                    text: text.slice(0, 20),
                }});
            }}
            return JSON.stringify(results);
        }})()
        """
        raw = self._evaluate(target_id, js_collect)
        candidates = json.loads(raw) if isinstance(raw, str) else []
        if not candidates:
            return {"ok": True, "buttons": [], "count": 0}

        # 2. 逐个 hover，收集 tooltip
        # 先定义 tooltip 检测 JS（兼容 Ant Design / Element UI / Arco / 原生 title）
        js_get_tooltip = """
        (() => {
            // Ant Design: .ant-tooltip:not(.ant-tooltip-hidden)
            const antTips = document.querySelectorAll('.ant-tooltip');
            for (const t of antTips) {
                const rect = t.getBoundingClientRect();
                if (rect.width > 0 && rect.height > 0) {
                    const inner = t.querySelector('.ant-tooltip-inner');
                    return (inner || t).textContent.trim();
                }
            }
            // Element UI: .el-tooltip__popper[aria-hidden=false]
            const elTips = document.querySelectorAll('.el-tooltip__popper, .el-popper');
            for (const t of elTips) {
                const rect = t.getBoundingClientRect();
                if (rect.width > 0 && rect.height > 0) return t.textContent.trim();
            }
            // Arco Design: .arco-tooltip-content
            const arcoTips = document.querySelectorAll('.arco-tooltip-content');
            for (const t of arcoTips) {
                const rect = t.getBoundingClientRect();
                if (rect.width > 0 && rect.height > 0) return t.textContent.trim();
            }
            // role="tooltip"
            const roleTips = document.querySelectorAll('[role=tooltip]');
            for (const t of roleTips) {
                const rect = t.getBoundingClientRect();
                if (rect.width > 0 && rect.height > 0) return t.textContent.trim();
            }
            return '';
        })()
        """

        buttons: list[dict] = []
        # 先移到空白区域清除已有 tooltip
        self._cdp_mouse_click(target_id, 1, 1, button="none")
        _time.sleep(0.1)

        for cand in candidates:
            # 如果已有 title / ariaLabel，直接用，不需要 hover
            if cand.get("title") or cand.get("ariaLabel"):
                buttons.append({
                    "tooltip": cand.get("title") or cand.get("ariaLabel"),
                    "icon": cand.get("icon", ""),
                    "tag": cand["tag"],
                    "x": cand["x"], "y": cand["y"],
                    "w": cand["w"], "h": cand["h"],
                    "disabled": cand.get("disabled", False),
                    "source": "attr",
                })
                continue

            # hover 该元素
            self.page_call(target_id, "Input.dispatchMouseEvent", {
                "type": "mouseMoved", "x": cand["x"], "y": cand["y"],
            })
            _time.sleep(0.35)

            # 读取 tooltip
            tip = self._evaluate(target_id, js_get_tooltip) or ""

            # 移开清除
            self.page_call(target_id, "Input.dispatchMouseEvent", {
                "type": "mouseMoved", "x": 1, "y": 1,
            })
            _time.sleep(0.1)

            if tip:
                buttons.append({
                    "tooltip": tip,
                    "icon": cand.get("icon", ""),
                    "tag": cand["tag"],
                    "x": cand["x"], "y": cand["y"],
                    "w": cand["w"], "h": cand["h"],
                    "disabled": cand.get("disabled", False),
                    "source": "hover",
                })

        return {"ok": True, "buttons": buttons, "count": len(buttons)}

    # =================================================================

    def _drain_network_events(self, target_id: str, drain_ms: int = 800) -> None:
        """主动刷出 WS 上堆积的 Network 事件。

        发送一个轻量 JS 求值，等待响应的过程中会读取到
        堆积在 WS 上的 Network 事件并写入 buffer。
        连续做几轮以确保异步请求全部到达。

        如果 follow 模式启用，同时处理新 tab 队列。
        """
        rounds = max(1, drain_ms // 200)
        for _ in range(rounds):
            try:
                self._evaluate(target_id, "'__drain__'")
            except Exception:
                pass
            # follow 模式：处理新 tab 队列
            self._process_follow_queue(target_id)
            # 对已跟踪的新 tab 也 drain
            with self._net_lock:
                follow_tids = list(self._net_follow_tabs.keys())
            for ftid in follow_tids:
                try:
                    self._evaluate(ftid, "'__drain__'")
                except Exception:
                    pass
            time.sleep(0.15)

    def network_capture_start(self, target: str = "active",
                               follow: bool = False) -> dict:
        """开始网络抓包：启用 Network 域监听。

        :param follow: 启用 follow 模式，自动跟踪新打开的 tab 并抓包
        """
        target_id = self.resolve_target(target)
        sid = self._get_or_attach(target_id)

        # 启用 Network 域
        self.page_call(target_id, "Network.enable", {})
        started_at = time.time()

        with self._net_lock:
            self._net_capture_active[sid] = True
            self._net_capture_buffer[sid] = []
            self._net_last_event_at[sid] = started_at
            self._net_capture_started_at[sid] = started_at
            # 清理该 session 的旧请求映射
            stale = [k for k, v in self._net_request_map.items()
                     if v.get("sessionId") == sid]
            for k in stale:
                del self._net_request_map[k]

            # follow 模式初始化
            self._net_follow_origin = ""
            self._net_follow_known.clear()
            self._net_follow_queue.clear()
            self._net_follow_tabs.clear()
            if follow:
                self._net_follow_origin = target_id
                # 记录当前已有的所有 page targetId
                pages = self._cdp.get_pages()
                self._net_follow_known = {p["targetId"] for p in pages}

        return {"ok": True, "target_id": target_id, "session_id": sid,
                "follow": follow,
                "message": "network capture started" + (" (follow mode)" if follow else "")}

    @staticmethod
    def _capture_match_filter(
        req: dict[str, Any],
        method_filter: str = "",
        url_filter: str = "",
        exclude_domain: str = "",
        status_filter: str = "",
    ) -> bool:
        """判断单条请求是否命中过滤条件。"""
        method = str(req.get("method", "")).upper()
        url = str(req.get("url", ""))
        status = str(req.get("status", ""))
        if method_filter and method != method_filter.upper():
            return False
        if url_filter and url_filter.lower() not in url.lower():
            return False
        if exclude_domain and exclude_domain.lower() in url.lower():
            return False
        if status_filter and status != str(status_filter):
            return False
        return True

    @staticmethod
    def _capture_match_until(req: dict[str, Any], until_match: str = "") -> bool:
        """判断单条请求是否命中提前结束关键字。"""
        if not until_match:
            return False
        needle = until_match.lower()
        url = str(req.get("url", "")).lower()
        method = str(req.get("method", "")).lower()
        status = str(req.get("status", "")).lower()
        return needle in url or needle in method or needle in status

    def _capture_sessions_snapshot(
        self,
        origin_sid: str,
    ) -> tuple[list[str], dict[str, dict[str, Any]]]:
        """返回当前抓包相关 session 列表和 follow tab 快照。"""
        with self._net_lock:
            follow_tabs_info = dict(self._net_follow_tabs)
        all_sessions = [origin_sid]
        for finfo in follow_tabs_info.values():
            fsid = str(finfo.get("sessionId") or "")
            if fsid and fsid not in all_sessions:
                all_sessions.append(fsid)
        return all_sessions, follow_tabs_info

    def _capture_requests_snapshot(
        self,
        origin_sid: str,
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        """读取当前已缓存请求快照，供 idle / until-match 判定。"""
        all_sessions, follow_tabs_info = self._capture_sessions_snapshot(origin_sid)
        with self._net_lock:
            requests = [
                dict(item)
                for sid in all_sessions
                for item in self._net_capture_buffer.get(sid, [])
            ]
        return requests, follow_tabs_info

    def _wait_capture_ready(
        self,
        target_id: str,
        origin_sid: str,
        wait_ms: int = 0,
        idle_ms: int = 0,
        until_match: str = "",
    ) -> tuple[bool, list[dict[str, Any]], dict[str, dict[str, Any]]]:
        """在 stop 前等待固定窗口、网络空闲或目标请求出现。"""
        matched = False
        requests_snapshot: list[dict[str, Any]] = []
        follow_tabs_info: dict[str, dict[str, Any]] = {}
        remaining_wait = max(0, int(wait_ms))
        while remaining_wait > 0:
            sleep_ms = min(200, remaining_wait)
            time.sleep(sleep_ms / 1000.0)
            self._drain_network_events(target_id, drain_ms=sleep_ms)
            requests_snapshot, follow_tabs_info = self._capture_requests_snapshot(origin_sid)
            matched = any(self._capture_match_until(req, until_match) for req in requests_snapshot)
            if matched:
                return True, requests_snapshot, follow_tabs_info
            remaining_wait -= sleep_ms

        if idle_ms > 0:
            idle_seconds = max(0.1, int(idle_ms) / 1000.0)
            idle_deadline = time.time() + max(3.0, idle_seconds * 8)
            while True:
                self._drain_network_events(target_id, drain_ms=min(max(int(idle_ms), 200), 1000))
                requests_snapshot, follow_tabs_info = self._capture_requests_snapshot(origin_sid)
                matched = any(self._capture_match_until(req, until_match) for req in requests_snapshot)
                if matched:
                    return True, requests_snapshot, follow_tabs_info
                all_sessions, _ = self._capture_sessions_snapshot(origin_sid)
                with self._net_lock:
                    last_event = max(
                        (self._net_last_event_at.get(sid, self._net_capture_started_at.get(sid, time.time()))
                         for sid in all_sessions),
                        default=time.time(),
                    )
                if time.time() - last_event >= idle_seconds:
                    break
                if time.time() >= idle_deadline:
                    break
                time.sleep(min(0.2, idle_seconds / 2))

        if not requests_snapshot:
            requests_snapshot, follow_tabs_info = self._capture_requests_snapshot(origin_sid)
        return matched, requests_snapshot, follow_tabs_info

    @staticmethod
    def _response_size_hint(req_info: dict[str, Any]) -> int:
        """估算响应体大小，优先使用 encodedDataLength / Content-Length。"""
        encoded_len = req_info.get("encodedDataLength")
        if isinstance(encoded_len, (int, float)) and encoded_len > 0:
            return int(encoded_len)
        headers = req_info.get("responseHeaders") or {}
        raw = headers.get("content-length") or headers.get("Content-Length")
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def navigate_page(
        self,
        target: str = "active",
        url: str | None = None,
        reload: bool = False,
    ) -> dict:
        """导航到 URL 或刷新当前页（Page.navigate / Page.reload）。"""
        target_id = self.resolve_target(target)
        if reload and not url:
            self.page_call(target_id, "Page.reload", {"ignoreCache": False})
            return {"ok": True, "target_id": target_id, "action": "reload"}
        if not url:
            return {"ok": False, "error": "missing 'url' (or set reload=true)"}
        self.page_call(target_id, "Page.navigate", {"url": url})
        return {"ok": True, "target_id": target_id, "action": "navigate", "url": url}

    def network_capture_stop(self, target: str = "active",
                              get_body: bool = False,
                              wait_ms: int = 0,
                              body_mode: str = "",
                              idle_ms: int = 0,
                              max_bodies: int = 0,
                              max_body_bytes: int = 0,
                              method_filter: str = "",
                              url_filter: str = "",
                              exclude_domain: str = "",
                              status_filter: str = "",
                              until_match: str = "") -> dict:
        """停止网络抓包，返回捕获的 API 请求列表。

        :param get_body: 是否同时获取响应 body（较慢，按需开启）
        :param wait_ms: stop 前额外等待并 drain 网络事件（SPA 场景建议 3000~8000）
        :param body_mode: body 抓取策略，支持 none / filtered / all
        :param idle_ms: 额外等待网络空闲窗口，适合替代固定 wait_ms
        :param max_bodies: 最多抓取多少个响应 body，0 表示不限
        :param max_body_bytes: 超过阈值时跳过 body 获取，0 表示不限
        :param until_match: 命中关键字后提前结束等待

        follow 模式下会聚合所有跟踪 tab 的请求，并返回新 tab 信息。
        """
        target_id = self.resolve_target(target)
        sid = self._get_or_attach(target_id)
        body_mode_val = str(body_mode or ("all" if get_body else "none")).strip().lower()
        if body_mode_val not in {"none", "filtered", "all"}:
            body_mode_val = "all" if get_body else "none"

        self._wait_capture_ready(
            target_id=target_id,
            origin_sid=sid,
            wait_ms=wait_ms,
            idle_ms=idle_ms,
            until_match=until_match,
        )

        # 先刷出堆积事件（包括 follow 新 tab）
        self._drain_network_events(target_id, drain_ms=1200)

        with self._net_lock:
            # 收集所有 active session 的请求
            all_sessions = [sid]
            follow_tabs_info = dict(self._net_follow_tabs)
            for ftid, finfo in follow_tabs_info.items():
                fsid = finfo.get("sessionId", "")
                if fsid and fsid != sid:
                    all_sessions.append(fsid)

            requests = []
            for s in all_sessions:
                self._net_capture_active[s] = False
                reqs = self._net_capture_buffer.pop(s, [])
                # 标记来源 tab
                is_origin = (s == sid)
                for r in reqs:
                    r["_source"] = "origin" if is_origin else "new_tab"
                    # 给新 tab 请求附上 tab 信息
                    if not is_origin:
                        for ftid, finfo in follow_tabs_info.items():
                            if finfo.get("sessionId") == s:
                                r["_tab_url"] = finfo.get("url", "")
                                r["_tab_title"] = finfo.get("title", "")
                                break
                requests.extend(reqs)

            # 清理请求映射
            stale = [k for k, v in self._net_request_map.items()
                     if v.get("sessionId") in all_sessions]
            for k in stale:
                del self._net_request_map[k]
            for s in all_sessions:
                self._net_last_event_at.pop(s, None)
                self._net_capture_started_at.pop(s, None)

            # 清理 follow 状态
            self._net_follow_origin = ""
            self._net_follow_known.clear()
            self._net_follow_queue.clear()
            self._net_follow_tabs.clear()

        # 可选：获取响应 body
        BODY_SIZE_THRESHOLD = 512 * 1024  # 512KB → 大于此值写临时文件
        body_fetch: dict[str, Any] = {
            "mode": body_mode_val,
            "selected": 0,
            "fetched": 0,
            "skipped_unmatched": 0,
            "skipped_limit": 0,
            "skipped_too_large": 0,
            "skipped_error": 0,
            "max_bodies": max(0, int(max_bodies or 0)),
            "max_body_bytes": max(0, int(max_body_bytes or 0)),
        }
        if body_mode_val != "none":
            import tempfile as _tmpmod, base64 as _b64, gzip as _gz
            selected_requests: list[dict[str, Any]] = []
            for req_info in requests:
                matched = body_mode_val == "all" or (
                    body_mode_val == "filtered"
                    and (
                        self._capture_match_filter(
                            req_info,
                            method_filter=method_filter,
                            url_filter=url_filter,
                            exclude_domain=exclude_domain,
                            status_filter=status_filter,
                        )
                        or self._capture_match_until(req_info, until_match)
                    )
                )
                if not matched:
                    body_fetch["skipped_unmatched"] += 1
                    req_info["responseBodySkipped"] = "unmatched"
                    continue
                if body_fetch["max_bodies"] and len(selected_requests) >= body_fetch["max_bodies"]:
                    body_fetch["skipped_limit"] += 1
                    req_info["responseBodySkipped"] = "max_bodies"
                    continue
                size_hint = self._response_size_hint(req_info)
                if body_fetch["max_body_bytes"] and size_hint and size_hint > body_fetch["max_body_bytes"]:
                    body_fetch["skipped_too_large"] += 1
                    req_info["responseBodySkipped"] = "max_body_bytes"
                    req_info["responseBodySize"] = size_hint
                    continue
                selected_requests.append(req_info)

            body_fetch["selected"] = len(selected_requests)
            for req_info in selected_requests:
                rid = req_info.get("requestId", "")
                source_sid = req_info.get("sessionId", "")
                if rid and source_sid:
                    req_tid = self._session_to_target(source_sid) or target_id
                    try:
                        body_resp = self.page_call(
                            req_tid, "Network.getResponseBody",
                            {"requestId": rid}
                        )
                        raw_body: str = body_resp.get("body", "")
                        is_b64: bool = body_resp.get("base64Encoded", False)

                        # #18 base64 自动解码
                        if is_b64 and raw_body:
                            try:
                                decoded_bytes = _b64.b64decode(raw_body)
                                # 尝试 gzip 解压
                                try:
                                    decoded_bytes = _gz.decompress(decoded_bytes)
                                except (_gz.BadGzipFile, OSError):
                                    pass  # 不是 gzip，直接用解码后的字节
                                # 尝试 UTF-8 解码，否则保留 base64
                                try:
                                    raw_body = decoded_bytes.decode("utf-8")
                                    is_b64 = False
                                except UnicodeDecodeError:
                                    raw_body = _b64.b64encode(decoded_bytes).decode()
                            except Exception:
                                pass  # 解码失败保留原样

                        # #12 大 body 写临时文件
                        body_size = len(raw_body.encode("utf-8") if isinstance(raw_body, str) else raw_body)
                        if body_fetch["max_body_bytes"] and body_size > body_fetch["max_body_bytes"]:
                            req_info["responseBody"] = None
                            req_info["responseBodySize"] = body_size
                            req_info["responseBodySkipped"] = "max_body_bytes_after_fetch"
                            body_fetch["skipped_too_large"] += 1
                            continue
                        if body_size > BODY_SIZE_THRESHOLD:
                            tmp_dir = _tmpmod.gettempdir()
                            safe_rid = rid.replace(".", "_").replace("/", "_")[:40]
                            body_file = os.path.join(tmp_dir, f"cdp_body_{safe_rid}.txt")
                            with open(body_file, "w", encoding="utf-8") as f:
                                f.write(raw_body)
                            req_info["responseBody"] = None
                            req_info["responseBodyFile"] = body_file
                            req_info["responseBodySize"] = body_size
                            req_info["base64Encoded"] = False
                        else:
                            req_info["responseBody"] = raw_body
                            req_info["responseBodySize"] = body_size
                            req_info["base64Encoded"] = is_b64
                        body_fetch["fetched"] += 1
                    except Exception:
                        req_info["responseBody"] = None
                        req_info["responseBodySkipped"] = "fetch_error"
                        body_fetch["skipped_error"] += 1

        # 禁用 Network 域（减少开销）
        try:
            self.page_call(target_id, "Network.disable", {})
        except Exception:
            pass
        for ftid in follow_tabs_info:
            try:
                self.page_call(ftid, "Network.disable", {})
            except Exception:
                pass

        # 构建新 tab 信息（刷新 URL/title，可能在 attach 后有变化）
        new_tabs = []
        for ftid, finfo in follow_tabs_info.items():
            tab_info = {"targetId": ftid}
            try:
                tab_info["url"] = self.get_url(target=ftid)
                tab_info["title"] = self.get_title(target=ftid)
            except Exception:
                tab_info["url"] = finfo.get("url", "")
                tab_info["title"] = finfo.get("title", "")
            new_tabs.append(tab_info)

        result: dict[str, Any] = {
            "ok": True, "target_id": target_id,
            "count": len(requests), "requests": requests,
            "body_fetch": body_fetch,
        }
        if new_tabs:
            result["new_tabs"] = new_tabs
        return result

    def network_capture_peek(self, target: str = "active") -> dict:
        """读取当前抓包快照，不停止抓包也不清空缓冲区。"""
        target_id = self.resolve_target(target)
        sid = self._get_or_attach(target_id)

        with self._net_lock:
            if not self._net_capture_active.get(sid):
                return {"ok": False, "error": "当前 target 未处于抓包状态"}

        self._drain_network_events(target_id, drain_ms=200)
        requests, _ = self._capture_requests_snapshot(sid)
        all_sessions, _ = self._capture_sessions_snapshot(sid)

        with self._net_lock:
            last_event_at = max(
                (
                    self._net_last_event_at.get(session_id, 0.0)
                    for session_id in all_sessions
                ),
                default=self._net_capture_started_at.get(sid, 0.0),
            )

        return {
            "ok": True,
            "target_id": target_id,
            "count": len(requests),
            "requests": requests,
            "last_event_at": last_event_at,
        }

    def network_capture_export(self, requests: list[dict],
                                 fmt: str = "python") -> str:
        """将捕获的请求列表导出为可执行代码。

        :param requests: network_capture_stop 返回的 requests 列表
        :param fmt: 导出格式（python / curl）
        """
        if fmt == "curl":
            return self._export_curl(requests)
        return self._export_python(requests)

    @staticmethod
    def _export_python(requests: list[dict]) -> str:
        """导出为 Python requests 代码。"""
        lines = [
            '"""自动生成的 API 请求代码（由 CDP network-capture 导出）"""',
            "",
            "import requests",
            "",
            "# 从浏览器复制你的 cookie（或使用 cdp_client.get_cookies）",
            'session = requests.Session()',
            "",
        ]
        for i, req in enumerate(requests):
            method = req.get("method", "GET").upper()
            url = req.get("url", "")
            headers = req.get("headers", {})
            post_data = req.get("postData")
            status = req.get("status", "?")

            lines.append(f"# [{i+1}] {method} {url}")
            lines.append(f"# 响应状态: {status}")

            # 过滤掉自动添加的浏览器头（保留业务认证头如 uid / csrf-token / micro-app-* 等）
            skip_headers = {
                "host", "connection", "content-length",
                "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform",
                "sec-fetch-dest", "sec-fetch-mode", "sec-fetch-site",
                "upgrade-insecure-requests", "accept-encoding",
                "cache-control", "pragma",
            }
            filtered_headers = {
                k: v for k, v in headers.items()
                if k.lower() not in skip_headers
            }
            lines.append(f"headers_{i+1} = {json.dumps(filtered_headers, indent=4, ensure_ascii=False)}")

            if method in ("POST", "PUT", "PATCH") and post_data:
                # 判断是 JSON 还是 form-data
                content_type = headers.get("Content-Type", headers.get("content-type", ""))
                if "application/json" in content_type:
                    try:
                        body = json.loads(post_data)
                        lines.append(f"body_{i+1} = {json.dumps(body, indent=4, ensure_ascii=False)}")
                        lines.append(
                            f"resp_{i+1} = session.{method.lower()}("
                            f"\n    {json.dumps(url)},"
                            f"\n    headers=headers_{i+1},"
                            f"\n    json=body_{i+1},"
                            f"\n)"
                        )
                    except json.JSONDecodeError:
                        lines.append(f"data_{i+1} = {json.dumps(post_data)}")
                        lines.append(
                            f"resp_{i+1} = session.{method.lower()}("
                            f"\n    {json.dumps(url)},"
                            f"\n    headers=headers_{i+1},"
                            f"\n    data=data_{i+1},"
                            f"\n)"
                        )
                else:
                    lines.append(f"data_{i+1} = {json.dumps(post_data)}")
                    lines.append(
                        f"resp_{i+1} = session.{method.lower()}("
                        f"\n    {json.dumps(url)},"
                        f"\n    headers=headers_{i+1},"
                        f"\n    data=data_{i+1},"
                        f"\n)"
                    )
            else:
                lines.append(
                    f"resp_{i+1} = session.{method.lower()}("
                    f"\n    {json.dumps(url)},"
                    f"\n    headers=headers_{i+1},"
                    f"\n)"
                )
            lines.append(f"print(f'[{i+1}] {{resp_{i+1}.status_code}} {method} {url[:80]}')")
            lines.append(f"# print(resp_{i+1}.text[:500])")
            lines.append("")

        return "\n".join(lines)

    # =================================================================
    # 网络请求（fetch / replay）
    # =================================================================

    def _capture_file_path(self) -> str:
        import tempfile
        return os.path.join(tempfile.gettempdir(), "cdp_network_capture.json")

    def network_fetch(
        self,
        url: str,
        method: str = "GET",
        headers: dict | None = None,
        body: str = "",
        target: str = "active",
    ) -> dict:
        """在页面上下文执行 fetch()，自动携带 cookie/session。

        :param url: 请求 URL（支持相对路径，相对于当前页面）
        :param method: HTTP 方法
        :param headers: 额外请求头
        :param body: 请求体（POST/PUT）
        :return: {"ok", "status", "statusText", "headers", "body", "url"}
        """
        target_id = self.resolve_target(target)

        headers_js = json.dumps(headers or {})
        body_js = json.dumps(body) if body else "undefined"

        js = f"""
            (async () => {{
                try {{
                    var opts = {{
                        method: {json.dumps(method)},
                        credentials: 'include',
                        headers: {headers_js},
                    }};
                    var bodyStr = {body_js};
                    if (bodyStr && {json.dumps(method)} !== 'GET' && {json.dumps(method)} !== 'HEAD') {{
                        opts.body = bodyStr;
                    }}
                    var resp = await fetch({json.dumps(url)}, opts);
                    var respHeaders = {{}};
                    resp.headers.forEach(function(v, k) {{ respHeaders[k] = v; }});
                    var contentType = (respHeaders['content-type'] || '').toLowerCase();
                    var body;
                    if (contentType.includes('json')) {{
                        try {{ body = await resp.json(); }} catch(e) {{ body = await resp.text(); }}
                    }} else {{
                        body = await resp.text();
                        if (body.length > 10000) body = body.substring(0, 10000) + '... (truncated)';
                    }}
                    return JSON.stringify({{
                        ok: true,
                        status: resp.status,
                        statusText: resp.statusText,
                        headers: respHeaders,
                        body: body,
                        url: resp.url,
                    }});
                }} catch(e) {{
                    return JSON.stringify({{ok: false, error: e.message}});
                }}
            }})()
        """
        resp = self.page_call(target_id, "Runtime.evaluate", {
            "expression": js,
            "returnByValue": True,
            "awaitPromise": True,
        })
        raw = resp.get("result", {}).get("value", "{}")
        result = json.loads(raw) if isinstance(raw, str) else raw
        return result

    def network_replay(
        self,
        index: int = 1,
        target: str = "active",
        override_url: str = "",
        override_method: str = "",
        override_body: str = "",
    ) -> dict:
        """重放上次 network-capture 抓取的第 N 个请求。

        :param index: 请求序号（1-based，对应 stop 输出的 [N]）
        :param override_url: 覆盖 URL
        :param override_method: 覆盖 HTTP 方法
        :param override_body: 覆盖请求体
        """
        import os
        capture_file = self._capture_file_path()
        if not os.path.exists(capture_file):
            return {"ok": False, "error": "无抓包数据，请先执行 network-capture start/stop"}

        with open(capture_file) as f:
            requests = json.loads(f.read())

        if index < 1 or index > len(requests):
            return {"ok": False, "error": f"序号 {index} 超出范围（共 {len(requests)} 个请求）"}

        req = requests[index - 1]
        url = override_url or req.get("url", "")
        method = override_method or req.get("method", "GET")
        body = override_body or req.get("postData", "")
        headers = req.get("headers", {})
        # 过滤掉不适合 fetch 的 headers
        fetch_headers = {}
        skip_keys = {"host", "connection", "content-length", "accept-encoding",
                      "sec-fetch-site", "sec-fetch-mode", "sec-fetch-dest", "sec-ch-ua",
                      "sec-ch-ua-mobile", "sec-ch-ua-platform", "upgrade-insecure-requests"}
        for k, v in headers.items():
            if k.lower() not in skip_keys:
                fetch_headers[k] = v

        result = self.network_fetch(
            url=url,
            method=method,
            headers=fetch_headers,
            body=body,
            target=target,
        )
        result["replayed_index"] = index
        result["original_status"] = req.get("status")
        result["original_url"] = req.get("url")
        return result

    # =================================================================
    # eval-js / capture-headers / scan-shortcuts
    # =================================================================

    # =================================================================
    # localStorage / sessionStorage
    # =================================================================

    def local_storage_get(
        self,
        key: str,
        storage: str = "local",
        target: str = "active",
    ) -> dict:
        """读取 localStorage 或 sessionStorage 中指定 key 的值。

        :param key:     存储键名（空字符串则返回所有 key）
        :param storage: "local"（默认）或 "session"
        :return: {"ok": True, "key": key, "value": <str|None>, "parsed": <obj if JSON>}
        """
        target_id = self.resolve_target(target)
        obj = "localStorage" if storage != "session" else "sessionStorage"

        if key:
            js = f'(function(){{ var v = {obj}.getItem({json.dumps(key)}); return JSON.stringify({{found: v !== null, value: v}}); }})()'
            raw = self._evaluate(target_id, js)
            try:
                data = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                return {"ok": False, "error": f"JS eval failed: {raw}"}
            value = data.get("value")
            parsed = None
            if value:
                try:
                    parsed = json.loads(value)
                except Exception:
                    pass
            return {"ok": True, "key": key, "value": value, "parsed": parsed,
                    "found": data.get("found", False), "storage": obj}
        else:
            # 返回所有 key-value
            js = f"""(function(){{
                var out = {{}};
                for (var i = 0; i < {obj}.length; i++) {{
                    var k = {obj}.key(i);
                    out[k] = {obj}.getItem(k);
                }}
                return JSON.stringify(out);
            }})()"""
            raw = self._evaluate(target_id, js)
            try:
                data = json.loads(raw) if isinstance(raw, str) else (raw or {})
            except Exception:
                data = {}
            return {"ok": True, "storage": obj, "items": data, "count": len(data)}

    def local_storage_set(
        self,
        key: str,
        value: str,
        storage: str = "local",
        target: str = "active",
    ) -> dict:
        """写入 localStorage 或 sessionStorage。

        :param key:   键名
        :param value: 值（字符串，如需写对象请先 json.dumps）
        :param storage: "local" 或 "session"
        """
        target_id = self.resolve_target(target)
        obj = "localStorage" if storage != "session" else "sessionStorage"
        js = f'{obj}.setItem({json.dumps(key)}, {json.dumps(value)}); "ok"'
        self._evaluate(target_id, js)
        return {"ok": True, "key": key, "storage": obj}

    def local_storage_remove(
        self,
        key: str,
        storage: str = "local",
        target: str = "active",
    ) -> dict:
        """删除 localStorage 或 sessionStorage 中指定 key。"""
        target_id = self.resolve_target(target)
        obj = "localStorage" if storage != "session" else "sessionStorage"
        js = f'{obj}.removeItem({json.dumps(key)}); "ok"'
        self._evaluate(target_id, js)
        return {"ok": True, "key": key, "storage": obj}

    def extract_metric(
        self,
        title: str = "",
        api_url: str = "",
        target: str = "active",
    ) -> dict:
        """从图表组件提取指标数值。

        策略（按优先级）：
        1. DOM 文本节点：从 [class*=metric]、[class*=value]、SVG text 中提取数字
        2. 页面内 fetch REST API（api_url 不为空时）：在页面上下文 fetch，自动携带 cookie
        3. 返回所有找到的数值供调用方选择

        :param title: 指标名（如 "numRecordsIn"），用于过滤匹配的 DOM 容器
        :param api_url: REST API 路径（如 /jobs/.../metrics?get=xxx），在页面上下文 fetch
        """
        target_id = self.resolve_target(target)

        results: dict[str, Any] = {"ok": True, "values": [], "sources": []}

        # ---- 策略1：DOM 扫描 ----
        title_arg = json.dumps(title) if title else '""'
        dom_js = f"""
        (() => {{
            var titleFilter = {title_arg}.toLowerCase();
            var found = [];
            // 找包含 title 关键词的容器
            var containers = titleFilter
                ? [...document.querySelectorAll('*')].filter(el => {{
                    var t = (el.textContent || '').toLowerCase();
                    return t.includes(titleFilter) && el.children.length < 10;
                  }})
                : [document.body];

            for (var c of containers.slice(0, 5)) {{
                // SVG text 节点
                var svgTexts = [...c.querySelectorAll('text, tspan')].map(t => t.textContent.trim()).filter(t => /^[\d,\.\-]+$/.test(t));
                // DOM 叶节点数值
                var leafNums = [...c.querySelectorAll('[class*=value],[class*=metric],[class*=count],[class*=number]')]
                    .map(el => (el.textContent || '').trim())
                    .filter(t => /^[\d,\.\-\s]+$/.test(t) && t.length < 30);
                // Big/Small 标签旁边的值
                var bigSmall = [];
                var labels = [...c.querySelectorAll('*')].filter(el => ['Big','Small','Current','Latest'].includes((el.textContent||'').trim()));
                for (var lbl of labels) {{
                    var sib = lbl.nextSibling || lbl.nextElementSibling;
                    if (sib) bigSmall.push((sib.textContent||'').trim());
                }}
                found.push(...svgTexts, ...leafNums, ...bigSmall);
            }}
            return JSON.stringify({{domValues: [...new Set(found)].filter(v => v && v !== '-')}});
        }})()
        """
        try:
            dom_result = json.loads(self._evaluate(target_id, dom_js))
            dom_values = dom_result.get("domValues", [])
            if dom_values:
                results["values"].extend(dom_values)
                results["sources"].append("dom")
        except Exception:
            pass

        # ---- 策略2：REST API fetch（在页面上下文执行，自动带 cookie）----
        if api_url:
            fetch_js = f"""
            (() => {{
                return fetch({json.dumps(api_url)})
                    .then(r => r.json())
                    .then(d => JSON.stringify(d))
                    .catch(e => JSON.stringify({{error: e.message}}));
            }})()
            """
            try:
                resp = self.page_call(target_id, "Runtime.evaluate", {
                    "expression": fetch_js,
                    "returnByValue": True,
                    "awaitPromise": True,
                })
                raw = resp.get("result", {}).get("value", "")
                if raw:
                    api_data = json.loads(raw)
                    # api_data 可能是 list（Flink metrics）或 dict
                    has_error = isinstance(api_data, dict) and api_data.get("error")
                    if not has_error:
                        results["api_data"] = api_data
                        results["sources"].append("rest_api")
                        # 提取 value 字段
                        items = api_data if isinstance(api_data, list) else [api_data]
                        for item in items:
                            if isinstance(item, dict):
                                v = item.get("value")
                                if v is not None:
                                    results["values"].append(str(v))
            except Exception as exc:
                results["api_error"] = str(exc)

        if not results["values"] and "api_data" not in results:
            results["warning"] = "no metric values found; try --api <rest_url> or check chart is loaded"

        return results

    def eval_js(        self,
        expression: str,
        await_promise: bool = False,
        target: str = "active",
    ) -> dict:
        """在页面上下文执行 JS 表达式并返回结果。

        :param expression: JavaScript 表达式或语句
        :param await_promise: 若表达式返回 Promise，是否等待它 resolve
        :return: {"ok": True, "result": <value>, "type": <str>}
        """
        target_id = self.resolve_target(target)
        resp = self.page_call(target_id, "Runtime.evaluate", {
            "expression": expression,
            "returnByValue": True,
            "awaitPromise": await_promise,
        })
        result = resp.get("result", {})
        exc = resp.get("exceptionDetails")
        if exc:
            msg = exc.get("exception", {}).get("description") or exc.get("text", "JS error")
            return {"ok": False, "error": msg}
        val = result.get("value")
        typ = result.get("type", "")
        return {"ok": True, "result": val, "type": typ}

    def capture_headers(
        self,
        url_filter: str = "",
        wait_sec: float = 10,
        target: str = "active",
    ) -> dict:
        """临时开启网络捕获，等待 wait_sec 秒后停止，返回匹配请求的 headers。

        :param url_filter: URL 关键字过滤（空则返回所有）
        :param wait_sec: 等待时间（秒）
        :return: {"ok": True, "requests": [{"url", "method", "headers"}, ...]}
        """
        import time as _time
        self.network_capture_start(target=target)
        _time.sleep(wait_sec)
        stop_result = self.network_capture_stop(target=target, get_body=False)
        if not stop_result.get("ok"):
            return stop_result
        all_reqs = stop_result.get("requests", [])
        if url_filter:
            all_reqs = [r for r in all_reqs if url_filter.lower() in r.get("url", "").lower()]
        result_list = []
        for r in all_reqs:
            result_list.append({
                "url": r.get("url", ""),
                "method": r.get("method", "GET"),
                "status": r.get("status", "?"),
                "headers": r.get("headers", {}),
            })
        return {"ok": True, "requests": result_list, "count": len(result_list)}

    def scan_shortcuts(self, target: str = "active") -> dict:
        """扫描页面中可见的键盘快捷键提示。

        扫描策略：
        1. 读取所有元素的 title / aria-label / data-tooltip / data-hotkey 属性
        2. 正则匹配包含快捷键模式的文本（Ctrl+X、Cmd+S、⌘S、⌥K 等）
        3. 返回去重后的 [{text, key_hint, tag, selector}] 列表

        :return: {"ok": True, "shortcuts": [...], "count": N}
        """
        target_id = self.resolve_target(target)
        js = r"""
        (() => {
            const KEY_RE = /(?:Ctrl|Cmd|Meta|Alt|Shift|Option|⌘|⌥|⌃|⇧)[+\s]?\w+|(?:\w+\s+)?(?:⌘|⌥|⌃|⇧)\w+/i;
            const ATTRS = ['title', 'aria-label', 'data-tooltip', 'data-hotkey', 'data-title', 'placeholder'];
            const results = [];
            const seen = new Set();
            for (const el of document.querySelectorAll('*')) {
                for (const attr of ATTRS) {
                    const val = el.getAttribute(attr);
                    if (!val) continue;
                    const m = val.match(KEY_RE);
                    if (!m) continue;
                    const key_hint = m[0];
                    const full_text = val.trim();
                    const dedup = key_hint + '||' + el.tagName;
                    if (seen.has(dedup)) continue;
                    seen.add(dedup);
                    // 构建简单选择器
                    let sel = el.tagName.toLowerCase();
                    if (el.id) sel += '#' + el.id;
                    else if (el.className && typeof el.className === 'string') {
                        const cls = el.className.trim().split(/\s+/)[0];
                        if (cls) sel += '.' + cls;
                    }
                    results.push({
                        text: full_text,
                        key_hint: key_hint,
                        tag: el.tagName.toLowerCase(),
                        attr: attr,
                        selector: sel,
                    });
                }
            }
            return JSON.stringify(results);
        })()
        """
        raw = self._evaluate(target_id, js)
        try:
            shortcuts = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            shortcuts = []
        return {"ok": True, "shortcuts": shortcuts or [], "count": len(shortcuts or [])}

    @staticmethod
    def _export_python_client(requests: list[dict], daemon_script: str = "") -> str:
        """导出为完整可直接运行的 Python 客户端代码。

        与 _export_python 的区别：
        - 自动生成 get_cookies() 调用获取实时 cookie
        - 自动注入 uid / csrf-token 等非标 header 的获取示意
        - 生成完整可运行的 requests.Session 客户端
        """
        # 从第一个请求推断页面 URL 用于 cookies
        first_url = requests[0].get("url", "") if requests else ""
        from urllib.parse import urlparse
        parsed = urlparse(first_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else "<BASE_URL>"

        daemon = daemon_script or "$DAEMON $SCRIPT"

        # 收集所有出现的非标准 header（在所有请求中）
        all_non_std = set()
        STD_SKIP = {
            "host", "connection", "content-length", "accept-encoding", "accept-language",
            "accept", "user-agent", "origin", "referer", "cache-control", "pragma",
            "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform",
            "sec-fetch-dest", "sec-fetch-mode", "sec-fetch-site",
            "upgrade-insecure-requests", "content-type",
        }
        for req in requests:
            for k in req.get("headers", {}):
                if k.lower() not in STD_SKIP:
                    all_non_std.add(k)

        lines = [
            '"""',
            '自动生成的 API 客户端（由 CDP network-capture export --python-client 生成）',
            '',
            '运行前需要：',
            '  1. Chrome CDP daemon 正在运行',
            '  2. 浏览器已登录目标站点；脚本通过 cdp_client SDK 获取实时 cookie',
            '"""',
            '',
            'import json',
            'import sys',
            'import warnings',
            'from pathlib import Path',
            'import requests',
            'import urllib3',
            '',
            '# ---------- SSL 处理（内网自签证书场景）----------',
            '# 如果服务器使用公网受信证书，可将下面两行注释掉',
            'warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)',
            'VERIFY_SSL = False  # 改为 True 或 CA 证书路径以启用验证',
            '',
            '# ---------- 获取实时 Cookie ----------',
            f'DAEMON_SCRIPT = "{daemon_script or "/path/to/daemon.py"}"',
            f'TARGET_URL = "{base_url}"',
            'CDP_DAEMON_SCRIPTS = str(Path(DAEMON_SCRIPT).resolve().parent)',
            'if CDP_DAEMON_SCRIPTS not in sys.path:',
            '    sys.path.insert(0, CDP_DAEMON_SCRIPTS)',
            'from cdp_client import get_auth_material, get_cookies, get_storage, request_auth_token',
            '',
            'live_cookies = get_cookies(TARGET_URL)',
            '',
            '# ---------- 构建 Session ----------',
            'session = requests.Session()',
            'session.verify = VERIFY_SSL  # SSL 证书验证（内网场景通常需要 False）',
            'session.cookies.update(live_cookies)',
            '',
        ]

        # 如果有 uid / csrf-token 等非标 header，给出示例
        if all_non_std:
            lines += [
                '# ---------- 非标认证 Header（从抓包自动提取，请按需更新） ----------',
                '# 以下 header 在原始请求中出现，可能需要动态获取（如 csrf-token 每次页面加载都变）',
                '# 如需 cookie 换 token，可用 request_auth_token(url, method="POST", extract="data.token")',
                '# 如 token 存在 storage，可用 get_storage("token", storage="local" 或 "session")',
            ]
            # 取第一个有这些 header 的请求作为示例值
            example_vals: dict[str, str] = {}
            for req in requests:
                for k in all_non_std:
                    if k not in example_vals and k in req.get("headers", {}):
                        example_vals[k] = req["headers"][k]
            for k in sorted(all_non_std):
                val = example_vals.get(k, "<value>")
                lines.append(f'COMMON_HEADERS_{k.upper().replace("-","_")} = {json.dumps(val)}  # 示例值，需动态更新')
            lines += [
                '',
                '# csrf-token 可从页面 meta 标签动态获取（如适用）：',
                '# from cdp_client import page_call',
                '# meta_val = page_call("active", "Runtime.evaluate", {',
                '#     "expression": \'document.querySelector("meta[name=csrf-token]")?.content\',',
                '#     "returnByValue": True,',
                '# }).get("result", {}).get("value", "")',
                '',
            ]
            # 构建通用 headers 字典
            common_h: dict[str, str] = {}
            for req in requests:
                for k in all_non_std:
                    if k not in common_h and k in req.get("headers", {}):
                        common_h[k] = req["headers"][k]
            lines.append(f'COMMON_HEADERS = {json.dumps(common_h, indent=4, ensure_ascii=False)}')
            lines.append('')

        # 各请求
        for i, req in enumerate(requests):
            method = req.get("method", "GET").upper()
            url = req.get("url", "")
            headers = req.get("headers", {})
            post_data = req.get("postData")
            status = req.get("status", "?")
            body_preview = req.get("responseBody", "")

            lines.append(f"# ══════════════════════════════════════════════")
            lines.append(f"# [{i+1}] {method} {url}")
            lines.append(f"# 响应状态: {status}")
            if body_preview:
                preview = str(body_preview)[:200].replace("\n", " ")
                lines.append(f"# 响应预览: {preview}")

            # 仅保留非标准 header（标准的由 session 自动处理）
            req_specific = {k: v for k, v in headers.items() if k.lower() not in STD_SKIP}
            if all_non_std and req_specific:
                # 去除通用 header，只留该请求特有的
                extra = {k: v for k, v in req_specific.items() if k in all_non_std}
                unique = {k: v for k, v in req_specific.items() if k not in all_non_std}
                h_expr = "COMMON_HEADERS" if all_non_std else "{}"
                if unique:
                    lines.append(f"headers_{i+1} = {{**COMMON_HEADERS, **{json.dumps(unique, ensure_ascii=False)}}}")
                else:
                    lines.append(f"headers_{i+1} = COMMON_HEADERS")
            else:
                lines.append(f"headers_{i+1} = {json.dumps(req_specific, indent=4, ensure_ascii=False)}")

            if method in ("POST", "PUT", "PATCH") and post_data:
                content_type = headers.get("Content-Type", headers.get("content-type", ""))
                if "application/json" in content_type:
                    try:
                        body = json.loads(post_data)
                        lines.append(f"body_{i+1} = {json.dumps(body, indent=4, ensure_ascii=False)}")
                        lines.append(
                            f"resp_{i+1} = session.{method.lower()}("
                            f"\n    {json.dumps(url)},"
                            f"\n    headers=headers_{i+1},"
                            f"\n    json=body_{i+1},"
                            f"\n)"
                        )
                    except json.JSONDecodeError:
                        lines.append(f"data_{i+1} = {json.dumps(post_data)}")
                        lines.append(
                            f"resp_{i+1} = session.{method.lower()}("
                            f"\n    {json.dumps(url)},"
                            f"\n    headers=headers_{i+1},"
                            f"\n    data=data_{i+1},"
                            f"\n)"
                        )
                else:
                    lines.append(f"data_{i+1} = {json.dumps(post_data)}")
                    lines.append(
                        f"resp_{i+1} = session.{method.lower()}("
                        f"\n    {json.dumps(url)},"
                        f"\n    headers=headers_{i+1},"
                        f"\n    data=data_{i+1},"
                        f"\n)"
                    )
            else:
                lines.append(
                    f"resp_{i+1} = session.{method.lower()}("
                    f"\n    {json.dumps(url)},"
                    f"\n    headers=headers_{i+1},"
                    f"\n)"
                )
            lines.append(f"print(f'[{i+1}] {{resp_{i+1}.status_code}} {method} {url[:80]}')")
            lines.append(f"assert resp_{i+1}.ok, f'FAILED: {{resp_{i+1}.status_code}} {{resp_{i+1}.text[:200]}}'")
            lines.append("")

        lines += [
            "print('All requests succeeded!')",
            "",
        ]
        return "\n".join(lines)

    @staticmethod
    def _export_curl(requests: list[dict]) -> str:
        """导出为 curl 命令。"""
        lines = ["#!/bin/bash", "# 自动生成的 curl 命令（由 CDP network-capture 导出）", ""]
        for i, req in enumerate(requests):
            method = req.get("method", "GET").upper()
            url = req.get("url", "")
            headers = req.get("headers", {})
            post_data = req.get("postData")
            status = req.get("status", "?")

            lines.append(f"# [{i+1}] {method} → {status}")
            parts = [f"curl -X {method}"]
            # 关键 headers
            for k, v in headers.items():
                kl = k.lower()
                if kl in ("cookie", "authorization", "content-type",
                          "accept", "referer", "origin", "x-csrf-token",
                          "x-requested-with"):
                    parts.append(f"  -H '{k}: {v}'")
            if post_data:
                escaped = post_data.replace("'", "'\\''")
                parts.append(f"  -d '{escaped}'")
            parts.append(f"  '{url}'")
            lines.append(" \\\n".join(parts))
            lines.append("")
        return "\n".join(lines)

    # =================================================================
    # Action 路由（供 daemon handle_client 调用）
    # =================================================================

    def handle_action(self, action: str, req: dict) -> dict:
        """统一动作分发，供 daemon.handle_client 调用。

        :param action: 动作名（snapshot / click / fill / select / check /
                       press / scroll / wait / get_text / get_url / get_title / page_call）
        :param req: 请求参数字典
        :return: 响应字典 {"ok": True, ...} 或 {"ok": False, "error": ...}
        """
        try:
            target = req.get("target", "active")
            if action in self._MUTATING_TARGET_ACTIONS and isinstance(target, str) and target.startswith("url:"):
                return {
                    "ok": False,
                    "error": "mutating actions do not allow url: target; use tab:alias, active, or exact targetId",
                }

            if action == "target_resolve":
                return self.resolve_target_info(target)

            if action == "page_call":
                method = req.get("method", "")
                if not method:
                    return {"ok": False, "error": "missing 'method'"}
                target_id = self.resolve_target(target)
                result = self.page_call(target_id, method, req.get("params"))
                return {"ok": True, "result": result, "target_id": target_id}

            elif action == "snapshot":
                with_tooltips = req.get("with_tooltips", False)
                result = self.snapshot(
                    target=target,
                    scope=req.get("scope"),
                    include_cursor=req.get("include_cursor", False),
                    compact=req.get("compact", False),
                    depth=req.get("depth"),
                    include_urls=req.get("include_urls", False),
                )
                # -T / --with-tooltips：对无描述的小型元素补全 tooltip
                if with_tooltips and result.get("elements"):
                    try:
                        tooltip_result = self.scan_tooltips(target=target)
                        tooltips_by_pos = {
                            (b["x"], b["y"]): b.get("tooltip", "")
                            for b in tooltip_result.get("buttons", [])
                            if b.get("tooltip")
                        }
                        for el in result["elements"]:
                            if not el.get("desc") or len(el.get("desc", "")) < 2:
                                x, y = el.get("x", 0), el.get("y", 0)
                                # 找最近的 tooltip（±15px 容差）
                                for (tx, ty), tip in tooltips_by_pos.items():
                                    if abs(tx - x) <= 15 and abs(ty - y) <= 15:
                                        el["desc"] = tip
                                        el["desc_src"] = "tooltip"
                                        break
                        result["with_tooltips"] = True
                    except Exception:
                        pass
                return {"ok": True, **result}

            elif action == "click":
                ref = req.get("ref", "")
                at_raw = req.get("at")
                at = tuple(at_raw) if isinstance(at_raw, (list, tuple)) and len(at_raw) == 2 else None
                if not ref and not at:
                    return {"ok": False, "error": "missing 'ref' or 'at'"}
                result = self.click(
                    ref or "__dummy__",
                    target=target,
                    dblclick=req.get("dblclick", False),
                    right=req.get("right", False),
                    at=at,
                    force_js=req.get("force_js", False),
                )
                return {"ok": True, **result}

            elif action == "fill":
                ref = req.get("ref", "")
                text = req.get("text", "")
                at_raw = req.get("at")
                if not ref and not at_raw:
                    return {"ok": False, "error": "missing 'ref' or 'at'"}
                # 支持 --at 坐标：先 CDP 鼠标点击该坐标聚焦，再 fill
                if at_raw and len(at_raw) == 2 and (not ref or ref in ("__dummy__", "dummy")):
                    x, y = int(at_raw[0]), int(at_raw[1])
                    tid = self.resolve_target(target)
                    # 点击坐标聚焦
                    self._cdp_mouse_click(tid, x, y)
                    import time as _time; _time.sleep(0.15)
                    # 找当前聚焦的 input
                    focused_sel = self._evaluate(tid, """
                        (() => {
                            var el = document.activeElement;
                            if (!el || (el.tagName !== 'INPUT' && el.tagName !== 'TEXTAREA'
                                        && el.contentEditable !== 'true')) return '';
                            return el.tagName.toLowerCase() + (el.id ? '#' + CSS.escape(el.id) : '');
                        })()
                    """)
                    if focused_sel:
                        ref = focused_sel
                    else:
                        # 兜底：找坐标最近的 input
                        ref = self._evaluate(tid, f"""
                            (() => {{
                                var els = [...document.querySelectorAll('input,textarea,[contenteditable]')];
                                var best = null, bestDist = 9999;
                                for (var el of els) {{
                                    var r = el.getBoundingClientRect();
                                    if (r.width === 0) continue;
                                    var cx = r.left + r.width/2, cy = r.top + r.height/2;
                                    var d = Math.abs(cx - {x}) + Math.abs(cy - {y});
                                    if (d < bestDist) {{ bestDist = d; best = el; }}
                                }}
                                if (!best) return '';
                                return best.tagName.toLowerCase() + (best.id ? '#' + CSS.escape(best.id) : '');
                            }})()
                        """)
                if not ref:
                    return {"ok": False, "error": "fill: could not locate input at given coordinates"}
                result = self.fill(
                    ref, text,
                    clear=req.get("clear", True),
                    native=req.get("native", False),
                    submit=req.get("submit", False),
                    target=target,
                )
                return {"ok": True, **result}

            elif action == "select":
                ref = req.get("ref", "")
                value = req.get("value", "")
                if not ref or not value:
                    return {"ok": False, "error": "missing 'ref' or 'value'"}
                result = self.select(
                    ref, value,
                    by_label=req.get("by_label", False),
                    search_text=req.get("search_text"),
                    target=target,
                )
                return {"ok": True, **result}

            elif action == "check":
                ref = req.get("ref", "")
                if not ref:
                    return {"ok": False, "error": "missing 'ref'"}
                result = self.check(
                    ref,
                    checked=req.get("checked"),
                    target=target,
                )
                return {"ok": True, **result}

            elif action == "hover":
                at_raw = req.get("at")
                at = tuple(at_raw) if isinstance(at_raw, (list, tuple)) and len(at_raw) == 2 else None
                result = self.hover(
                    ref_or_selector=req.get("ref"),
                    at=at,
                    target=target,
                )
                return {"ok": True, **result}

            elif action == "press":
                key = req.get("key", "")
                if not key:
                    return {"ok": False, "error": "missing 'key'"}
                result = self.press(key, req.get("ref"), target=target)
                return {"ok": True, **result}

            elif action == "scroll":
                # 解析 at 坐标
                at_raw = req.get("at")
                at = tuple(at_raw) if isinstance(at_raw, (list, tuple)) and len(at_raw) == 2 else None
                result = self.scroll(
                    direction=req.get("direction", "down"),
                    amount=req.get("amount", 500),
                    ref_or_selector=req.get("ref") or req.get("selector"),
                    at=at,
                    target=target,
                )
                return {"ok": True, **result}

            elif action == "drag":
                sx = req.get("start_x") or req.get("sx", 0)
                sy = req.get("start_y") or req.get("sy", 0)
                ex = req.get("end_x") or req.get("ex", 0)
                ey = req.get("end_y") or req.get("ey", 0)
                if not (sx and sy and ex and ey):
                    return {"ok": False, "error": "missing start_x/start_y/end_x/end_y"}
                return self.drag(
                    start_x=int(sx), start_y=int(sy),
                    end_x=int(ex), end_y=int(ey),
                    target=target,
                    steps=req.get("steps", 10),
                    hold_ms=req.get("hold_ms", 100),
                )

            elif action == "wait":
                result = self.wait_for(
                    selector=req.get("selector"),
                    text=req.get("text"),
                    timeout_ms=req.get("timeout_ms", 10000),
                    target=target,
                )
                return {"ok": True, **result}

            elif action == "get_text":
                text = self.get_text(req.get("ref"), target=target)
                return {"ok": True, "text": text}

            elif action == "get_url":
                url = self.get_url(target=target)
                return {"ok": True, "url": url}

            elif action == "get_title":
                title = self.get_title(target=target)
                return {"ok": True, "title": title}

            elif action == "screenshot":
                return self.screenshot(
                    target=target,
                    path=req.get("path", ""),
                    annotate=req.get("annotate", False),
                    full_page=req.get("full_page", False),
                )

            elif action == "diagnose_page":
                return self.diagnose_page(
                    target=target,
                    path=req.get("path", ""),
                    full_page=req.get("full_page", False),
                    wait_ms=int(req.get("wait_ms", 0) or 0),
                )

            elif action == "activate":
                result = self.activate(target=target)
                return result

            elif action == "open_tab":
                url = req.get("url", "")
                if not url:
                    return {"ok": False, "error": "missing 'url'"}
                result = self.open_tab(
                    url,
                    wait_ms=req.get("wait_ms", 3000),
                    activate=req.get("activate", True),
                    group=req.get("group", ""),
                    requested_group=req.get("requested_group", ""),
                    alias=req.get("alias", ""),
                )
                return result

            elif action == "close_tab":
                result = self.close_tab(target=target)
                return result

            elif action == "tab_bind":
                name = req.get("name", "")
                if not name:
                    return {"ok": False, "error": "missing 'name'"}
                return self.bind_tab(name, target=target)

            elif action == "tab_get":
                name = req.get("name", "")
                if not name:
                    return {"ok": False, "error": "missing 'name'"}
                return self.get_tab_binding(name)

            elif action == "tab_list":
                return self.list_tab_bindings()

            elif action == "tab_remove":
                name = req.get("name", "")
                if not name:
                    return {"ok": False, "error": "missing 'name'"}
                return self.remove_tab_binding(name)

            elif action == "group_create":
                name = req.get("name", "")
                if not name:
                    return {"ok": False, "error": "missing 'name'"}
                return self.group_create(
                    name,
                    targets=req.get("targets"),
                    color=req.get("color", ""),
                )

            elif action == "group_add":
                name = req.get("name", "")
                targets = req.get("targets", [])
                if not name or not targets:
                    return {"ok": False, "error": "missing 'name' or 'targets'"}
                return self.group_add(name, targets)

            elif action == "group_remove_tab":
                name = req.get("name", "")
                targets = req.get("targets", [])
                if not name or not targets:
                    return {"ok": False, "error": "missing 'name' or 'targets'"}
                return self.group_remove_tab(name, targets)

            elif action == "group_list":
                return self.group_list(name=req.get("name", ""))

            elif action == "group_close":
                name = req.get("name", "")
                if not name:
                    return {"ok": False, "error": "missing 'name'"}
                return self.group_close(name)

            elif action == "group_delete":
                name = req.get("name", "")
                if not name:
                    return {"ok": False, "error": "missing 'name'"}
                return self.group_delete(name)

            elif action == "group_activate":
                name = req.get("name", "")
                if not name:
                    return {"ok": False, "error": "missing 'name'"}
                return self.group_activate(name)

            elif action == "group_move":
                name = req.get("name", "")
                targets = req.get("targets", [])
                if not name or not targets:
                    return {"ok": False, "error": "missing 'name' or 'targets'"}
                return self.group_move(name, targets)

            elif action == "group_close_tabs":
                name = req.get("name", "")
                targets = req.get("targets", [])
                if not name or not targets:
                    return {"ok": False, "error": "missing 'name' or 'targets'"}
                return self.group_close_tabs(name, targets)

            elif action == "find_text":
                text = req.get("text", "")
                if not text:
                    return {"ok": False, "error": "missing 'text'"}
                return self.find_text(
                    text,
                    target=target,
                    tag=req.get("tag", ""),
                    region=req.get("region", ""),
                )

            elif action == "click_text":
                text = req.get("text", "")
                if not text:
                    return {"ok": False, "error": "missing 'text'"}
                result = self.click_text(
                    text,
                    target=target,
                    tag=req.get("tag", ""),
                    dblclick=req.get("dblclick", False),
                    right=req.get("right", False),
                    nth=req.get("nth", 1),
                    region=req.get("region", ""),
                )
                return {"ok": True, **result}

            # ---- Monaco/CodeMirror 编辑器 ----
            elif action == "editor_get":
                return self.editor_get(target=target)

            elif action == "editor_set":
                text = req.get("text", "")
                if not text and text != "":
                    return {"ok": False, "error": "missing 'text'"}
                return self.editor_set(text, target=target, append=req.get("append", False))

            elif action == "editor_type":
                text = req.get("text", "")
                if not text:
                    return {"ok": False, "error": "missing 'text'"}
                return self.editor_type(text, target=target)

            # ---- 图标搜索 / 点击 ----
            elif action == "find_icon":
                query = req.get("query", "")
                if not query:
                    return {"ok": False, "error": "missing 'query'"}
                matches = self.find_icon(query, target=target, region=req.get("region", ""))
                return {"ok": True, "matches": matches, "count": len(matches)}

            elif action == "click_icon":
                query = req.get("query", "")
                if not query:
                    return {"ok": False, "error": "missing 'query'"}
                return self.click_icon(
                    query, target=target,
                    region=req.get("region", ""),
                    nth=req.get("nth", 1),
                    dblclick=req.get("dblclick", False),
                    right=req.get("right", False),
                )

            elif action == "scan_tooltips":
                return self.scan_tooltips(
                    target=target,
                    region=req.get("region", ""),
                    scope=req.get("scope", ""),
                )

            elif action == "network_capture_start":
                result = self.network_capture_start(
                    target=target,
                    follow=req.get("follow", False),
                )
                return result

            elif action == "navigate_page":
                return self.navigate_page(
                    target=target,
                    url=req.get("url"),
                    reload=bool(req.get("reload", False)),
                )

            elif action == "reload_page":
                return self.navigate_page(target=target, reload=True)

            elif action == "network_capture_stop":
                result = self.network_capture_stop(
                    target=target,
                    get_body=req.get("get_body", True),
                    wait_ms=int(req.get("wait_ms") or 0),
                    body_mode=str(req.get("body_mode") or ""),
                    idle_ms=int(req.get("idle_ms") or 0),
                    max_bodies=int(req.get("max_bodies") or 0),
                    max_body_bytes=int(req.get("max_body_bytes") or 0),
                    method_filter=str(req.get("method_filter") or ""),
                    url_filter=str(req.get("url_filter") or ""),
                    exclude_domain=str(req.get("exclude_domain") or ""),
                    status_filter=str(req.get("status_filter") or ""),
                    until_match=str(req.get("until_match") or ""),
                )
                return result

            elif action == "network_capture_peek":
                result = self.network_capture_peek(target=target)
                return result

            elif action == "network_capture_export":
                requests = req.get("requests", [])
                fmt = req.get("format", "python")
                code = self.network_capture_export(requests, fmt=fmt)
                return {"ok": True, "code": code, "format": fmt}

            elif action == "network_fetch":
                url = req.get("url", "")
                if not url:
                    return {"ok": False, "error": "missing 'url'"}
                return self.network_fetch(
                    url=url,
                    method=req.get("method", "GET"),
                    headers=req.get("headers"),
                    body=req.get("body", ""),
                    target=target,
                )

            elif action == "network_replay":
                return self.network_replay(
                    index=req.get("index", 1),
                    target=target,
                    override_url=req.get("override_url", ""),
                    override_method=req.get("override_method", ""),
                    override_body=req.get("override_body", ""),
                )

            elif action == "local_storage_get":
                return self.local_storage_get(
                    key=req.get("key", ""),
                    storage=req.get("storage", "local"),
                    target=target,
                )

            elif action == "local_storage_set":
                key = req.get("key", "")
                value = req.get("value", "")
                if not key:
                    return {"ok": False, "error": "missing 'key'"}
                return self.local_storage_set(
                    key=key, value=value,
                    storage=req.get("storage", "local"),
                    target=target,
                )

            elif action == "local_storage_remove":
                key = req.get("key", "")
                if not key:
                    return {"ok": False, "error": "missing 'key'"}
                return self.local_storage_remove(
                    key=key,
                    storage=req.get("storage", "local"),
                    target=target,
                )

            elif action == "extract_metric":
                return self.extract_metric(
                    title=req.get("title", ""),
                    api_url=req.get("api_url", ""),
                    target=target,
                )

            elif action == "eval_js":
                expression = req.get("expression", "")
                if not expression:
                    return {"ok": False, "error": "missing 'expression'"}
                return self.eval_js(
                    expression=expression,
                    await_promise=req.get("await_promise", False),
                    target=target,
                )

            elif action == "capture_headers":
                return self.capture_headers(
                    url_filter=req.get("url_filter", ""),
                    wait_sec=req.get("wait_sec", 10),
                    target=target,
                )

            elif action == "scan_shortcuts":
                return self.scan_shortcuts(target=target)

            else:
                return {"ok": False, "error": f"unknown page action: {action}"}

        except Exception as exc:
            return {"ok": False, "error": str(exc)}
