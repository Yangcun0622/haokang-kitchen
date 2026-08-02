/* ============================================================
   撇脱私厨 · 杨存私厨工作台 —— 逻辑
   ============================================================ */
(function () {
  'use strict';
  const D = window.__DATA__;
  const $ = (s, r) => (r || document).querySelector(s);
  const $$ = (s, r) => Array.from((r || document).querySelectorAll(s));
  const uid = () => 'x' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);

  // ---------- 数据存储 ----------
  const KEY = 'haokang_kitchen_v1';
  let DB = load();

  function load() {
    try {
      const raw = localStorage.getItem(KEY);
      if (raw) return JSON.parse(raw);
    } catch (e) {}
    return {
      customers: D.SEED_CUSTOMERS.slice(),
      orders: D.SEED_ORDERS.slice(),
      expenses: D.SEED_EXPENSES.slice(),
      cleared: false
    };
  }
  let persistOK = true;
  function save() {
    try { localStorage.setItem(KEY, JSON.stringify(DB)); lastSnap = localStorage.getItem(KEY) || ''; } catch (e) { persistOK = false; }
    if (bc) { try { bc.postMessage({ type: 'sync' }); } catch (e) {} }
  }
  function storageAvailable() { try { localStorage.setItem(KEY + '__t', '1'); localStorage.removeItem(KEY + '__t'); return true; } catch (e) { return false; } }

  // ---------- 实时同步（同源：手机点单 → 电脑后台即时刷新） ----------
  // 说明：同一浏览器/同源多标签页（如厨房电脑同时开着后台与点菜页）可秒级同步；
  // 不同设备（手机≠电脑浏览器）的 localStorage 不互通，仍需「复制完整订单 → 导入」。
  let currentPage = 'recipe';
  let newOrderBadge = 0;
  let lastRefresh = 0;
  let lastSnap = '';
  try { lastSnap = localStorage.getItem(KEY) || ''; } catch (e) {}
  const bc = ('BroadcastChannel' in window) ? new BroadcastChannel('pk_sync') : null;

  function toast(msg) {
    let t = document.getElementById('toast');
    if (!t) { t = document.createElement('div'); t.id = 'toast'; t.className = 'toast no-print'; document.body.appendChild(t); }
    t.textContent = msg; t.classList.add('show');
    clearTimeout(toast._t); toast._t = setTimeout(function () { t.classList.remove('show'); }, 3200);
  }
  function updateOrderBadge() {
    const a = document.querySelector('.nav a[data-p="order"]');
    if (!a) return;
    let b = a.querySelector('.nav-badge');
    if (newOrderBadge > 0) {
      if (!b) { b = document.createElement('span'); b.className = 'nav-badge'; a.appendChild(b); }
      b.textContent = newOrderBadge > 99 ? '99+' : newOrderBadge;
    } else if (b) { b.remove(); }
  }
  function refreshFromExternal(opts) {
    const now = Date.now();
    if (now - lastRefresh < 600) return;   // 防止 storage + bc 双触发造成重复刷新
    lastRefresh = now;
    let raw;
    try { raw = localStorage.getItem(KEY); } catch (e) { return; }
    if (raw === null) return;
    let next; try { next = JSON.parse(raw); } catch (e) { return; }
    const prevIds = {}; (DB.orders || []).forEach(function (o) { if (o && o.id) prevIds[o.id] = 1; });
    DB = next; lastSnap = raw;
    const added = (DB.orders || []).filter(function (o) { return o && o.id && !prevIds[o.id]; }).length;
    if (currentPage === 'recipe') renderRecipes();
    else if (currentPage === 'customer') renderCustomers();
    else if (currentPage === 'order') renderOrders();
    else if (currentPage === 'daily') renderDaily();
    else if (currentPage === 'expense') renderExpenses();
    else if (currentPage === 'print') renderPrint();
    else if (currentPage === 'done') renderDone();
    if (added > 0 && (!opts || !opts.silent)) {
      newOrderBadge += added; updateOrderBadge();
      toast('📥 收到 ' + added + ' 笔新订单' + (currentPage === 'order' ? '' : '（见「订单出餐」）'));
    }
  }
  if (bc) bc.onmessage = function (e) { if (e && e.data && (e.data.type === 'order' || e.data.type === 'sync')) refreshFromExternal(); };
  window.addEventListener('storage', function (e) { if (e.key === KEY) refreshFromExternal(); });
  setInterval(function () {
    try { const s = localStorage.getItem(KEY) || ''; if (s !== lastSnap) refreshFromExternal({ silent: true }); } catch (e) {}
  }, 3000);

  // ---------- 跨设备实时接单（公共 MQTT broker 中转，无需服务器） ----------
  const MQTT_URL = 'wss://broker.emqx.io:8084/mqtt';
  const CHANNEL = 'haokang/orders';   // 跨设备频道；多人共用可改此值避免串单
  let mqttClient = null;
  function setSyncDot(ok) {
    const dot = document.getElementById('sync_dot');
    if (!dot) return;
    dot.classList.toggle('off', !ok);
    dot.title = ok ? '跨设备实时接单已连接（手机点单电脑秒收）' : '实时接单未连接（请检查网络）';
  }
  function setupMqtt() {
    if (typeof mqtt === 'undefined') { setSyncDot(false); return; }
    try {
      mqttClient = mqtt.connect(MQTT_URL, {
        clientId: 'pk_backend_' + Math.random().toString(16).slice(2, 12),
        clean: true, reconnectPeriod: 4000, connectTimeout: 8000
      });
      mqttClient.on('connect', function () {
        mqttClient.subscribe(CHANNEL, { qos: 0 }, function (e) { if (!e) setSyncDot(true); });
      });
      mqttClient.on('error', function () { setSyncDot(false); });
      mqttClient.on('reconnect', function () { setSyncDot(false); });
      mqttClient.on('close', function () { setSyncDot(false); });
      mqttClient.on('message', function (topic, payload) {
        if (topic !== CHANNEL) return;
        let msg; try { msg = JSON.parse(payload.toString()); } catch (e) { return; }
        if (!msg || msg.type !== 'order') return;
        const newCust = (msg.customers || []).filter(function (c) { return c && c.id && !DB.customers.some(function (x) { return x.id === c.id; }); });
        const newOrd = (msg.orders || []).filter(function (o) { return o && o.id && !DB.orders.some(function (x) { return x.id === o.id; }); });
        if (!newCust.length && !newOrd.length) return;
        DB.customers = DB.customers.concat(newCust);
        DB.orders = DB.orders.concat(newOrd);
        save();
        const added = newOrd.length;
        newOrderBadge += added; updateOrderBadge();
        const rend = { recipe: renderRecipes, customer: renderCustomers, order: renderOrders, daily: renderDaily, expense: renderExpenses, print: renderPrint, done: renderDone }[currentPage];
        if (rend) rend();
        if (added) toast('📥 收到 ' + added + ' 笔新订单' + (currentPage === 'order' ? '' : '（见「订单出餐」）'));
      });
    } catch (e) { setSyncDot(false); }
  }

  const dishById = id => D.DISHES.find(x => x.id === id);
  const catById = id => D.CATEGORIES.find(x => x.id === id);
  const custById = id => DB.customers.find(x => x.id === id);

  // 时间工具：所有时间统一精确到秒
  const pad2 = n => String(n).padStart(2, '0');
  function nowStamp(d) {
    d = d || new Date();
    return d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate()) + ' ' +
      pad2(d.getHours()) + ':' + pad2(d.getMinutes()) + ':' + pad2(d.getSeconds());
  }
  function withSeconds(t) {
    if (!t) return '';
    t = String(t).trim();
    if (/^\d{1,2}:\d{1,2}$/.test(t)) return t + ':00';
    return t;
  }

  // ---------- 导航 ----------
  function go(page) {
    currentPage = page;
    if (page === 'order') { newOrderBadge = 0; updateOrderBadge(); }
    $$('.nav a').forEach(a => a.classList.toggle('active', a.dataset.p === page));
    $$('.page').forEach(p => p.classList.toggle('active', p.id === 'p_' + page));
    if (page === 'recipe') renderRecipes();
    if (page === 'customer') renderCustomers();
    if (page === 'order') renderOrders();
    if (page === 'daily') renderDaily();
    if (page === 'expense') renderExpenses();
    if (page === 'print') renderPrint();
    if (page === 'done') renderDone();
  }

  // ---------- 菜品库 ----------
  let curCat = 'all';
  function renderRecipes() {
    const chips = ['<button class="btn sm ' + (curCat === 'all' ? 'gold' : 'ghost') + '" data-c="all">全部</button>']
      .concat(D.CATEGORIES.map(c => '<button class="btn sm ' + (curCat === c.id ? 'gold' : 'ghost') + '" data-c="' + c.id + '">' + c.name + '</button>'));
    $('#recipe_chips').innerHTML = chips.join(' ');
    $$('#recipe_chips button').forEach(b => b.onclick = () => { curCat = b.dataset.c; renderRecipes(); });

    const list = D.DISHES.filter(d => curCat === 'all' || d.cat === curCat);
    $('#recipe_cards').innerHTML = list.map(d => {
      const c = catById(d.cat);
      const img = d.img ? '<img class="rcp-img" src="' + d.img + '" alt="' + d.name + '" onerror="this.style.display=\'none\'">' : '';
      return '<div class="card">' + img +
        '<h3>' + d.name + '</h3>' +
        '<span class="tag">' + c.name + '</span>' +
        '<p style="font-size:13px;color:#6b5f52">' + d.intro + '</p>' +
        '<button class="btn sm" onclick="APP.openDish(\'' + d.id + '\')">查看配方 / 打印</button></div>';
    }).join('');
  }

  window.APP = {
    openDish(id) {
      const d = dishById(id);
      const c = catById(d.cat);
      const ing = d.ing.map(i => '<li>· ' + i.n + ' —— <b>' + i.q + '</b></li>').join('');
      const sea = d.sea.map(s => '<li>· ' + s.n + ' —— <b>' + s.g + 'g</b></li>').join('');
      const steps = d.steps.map((s, i) =>
        '<div class="step"><div class="n">' + (i + 1) + '</div><div class="pic">' + s.icon + '</div>' +
        '<div class="desc"><b>' + s.t + '</b><span>' + s.d + '</span></div></div>').join('');
      $('#m_body').innerHTML =
        '<h3>' + d.name + ' <span class="tag">' + c.name + '</span></h3>' +
        (d.img ? '<img class="m-img" src="' + d.img + '" alt="' + d.name + '" onerror="this.style.display=\'none\'">' : '') +
        '<p style="margin:6px 0 12px;color:#6b5f52">' + d.intro + '</p>' +
        '<div class="sec"><div class="sec-t">食材清单</div><ul>' + ing + '</ul></div>' +
        '<div class="sec"><div class="sec-t">调味料（精确到克）</div><ul>' + sea + '</ul></div>' +
        '<div class="sec"><div class="sec-t">制作步骤（图文）</div>' + steps + '</div>';
      $('#m_title').textContent = '菜品配方 · ' + d.name;
      $('#m_print').style.display = 'inline-block';
      $('#m_print').onclick = () => printDish(id);
      showModal();
    }
  };

  function printDish(id) {
    const d = dishById(id);
    const c = catById(d.cat);
    const ing = d.ing.map(i => '<li>· ' + i.n + ' —— <b>' + i.q + '</b></li>').join('');
    const sea = d.sea.map(s => '<li>· ' + s.n + ' —— <b>' + s.g + 'g</b></li>').join('');
    const steps = d.steps.map((s, i) =>
      '<div class="step"><div class="n">' + (i + 1) + '</div><div class="pic">' + s.icon + '</div>' +
      '<div class="desc"><b>' + s.t + '</b><span>' + s.d + '</span></div></div>').join('');
    const html =
      '<div class="ticket"><div class="hd"><b>' + d.name + '</b><br><span>' + c.name + ' · 撇脱私厨</span></div>' +
      '<p style="font-size:13px;color:#555;margin:6px 0">' + d.intro + '</p>' +
      '<div class="sec"><div class="sec-t">食材清单</div><ul>' + ing + '</ul></div>' +
      '<div class="sec"><div class="sec-t">调味料（精确到克）</div><ul>' + sea + '</ul></div>' +
      '<div class="sec"><div class="sec-t">制作步骤（图文）</div>' + steps + '</div>' +
      '<div class="ft">撇脱私厨 · 杨存私厨 · 适配重庆口味 · 打印于 ' + nowStamp() + '</div></div>';
    openPrintWindow(html, d.name + ' 菜谱');
  }

  // ---------- 顾客管理 ----------
  function renderCustomers() {
    const rows = DB.customers.map(c =>
      '<tr><td>' + c.name + '</td><td>' + (c.phone || '-') + '</td><td>' + c.address + '</td>' +
      '<td>' + (c.offWork || '-') + '</td><td>' + (c.taboo || '-') + '</td>' +
      '<td><button class="btn sm ghost" onclick="APP.editCust(\'' + c.id + '\')">编辑</button> ' +
      '<button class="btn sm ghost" style="color:#c0392b;border-color:#c0392b" onclick="APP.delCust(\'' + c.id + '\')">删除</button></td></tr>'
    ).join('');
    $('#cust_table').innerHTML = rows || '<tr><td colspan="6" class="empty">还没有顾客，先添加一位</td></tr>';
  }
  window.APP.editCust = function (id) {
    const c = custById(id); if (!c) return;
    $('#f_cust_name').value = c.name; $('#f_cust_phone').value = c.phone || '';
    $('#f_cust_addr').value = c.address; $('#f_cust_off').value = c.offWork || '';
    $('#f_cust_taboo').value = c.taboo || '';
    $('#cust_form').dataset.eid = id;
    $('#cust_submit').textContent = '保存修改';
  };
  window.APP.delCust = function (id) {
    if (!confirm('确认删除该顾客？')) return;
    DB.customers = DB.customers.filter(c => c.id !== id); save(); renderCustomers();
  };

  // ---------- 订单 / 出餐 ----------
  let dishOutsideHandler = null;
  function renderDishPicker() {
    const list = document.getElementById('o_dish_list');
    if (!list) return;
    const draw = (kw) => {
      kw = (kw || '').trim().toLowerCase();
      const items = kw ? D.DISHES.filter(d => d.name.toLowerCase().indexOf(kw) > -1) : D.DISHES;
      list.innerHTML = items.map(d =>
        '<div class="dish-opt" data-id="' + d.id + '" data-name="' + d.name + '">' +
        '<span class="dn">' + d.name + '</span><span class="dc">' + catById(d.cat).name + '</span></div>'
      ).join('') || '<div class="dish-empty">没找到匹配菜品</div>';
      list.querySelectorAll('.dish-opt').forEach(el => {
        el.onclick = () => {
          document.getElementById('o_dish').value = el.dataset.id;
          document.getElementById('o_dish_search').value = el.dataset.name;
          list.style.display = 'none';
        };
      });
    };
    draw('');
    list.style.display = 'none';
    const sb = document.getElementById('o_dish_search');
    sb.oninput = e => { list.style.display = 'block'; draw(e.target.value); };
    sb.onfocus = () => { list.style.display = 'block'; draw(sb.value); };
    if (dishOutsideHandler) document.removeEventListener('click', dishOutsideHandler);
    dishOutsideHandler = e => { if (!e.target.closest('.dish-picker')) list.style.display = 'none'; };
    document.addEventListener('click', dishOutsideHandler);
  }

  function genOrderCode() {
    let c = String(Math.floor(100000 + Math.random() * 900000));
    const used = {}; DB.orders.forEach(o => { if (o && o.code) used[o.code] = 1; });
    let g = 0;
    while (used[c] && g < 200) { c = String(Math.floor(100000 + Math.random() * 900000)); g++; }
    return c;
  }
  function renderOrders() {
    const optC = DB.customers.map(c => '<option value="' + c.id + '">' + c.name + '（' + c.address + '）</option>').join('');
    $('#o_cust').innerHTML = optC || '<option>先去添加顾客</option>';
    renderDishPicker();
    const rows = DB.orders.slice().reverse().map(o => {
      const c = custById(o.custId), d = dishById(o.dishId);
      return '<tr><td class="no-print"><input type="checkbox" class="o-chk" value="' + o.id + '"></td>' +
        '<td>' + (o.ts || o.date) + '</td><td>' + (c ? c.name : '?') + '</td><td>' + (d ? d.name : '?') + ' ×' + o.qty + '</td>' +
        '<td class="code-cell">' + (o.code || '-') + '</td>' +
        '<td>' + (o.printed ? '<span class="badge done">已打印</span>' : '<span class="badge pend">待打印</span>') + '</td>' +
        '<td>' + (withSeconds(o.deliverAt) || '-') + '</td><td>' + (o.taboo || '-') + '</td>' +
        '<td><button class="btn sm" onclick="APP.previewOrder(\'' + o.id + '\')">出餐单</button> ' +
        '<button class="btn sm ghost" style="color:#c0392b;border-color:#c0392b" onclick="APP.delOrder(\'' + o.id + '\')">删</button></td></tr>';
    }).join('');
    $('#order_table').innerHTML = rows || '<tr><td colspan="9" class="empty">还没有订单</td></tr>';
    const selAll = $('#o_selall'), info = $('#o_sel_info');
    if (rows) {
      const checks = $$('#order_table .o-chk');
      function updCount() {
        const n = checks.filter(ch => ch.checked).length;
        if (info) info.textContent = n ? ('已选 ' + n + ' 条') : '勾选左侧复选框可批量删除';
        if (selAll) selAll.checked = n > 0 && n === checks.length;
      }
      if (selAll) selAll.onchange = function () { checks.forEach(ch => ch.checked = selAll.checked); updCount(); };
      checks.forEach(ch => ch.onchange = updCount);
    } else {
      if (selAll) selAll.checked = false;
      if (info) info.textContent = '勾选左侧复选框可批量删除';
    }
    const delBtn = $('#o_del_sel');
    if (delBtn) delBtn.onclick = function () { window.APP.delSelected(); };
  }
  window.APP.previewOrder = function (id) {
    const o = DB.orders.find(x => x.id === id);
    if (!o) return;
    if (o.printed) { renderDone(); go('done'); }
    else { renderPrint(id); go('print'); }
  };
  window.APP.delOrder = function (id) {
    if (!confirm('确认删除该订单？')) return;
    DB.orders = DB.orders.filter(o => o.id !== id); save(); renderOrders();
  };
  window.APP.delSelected = function () {
    const ids = $$('#order_table .o-chk:checked').map(ch => ch.value);
    if (!ids.length) return alert('请先勾选要删除的订单');
    if (!confirm('确认删除选中的 ' + ids.length + ' 笔订单？此操作不可撤销')) return;
    DB.orders = DB.orders.filter(o => ids.indexOf(o.id) < 0);
    save(); renderOrders();
  };

  // 跨设备接单：粘贴顾客点菜页生成的订单码，追加进本库（不覆盖现有数据）
  window.APP.importCustomerOrder = function () {
    const t = document.getElementById('paste_order').value.trim();
    if (!t) return alert('请先粘贴顾客发来的订单码');
    let data;
    try { data = JSON.parse(t); } catch (e) { return alert('订单码格式不正确，请确认完整复制'); }
    if (!data || !Array.isArray(data.customers) || !Array.isArray(data.orders)) {
      return alert('订单码缺少顾客或订单数据');
    }
    const custMap = {};
    data.customers.forEach(c => {
      const nid = uid(); custMap[c.id] = nid;
      DB.customers.push({ id: nid, code: c.code || '', name: c.name || '顾客', phone: c.phone || '', address: c.address || '', offWork: c.offWork || '', taboo: c.taboo || '' });
    });
    data.orders.forEach(o => {
      DB.orders.push({
        id: uid(), date: o.date || new Date().toISOString().slice(0, 10),
        ts: o.ts || nowStamp(),
        custId: custMap[o.custId] || o.custId, dishId: o.dishId,
        qty: Number(o.qty) || 1, price: Number(o.price) || 0,
        deliverAt: o.deliverAt || '', taboo: o.taboo || '', note: o.note || '',
        code: o.code || ''
      });
    });
    save(); renderCustomers(); renderOrders(); renderPrint();
    document.getElementById('paste_order').value = '';
    alert('已导入 ' + data.orders.length + ' 单，可在「打印中心」生成出餐单');
  };

  // ---------- 店铺支出 ----------
  function renderExpenses() {
    const rows = DB.expenses.slice().reverse().map(e =>
      '<tr><td>' + e.date + '</td><td>' + e.cat + '</td><td class="kpi-red">¥' + e.amount + '</td><td>' + (e.note || '-') + '</td>' +
      '<td><button class="btn sm ghost" style="color:#c0392b;border-color:#c0392b" onclick="APP.delExp(\'' + e.id + '\')">删</button></td></tr>'
    ).join('');
    $('#exp_table').innerHTML = rows || '<tr><td colspan="5" class="empty">还没有支出记录</td></tr>';
    const total = DB.expenses.reduce((a, e) => a + Number(e.amount), 0);
    $('#exp_total').textContent = '累计支出 ¥' + total;
  }
  window.APP.delExp = function (id) {
    DB.expenses = DB.expenses.filter(e => e.id !== id); save(); renderExpenses();
  };

  // ---------- 数据导出 / 导入（应对 file:// 下无法持久化） ----------
  window.APP.exportData = function () {
    const blob = new Blob([JSON.stringify(DB, null, 2)], { type: 'application/json' });
    const a = document.createElement('a');
    const url = URL.createObjectURL(blob);
    a.href = url; a.download = '撇脱私厨数据_' + new Date().toISOString().slice(0, 10) + '.json';
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  };
  window.APP.importData = function (file) {
    if (!file) return;
    const r = new FileReader();
    r.onload = () => {
      try {
        const data = JSON.parse(r.result);
        if (data && Array.isArray(data.customers) && Array.isArray(data.orders) && Array.isArray(data.expenses)) {
          DB = { customers: data.customers, orders: data.orders, expenses: data.expenses, cleared: !!data.cleared };
          save(); renderCustomers(); renderOrders(); renderDaily(); renderExpenses(); renderPrint();
          alert('数据已导入（' + DB.customers.length + ' 位顾客 / ' + DB.orders.length + ' 单 / ' + DB.expenses.length + ' 笔支出）');
        } else alert('文件格式不正确，请导入本工作台导出的 JSON');
      } catch (e) { alert('解析失败：' + e.message); }
    };
    r.readAsText(file);
  };

  // ---------- 每日业绩 ----------
  function renderDaily() {
    const map = {};
    DB.orders.forEach(o => {
      const d = dishById(o.dishId); if (!d) return;
      if (!map[o.date]) map[o.date] = { orders: 0, qty: 0, revenue: 0 };
      map[o.date].orders++; map[o.date].qty += Number(o.qty);
      map[o.date].revenue += Number(o.qty) * (Number(o.price) || 0);
    });
    const dates = Object.keys(map).sort().reverse();
    let tRev = 0, tExp = 0, tQty = 0, tOrd = 0;
    const rows = dates.map(dt => {
      const m = map[dt];
      const exp = DB.expenses.filter(e => e.date === dt).reduce((a, e) => a + Number(e.amount), 0);
      const profit = m.revenue - exp;
      const avg = (m.revenue / m.orders).toFixed(1);
      tRev += m.revenue; tExp += exp; tQty += m.qty; tOrd += m.orders;
      return '<tr><td>' + dt + '</td><td>' + m.orders + '</td><td>' + m.qty + '</td>' +
        '<td class="kpi-red">¥' + m.revenue + '</td><td>¥' + exp + '</td>' +
        '<td class="' + (profit >= 0 ? 'kpi-green' : 'kpi-red') + '">¥' + profit + '</td><td>¥' + avg + '</td></tr>';
    }).join('');
    $('#daily_table').innerHTML = rows || '<tr><td colspan="7" class="empty">还没有业绩数据，去「订单出餐」录一单</td></tr>';
    const tProfit = tRev - tExp;
    $('#daily_stat').innerHTML =
      stat(tOrd, '总订单数') + stat(tQty, '总份数') + stat('¥' + tRev, '总营业额') +
      stat('¥' + tExp, '总支出') + stat('¥' + tProfit, '总净利') + stat('¥' + (tOrd ? (tRev / tOrd).toFixed(1) : 0), '平均客单价');
  }
  function stat(v, l) { return '<div class="box"><div class="v">' + v + '</div><div class="l">' + l + '</div></div>'; }

  // ---------- 打印中心（出餐配送单） ----------
  function ticketHTML(o) {
    const c = custById(o.custId), d = dishById(o.dishId);
    if (!c || !d) return '';
    const taboo = o.taboo || c.taboo || '无';
    const ing = d.ing.map(i => '<li>· ' + i.n + ' —— <b>' + i.q + '</b></li>').join('');
    const sea = d.sea.map(s => '<li>· ' + s.n + ' —— <b>' + s.g + 'g</b></li>').join('');
    const steps = d.steps.map((s, i) =>
      '<div class="step"><div class="n">' + (i + 1) + '</div><div class="pic">' + s.icon + '</div>' +
      '<div class="desc"><b>' + s.t + '</b><span>' + s.d + '</span></div></div>').join('');
    return '<div class="ticket"><div class="hd"><b>撇脱私厨 · 出餐配送单</b><br><span>' + (o.ts || o.date) + ' · 杨存私厨</span></div>' +
      '<div class="row code-row"><div class="k">订单码</div><div class="code-val">' + (o.code || '-') + '</div></div>' +
      '<div class="row"><div class="k">顾客</div><div>' + c.name + '（' + (c.phone || '') + '）</div></div>' +
      '<div class="row"><div class="k">地址</div><div>' + c.address + '</div></div>' +
      '<div class="row"><div class="k">忌口</div><div>' + taboo + '</div></div>' +
      '<div class="row"><div class="k">预约送达</div><div>' + (withSeconds(o.deliverAt) || '未填') + '（顾客下班 ' + (c.offWork || '未知') + '）</div></div>' +
      '<div class="sec"><div class="sec-t">菜品：' + d.name + ' ×' + o.qty + '（' + catById(d.cat).name + '）</div>' +
      '<p style="font-size:13px;color:#555">' + d.intro + '</p></div>' +
      '<div class="sec"><div class="sec-t">材料清单</div><ul>' + ing + sea + '</ul></div>' +
      '<div class="sec"><div class="sec-t">制作步骤（图文）</div>' + steps + '</div>' +
      (o.note ? '<div class="row"><div class="k">备注</div><div>' + o.note + '</div></div>' : '') +
      '<div class="ft">撇脱私厨 · 杨存私厨 · 适配重庆口味 · 打印于 ' + nowStamp() + '</div></div>';
  }

  function renderPrint(preselectId) {
    // 只列出「待打印」订单所在的日期
    const dates = [...new Set(DB.orders.filter(o => !o.printed).map(o => o.date))].sort().reverse();
    if (!dates.length) { $('#print_area').innerHTML = '<div class="empty">还没有待打印的订单。' + (DB.orders.some(o => o.printed) ? '已打印的订单在「已处理订单」中查看。' : '先去「订单出餐」录单。') + '</div>'; $('#print_date').innerHTML = ''; updateSelCount(); return; }
    $('#print_date').innerHTML = dates.map(d => '<option value="' + d + '">' + d + '（' + DB.orders.filter(o => o.date === d && !o.printed).length + '单待打印）</option>').join('');
    if (!$('#print_date').value && $('#print_date').options.length) $('#print_date').value = $('#print_date').options[0].value;
    const buildList = () => {
      const dt = $('#print_date').value;
      const kw = ($('#print_code').value || '').trim().toLowerCase();
      const list = DB.orders.filter(o => {
        if (o.printed) return false;            // 已打印的不在待打印列表
        const okDate = o.date === dt;
        const okCode = kw ? (o.code || '').toLowerCase().indexOf(kw) > -1 : true;
        return kw ? okCode : okDate;
      });
      if (!list.length) {
        $('#print_area').innerHTML = '<div class="empty">没有待打印的订单' + (kw ? '（订单码含「' + kw + '」）' : '（' + dt + '）') + (DB.orders.some(o => o.printed) ? '。已打印的订单在「已处理订单」中查看。' : '') + '</div>';
        updateSelCount(); return;
      }
      $('#print_area').innerHTML = list.map(o =>
        '<div class="tk-wrap" data-oid="' + o.id + '">' +
          '<div class="tk-bar no-print">' +
            '<label class="tk-sel"><input type="checkbox" class="tk-chk" value="' + o.id + '"> 选</label>' +
            '<button class="btn sm" onclick="APP.printOne(\'' + o.id + '\')">🖨 打印此单</button>' +
          '</div>' + ticketHTML(o) +
        '</div>'
      ).join('<div style="height:18px"></div>');
      $('#print_area').querySelectorAll('.tk-chk').forEach(c => { c.onchange = updateSelCount; });
      updateSelCount();
    };
    $('#print_date').onchange = buildList;
    $('#print_code').oninput = buildList;
    $('#print_all').onchange = function () {
      $('#print_area').querySelectorAll('.tk-chk').forEach(c => { c.checked = this.checked; });
      updateSelCount();
    };
    if (preselectId) {
      const o = DB.orders.find(x => x.id === preselectId);
      if (o) { $('#print_date').value = o.date; }
    }
    buildList();
  }

  function updateSelCount() {
    const n = document.querySelectorAll('#print_area .tk-chk:checked').length;
    const el = document.getElementById('sel_count'); if (el) el.textContent = n;
  }
  // 把指定订单标记为已打印，并自动迁移到「已处理订单」
  function markPrinted(ids) {
    const set = new Set(ids);
    let changed = false;
    DB.orders.forEach(o => { if (set.has(o.id) && !o.printed) { o.printed = true; changed = true; } });
    if (changed) { save(); renderPrint(); renderDone(); renderOrders(); }
  }
  window.APP.printOne = function (id) {
    const o = DB.orders.find(x => x.id === id);
    if (!o) return;
    const d = dishById(o.dishId);
    openPrintWindow(ticketHTML(o), '出餐单-' + (d ? d.name : ''));
    markPrinted([id]);
  };
  window.APP.printSelected = function () {
    const ids = [...document.querySelectorAll('#print_area .tk-chk:checked')].map(c => c.value);
    const list = ids.length
      ? ids.map(id => DB.orders.find(o => o.id === id)).filter(Boolean)
      : DB.orders.filter(o => o.date === $('#print_date').value && !o.printed);
    if (!list.length) return alert('请先选择要打印的订单');
    const html = list.map(o => ticketHTML(o)).join('<div style="height:18px"></div>');
    openPrintWindow(html, '出餐单-' + ($('#print_date').value || '批量'));
    markPrinted(list.map(o => o.id));
  };

  // ---------- 已处理订单（打印后自动归入） ----------
  const orderKey = o => o.code || ('nc_' + o.custId);
  function renderDone() {
    const printed = DB.orders.filter(o => o.printed);
    const stat = document.getElementById('done_stat');
    const area = document.getElementById('done_area');
    if (!area) return;
    if (!printed.length) {
      area.innerHTML = '<div class="empty">还没有已打印的订单。在「打印中心」打印出餐单后，会自动归到这里。</div>';
      if (stat) stat.textContent = '';
      return;
    }
    const groups = {};
    printed.forEach(o => { (groups[orderKey(o)] = groups[orderKey(o)] || []).push(o); });
    const keys = Object.keys(groups).sort((a, b) =>
      (groups[b][0].date < groups[a][0].date ? -1 : groups[b][0].date > groups[a][0].date ? 1 : 0));
    const html = keys.map(code => {
      const lines = groups[code];
      const first = lines[0];
      const c = custById(first.custId);
      const taboo = first.taboo || (c ? c.taboo : '') || '无';
      const dishes = lines.map(o => { const d = dishById(o.dishId); return (d ? d.name : '?') + ' ×' + o.qty; }).join('，');
      return '<div class="tk-wrap done-card" data-code="' + code + '">' +
        '<div class="tk-bar no-print">' +
          '<span class="code-val">订单码 ' + (code || '-') + '</span>' +
          '<button class="btn sm" onclick="APP.reprintCode(\'' + code + '\')">🖨 重新打印</button> ' +
          '<button class="btn sm ghost" style="color:#c0392b;border-color:#c0392b" onclick="APP.unprintCode(\'' + code + '\')">↩ 退回待打印</button>' +
        '</div>' +
        '<div class="ticket">' +
          '<div class="row"><div class="k">顾客</div><div>' + (c ? c.name : '?') + '（' + (c ? (c.phone || '') : '') + '）</div></div>' +
          '<div class="row"><div class="k">地址</div><div>' + (c ? c.address : '-') + '</div></div>' +
          '<div class="row"><div class="k">预约送达</div><div>' + (withSeconds(first.deliverAt) || '未填') + '</div></div>' +
          '<div class="row"><div class="k">忌口</div><div>' + taboo + '</div></div>' +
          '<div class="sec"><div class="sec-t">菜品</div><div>' + dishes + '</div></div>' +
        '</div>' +
      '</div>';
    }).join('<div style="height:18px"></div>');
    area.innerHTML = html;
    if (stat) stat.textContent = '共 ' + keys.length + ' 笔已处理订单 · ' + printed.length + ' 条出餐记录';
  }
  window.APP.reprintCode = function (code) {
    const list = DB.orders.filter(o => orderKey(o) === code && o.printed);
    if (!list.length) return;
    const html = list.map(o => ticketHTML(o)).join('<div style="height:18px"></div>');
    openPrintWindow(html, '出餐单-' + code);
  };
  window.APP.unprintCode = function (code) {
    if (!confirm('确认将这笔订单退回「待打印」？（将重新出现在打印中心）')) return;
    DB.orders.forEach(o => { if (orderKey(o) === code) o.printed = false; });
    save(); renderDone(); renderPrint(); renderOrders();
  };

  // ---------- 弹窗 / 打印窗口 ----------
  function showModal() { $('#modal').classList.add('show'); }
  function hideModal() { $('#modal').classList.remove('show'); $('#m_print').style.display = 'none'; }
  function collectCSS() {
    let css = '';
    document.querySelectorAll('style').forEach(s => { css += s.textContent + '\n'; });
    try {
      for (const ss of document.styleSheets) {
        try { for (const r of ss.cssRules) css += r.cssText + '\n'; } catch (e) {}
      }
    } catch (e) {}
    return css;
  }
  function openPrintWindow(html, title) {
    const w = window.open('', '_blank');
    const inline = document.querySelector('style');
    const link = document.querySelector('link[rel="stylesheet"]');
    const headExtra = inline
      ? '<style>' + collectCSS() + '</style>'
      : (link ? '<link rel="stylesheet" href="' + link.href + '">' : '');
    w.document.write('<html><head><meta charset="utf-8"><title>' + title + '</title>' +
      headExtra + '</head><body><div class="main"><div style="padding:20px">' +
      html + '</div></div><script>window.onload=function(){setTimeout(function(){window.print();},300);}<\/script></body></html>');
    w.document.close();
  }

  function showPersistBanner() {
    if (document.getElementById('persist_banner')) return;
    const b = document.createElement('div');
    b.id = 'persist_banner';
    b.className = 'banner no-print';
    b.innerHTML = '⚠ 当前浏览器<b>无法保存数据</b>（通常是直接双击打开文件所致）。数据仅本次打开有效，刷新或关闭后会回到初始示例。' +
      '建议：① 到「打印中心」点「导出数据」每天备份成文件；② 或改用本地服务器打开以长期自动保存（可让我帮你启动）。';
    document.body.insertBefore(b, document.body.firstChild);
  }

  // ---------- 表单提交 ----------
  function bindForms() {
    // 顾客
    $('#cust_form').onsubmit = e => {
      e.preventDefault();
      const name = $('#f_cust_name').value.trim();
      if (!name) return alert('请填顾客姓名');
      const data = {
        name, phone: $('#f_cust_phone').value.trim(), address: $('#f_cust_addr').value.trim(),
        offWork: $('#f_cust_off').value.trim(), taboo: $('#f_cust_taboo').value.trim()
      };
      const eid = $('#cust_form').dataset.eid;
      if (eid) { Object.assign(custById(eid), data); } else { DB.customers.push(Object.assign({ id: uid() }, data)); }
      save(); renderCustomers();
      e.target.reset(); delete $('#cust_form').dataset.eid; $('#cust_submit').textContent = '添加顾客';
    };
    // 订单
    $('#order_form').onsubmit = e => {
      e.preventDefault();
      const custId = $('#o_cust').value, dishId = $('#o_dish').value;
      if (!custId || !dishId) return alert('请先添加顾客和选择菜品');
      DB.orders.push({
        id: uid(), date: $('#o_date').value || new Date().toISOString().slice(0, 10),
        ts: nowStamp(),
        custId, dishId, qty: Number($('#o_qty').value) || 1,
        price: Number($('#o_price').value) || 0,
        deliverAt: $('#o_deliver').value.trim(), taboo: $('#o_taboo').value.trim(), note: $('#o_note').value.trim(),
        code: genOrderCode()
      });
      save(); renderOrders(); e.target.reset();
      alert('已录入，可去「打印中心」生成出餐单');
    };
    // 支出
    $('#exp_form').onsubmit = e => {
      e.preventDefault();
      const amount = Number($('#e_amount').value);
      if (!amount) return alert('请填金额');
      DB.expenses.push({
        id: uid(), date: $('#e_date').value || new Date().toISOString().slice(0, 10),
        cat: $('#e_cat').value.trim() || '其他', amount, note: $('#e_note').value.trim()
      });
      save(); renderExpenses(); e.target.reset();
    };
    // 清空
    $('#btn_clear').onclick = () => {
      if (!confirm('确认清空所有顾客/订单/支出数据？（不可恢复）')) return;
      DB = { customers: [], orders: [], expenses: [], cleared: true }; save();
      renderCustomers(); renderOrders(); renderDaily(); renderExpenses(); renderPrint();
      alert('已清空');
    };
  }

  // ---------- 初始化 ----------
  document.addEventListener('DOMContentLoaded', () => {
    $$('.nav a').forEach(a => a.onclick = () => go(a.dataset.p));
    $('#modal').addEventListener('click', e => { if (e.target.id === 'modal') hideModal(); });
    $('#m_close').onclick = hideModal;
    bindForms();
    // 默认日期填充今天
    const today = new Date().toISOString().slice(0, 10);
    if ($('#o_date')) $('#o_date').value = today;
    if ($('#e_date')) $('#e_date').value = today;
    if (!storageAvailable()) showPersistBanner();
    setupMqtt();   // 启动跨设备实时接单（手机点单→电脑秒收）
    // 顾客点菜页链接（同源 order.html）
    const ol = document.getElementById('order_link');
    if (ol) { const b = location.href.split('/').slice(0, -1).join('/'); ol.href = b + '/order.html'; }
    go('recipe');
  });
})();
