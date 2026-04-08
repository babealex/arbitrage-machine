from __future__ import annotations

import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Two-Sleeve Control Panel</title>
  <style>
    :root { color-scheme: light; --bg:#f6f4ed; --panel:#fffdf7; --ink:#1e241d; --muted:#617064; --line:#d8d4c5; --accent:#0b6e4f; --warn:#9f3a20; font-family: Georgia, 'Times New Roman', serif; }
    body { margin:0; background:linear-gradient(180deg,#f6f4ed 0%,#ece6d8 100%); color:var(--ink); }
    header { padding:20px 24px; border-bottom:1px solid var(--line); background:rgba(255,253,247,0.92); position:sticky; top:0; backdrop-filter: blur(8px); }
    nav a { margin-right:16px; color:var(--accent); text-decoration:none; font-weight:600; }
    main { padding:24px; display:grid; gap:20px; }
    .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:16px; }
    .panel { background:var(--panel); border:1px solid var(--line); border-radius:14px; padding:16px; box-shadow:0 10px 30px rgba(46,52,36,0.06); }
    h1,h2,h3 { margin:0 0 10px; }
    p, td, th, li, code { font-size:14px; }
    .muted { color:var(--muted); }
    .status-running { color:var(--accent); font-weight:700; }
    .status-paused, .status-disabled { color:var(--warn); font-weight:700; }
    .status-error { color:#8d1f11; font-weight:700; }
    table { width:100%; border-collapse:collapse; }
    th, td { text-align:left; padding:8px 6px; border-bottom:1px solid var(--line); vertical-align:top; }
    .pill { display:inline-block; padding:3px 8px; border:1px solid var(--line); border-radius:999px; margin-right:6px; margin-bottom:6px; }
  </style>
</head>
<body>
  <header>
    <h1>Two-Sleeve Control Panel</h1>
    <nav>
      <a href="/">Overview</a>
      <a href="/comparison">Comparison</a>
      <a href="/sleeves/trend">Trend</a>
      <a href="/sleeves/kalshi_event">Kalshi/Event</a>
      <a href="/ledger">Trade Ledger</a>
      <a href="/errors">Errors</a>
    </nav>
  </header>
  <main id="app"></main>
  <script>
    async function loadState() {
      const response = await fetch('/api/state');
      return await response.json();
    }
    function statusClass(value) { return 'status-' + String(value || 'unknown').toLowerCase(); }
    function uniqueValues(rows, key) {
      return [...new Set(rows.map(row => row[key]).filter(value => value !== null && value !== undefined && value !== ''))].sort();
    }
    function renderOptions(values) {
      return ['<option value=\"\">All</option>'].concat(values.map(value => `<option value="${value}">${value}</option>`)).join('');
    }
    function filterLedgerRows(rows) {
      const sleeve = document.getElementById('ledger-sleeve')?.value || '';
      const date = document.getElementById('ledger-date')?.value || '';
      const signal = document.getElementById('ledger-signal')?.value || '';
      const fill = document.getElementById('ledger-fill')?.value || '';
      return rows.filter(row => {
        if (sleeve && row.sleeve_name !== sleeve) return false;
        if (date && !(row.timestamp || '').startsWith(date)) return false;
        if (signal && (row.signal_type || '') !== signal) return false;
        if (fill && (row.fill_status || '') !== fill) return false;
        return true;
      });
    }
    function tradeRows(rows) {
      return rows.map(row => `<tr><td>${row.timestamp}</td><td>${row.sleeve_name}</td><td>${row.instrument_or_market}</td><td>${row.signal_type || ''}</td><td>${row.fill_status || ''}</td><td>${row.expected_edge_after_costs_decimal || 'null'}</td><td>${row.realized_edge_after_costs_decimal || 'null'}</td><td>${row.slippage_estimate_bps_decimal || 'null'}</td><td>${row.realized_pnl_decimal || 'null'}</td></tr>`).join('');
    }
    function errorRows(rows) {
      return rows.map(row => `<tr><td>${row.created_at}</td><td>${row.sleeve_name}</td><td>${row.error_code}</td><td>${row.message}</td></tr>`).join('');
    }
    function overview(data) {
      return `<section class="panel"><h2>Overview</h2><p class="muted">Updated ${data.updated_at_utc} | Runtime ${data.runtime_name} | Run Mode ${data.run_mode}</p><p><strong>Master Thesis:</strong> ${data.master_thesis || 'n/a'}</p><p class="muted"><strong>Benchmark Question:</strong> ${data.benchmark_question || 'n/a'}</p><p class="muted"><strong>Operating Hierarchy:</strong> ${(data.operating_hierarchy || []).join(' -> ')}</p><div class="grid">${data.sleeves.map(sleeve => `<section class="panel"><h3>${sleeve.sleeve_name}</h3><p class="muted">Role ${sleeve.role || 'unknown'} | Maturity ${sleeve.maturity} | Run Mode ${sleeve.run_mode}</p><p class="${statusClass(sleeve.status)}">${sleeve.status}</p><p class="muted">${sleeve.mandate || ''}</p>${(sleeve.blocker_notes || []).map(note => `<span class="pill">${note}</span>`).join('')}<table><tr><th>Capital</th><td>${sleeve.capital_assigned}</td></tr><tr><th>Daily PnL</th><td>${sleeve.daily_pnl}</td></tr><tr><th>Cumulative PnL</th><td>${sleeve.cumulative_pnl}</td></tr><tr><th>Opportunities</th><td>${sleeve.opportunities_seen}</td></tr><tr><th>Attempted</th><td>${sleeve.trades_attempted}</td></tr><tr><th>Filled</th><td>${sleeve.trades_filled}</td></tr><tr><th>Best Edge Seen</th><td>${sleeve.edge_summary?.best_edge_seen || 'null'}</td></tr><tr><th>Open Positions</th><td>${sleeve.open_positions}</td></tr><tr><th>Trades</th><td>${sleeve.trade_count}</td></tr><tr><th>Errors</th><td>${sleeve.error_count}</td></tr></table></section>`).join('')}</div><section class="panel"><h3>Disabled Modules</h3>${data.disabled_modules.map(item => `<span class="pill">${item}</span>`).join('')}</section></section>`;
    }
    function comparison(data) {
      return `<section class="panel"><h2>Comparison</h2><p class="muted"><strong>Benchmark Question:</strong> ${data.benchmark_question || 'n/a'}</p><table><tr><th>Sleeve</th><th>Role</th><th>Run Mode</th><th>Capital</th><th>PnL</th><th>Opportunities</th><th>Attempted</th><th>Filled</th><th>Avg Expected Edge</th><th>Median Edge</th><th>Best Edge</th><th>Avg Realized Edge</th><th>Edge Gap</th><th>Fees</th><th>Slippage</th><th>Fill Rate</th><th>Drawdown</th><th>Capital Efficiency</th><th>Errors</th><th>Top No-Trade Reasons</th></tr>${data.comparison.map(row => `<tr><td>${row.sleeve_name}</td><td>${row.role || 'unknown'}</td><td>${row.run_mode}</td><td>${row.capital_assigned}</td><td>${row.cumulative_pnl}</td><td>${row.opportunities_seen}</td><td>${row.trades_attempted}</td><td>${row.trades_filled}</td><td>${row.avg_expected_edge}</td><td>${row.edge_summary?.median_expected_edge || 'null'}</td><td>${row.edge_summary?.best_edge_seen || 'null'}</td><td>${row.avg_realized_edge}</td><td>${row.expected_realized_gap}</td><td>${row.fees}</td><td>${row.avg_slippage_bps}</td><td>${row.fill_rate}</td><td>${row.drawdown_proxy}</td><td>${row.capital_efficiency_proxy}</td><td>${row.error_count}</td><td>${(row.top_no_trade_reasons || []).map(item => `${item.reason}:${item.count}`).join(', ')}</td></tr>`).join('')}</table></section>`;
    }
    function detail(sleeve) {
      return `<section class="panel"><h2>${sleeve.sleeve_name}</h2><p class="${statusClass(sleeve.status)}">${sleeve.status}</p><p class="muted">Role ${sleeve.role || 'unknown'} | Run Mode ${sleeve.run_mode}</p><p>${sleeve.mandate || ''}</p><p class="muted"><strong>Scale Profile:</strong> ${sleeve.scale_profile || 'n/a'}</p><p class="muted"><strong>Promotion Gate:</strong> ${sleeve.promotion_gate || 'n/a'}</p>${(sleeve.blocker_notes || []).map(note => `<span class="pill">${note}</span>`).join('')}<div class="grid"><section class="panel"><h3>Stats</h3><table><tr><th>Capital</th><td>${sleeve.capital_assigned}</td></tr><tr><th>Opportunities</th><td>${sleeve.opportunities_seen}</td></tr><tr><th>Attempted</th><td>${sleeve.trades_attempted}</td></tr><tr><th>Filled</th><td>${sleeve.trades_filled}</td></tr><tr><th>Realized PnL</th><td>${sleeve.realized_pnl}</td></tr><tr><th>Unrealized PnL</th><td>${sleeve.unrealized_pnl}</td></tr><tr><th>Fill Rate</th><td>${sleeve.fill_rate}</td></tr><tr><th>Avg Expected Edge</th><td>${sleeve.avg_expected_edge}</td></tr><tr><th>Median Edge</th><td>${sleeve.edge_summary?.median_expected_edge || 'null'}</td></tr><tr><th>Best Edge</th><td>${sleeve.edge_summary?.best_edge_seen || 'null'}</td></tr><tr><th>Avg Realized Edge</th><td>${sleeve.avg_realized_edge}</td></tr><tr><th>Edge Gap</th><td>${sleeve.expected_realized_gap}</td></tr><tr><th>Fees</th><td>${sleeve.fees}</td></tr><tr><th>Avg Slippage Bps</th><td>${sleeve.avg_slippage_bps}</td></tr></table></section><section class="panel"><h3>No-Trade Reasons</h3><table>${(sleeve.top_no_trade_reasons || []).map(item => `<tr><td>${item.reason}</td><td>${item.count}</td></tr>`).join('')}</table></section><section class="panel"><h3>Top Missed Candidates</h3>${(sleeve.top_candidates_nearest_to_trade || []).length ? `<table><tr><th>Ticker</th><th>Strategy</th><th>Reason</th><th>Edge After Fees</th></tr>${(sleeve.top_candidates_nearest_to_trade || []).map(item => `<tr><td>${item.ticker}</td><td>${item.strategy}</td><td>${item.reason}</td><td>${item.edge_after_fees}</td></tr>`).join('')}</table>` : '<p class="muted">No near-trade candidates recorded for this sleeve.</p>'}</section><section class="panel"><h3>PnL Curve</h3><table>${sleeve.pnl_curve.map(row => `<tr><td>${row.created_at}</td><td>${row.realized_pnl_decimal}</td><td>${row.unrealized_pnl_decimal}</td></tr>`).join('')}</table></section></div><section class="panel"><h3>Recent Trades</h3>${sleeve.recent_trades.length ? '' : '<p class="muted">No trades yet for this sleeve.</p>'}<table><tr><th>Time</th><th>Sleeve</th><th>Instrument</th><th>Signal</th><th>Status</th><th>Expected</th><th>Realized</th><th>Slippage</th><th>PnL</th></tr>${tradeRows(sleeve.recent_trades)}</table></section><section class="panel"><h3>Recent Errors</h3><table><tr><th>Time</th><th>Sleeve</th><th>Code</th><th>Message</th></tr>${errorRows(sleeve.recent_errors)}</table></section></section>`;
    }
    function ledger(data) {
      return `<section class="panel"><h2>Trade Ledger</h2>${data.ledger_explanation ? `<p class="muted">${data.ledger_explanation}</p>` : ''}<div class="grid"><section class="panel"><h3>Filters</h3><table><tr><th>Sleeve</th><td><select id="ledger-sleeve">${renderOptions(uniqueValues(data.ledger, 'sleeve_name'))}</select></td></tr><tr><th>Date</th><td><input id="ledger-date" type="date"></td></tr><tr><th>Signal Type</th><td><select id="ledger-signal">${renderOptions(uniqueValues(data.ledger, 'signal_type'))}</select></td></tr><tr><th>Fill Status</th><td><select id="ledger-fill">${renderOptions(uniqueValues(data.ledger, 'fill_status'))}</select></td></tr></table></section></div><table><thead><tr><th>Time</th><th>Sleeve</th><th>Instrument</th><th>Signal</th><th>Status</th><th>Expected</th><th>Realized</th><th>Slippage</th><th>PnL</th></tr></thead><tbody id="ledger-body">${tradeRows(data.ledger)}</tbody></table></section>`;
    }
    function errors(data) {
      return `<section class="panel"><h2>Error / Debug</h2><div class="grid">${data.sleeves.map(sleeve => `<section class="panel"><h3>${sleeve.sleeve_name}</h3><p class="${statusClass(sleeve.status)}">${sleeve.status}</p><p class="muted">Errors ${sleeve.error_count}</p></section>`).join('')}</div><table><tr><th>Time</th><th>Sleeve</th><th>Code</th><th>Message</th></tr>${errorRows(data.errors)}</table></section>`;
    }
    function attachLedgerFilters(data) {
      const redraw = () => {
        const body = document.getElementById('ledger-body');
        if (body) body.innerHTML = tradeRows(filterLedgerRows(data.ledger));
      };
      ['ledger-sleeve', 'ledger-date', 'ledger-signal', 'ledger-fill'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('input', redraw);
      });
    }
    loadState().then(data => {
      const path = window.location.pathname;
      const app = document.getElementById('app');
      if (path === '/comparison') { app.innerHTML = comparison(data); return; }
      if (path === '/ledger') { app.innerHTML = ledger(data); attachLedgerFilters(data); return; }
      if (path === '/errors') { app.innerHTML = errors(data); return; }
      if (path.startsWith('/sleeves/')) {
        const sleeve = data.sleeves.find(item => item.sleeve_name === path.split('/').pop());
        app.innerHTML = sleeve ? detail(sleeve) : '<section class="panel"><h2>Unknown sleeve</h2></section>';
        return;
      }
      app.innerHTML = overview(data);
    });
  </script>
</body>
</html>
"""


def start_control_panel(*, state_path: Path, host: str, port: int, logger) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path == "/api/state":
                body = (state_path.read_text(encoding="utf-8") if state_path.exists() else "{}").encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path in {"/", "/comparison", "/ledger", "/errors", "/sleeves/trend", "/sleeves/kalshi_event"}:
                body = HTML_TEMPLATE.encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(HTTPStatus.NOT_FOUND)
            self.end_headers()

        def log_message(self, format, *args):  # noqa: A003
            logger.info("control_panel_http", extra={"event": {"message": format % args}})

    server = ThreadingHTTPServer((host, port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("control_panel_started", extra={"event": {"host": host, "port": port, "state_path": str(state_path)}})
    return server
