#!/usr/bin/env python3
"""Progress dashboard for the mining daemon. Reads results.db. Stdlib only."""
import html
import json
import sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

BASE = Path(__file__).resolve().parent
DB = BASE / 'results.db'
STATUS = BASE / 'daemon_status.json'
LOG = BASE / 'daemon.log'
PORT = 8899

KEYS = ('expr', 'decay', 'neut', 'region', 'universe', 'status', 'alpha',
        'sharpe', 'fitness', 'returns', 'turnover', 'message', 'submit')


def q(sql, args=()):
    con = sqlite3.connect(DB)
    try:
        return con.execute(sql, args).fetchall()
    finally:
        con.close()


def rows_to_dicts(rows):
    return [dict(zip(KEYS, r)) for r in rows]


def best_line(hist):
    pts, best = [], -9.0
    for i, h in enumerate(hist):
        s = h.get('sharpe')
        if s is not None:
            best = max(best, s)
        if best > -9:
            pts.append((i, best))
    if not pts:
        return ''
    w, hgt, pad = 700, 160, 8
    n = max(len(pts), 1)
    xs = [pad + i / max(n - 1, 1) * (w - 2 * pad) for i, _ in pts]
    lo, hi = min(v for _, v in pts), max(v for _, v in pts)
    span = max(hi - lo, 0.01)
    ys = [hgt - pad - (v - lo) / span * (hgt - 2 * pad) for _, v in pts]
    path = 'M' + ' L'.join(f'{x:.0f},{y:.0f}' for x, y in zip(xs, ys))
    y125 = hgt - pad - (1.25 - lo) / span * (hgt - 2 * pad)
    return (f'<svg width="{w}" height="{hgt}" style="background:#0d1117;border-radius:8px">'
            f'<line x1="{pad}" x2="{w-pad}" y1="{y125:.0f}" y2="{y125:.0f}" '
            f'stroke="#f85149" stroke-dasharray="4" />'
            f'<path d="{path}" fill="none" stroke="#58a6ff" stroke-width="2"/>'
            f'<text x="{w-60}" y="{y125-4:.0f}" fill="#f85149" font-size="10">1.25</text>'
            f'<text x="{pad}" y="{14}" fill="#8b949e" font-size="10">best {hi:.2f}</text></svg>')


def page():
    try:
        total = q('SELECT COUNT(*) FROM sims')[0][0]
    except Exception:
        total = 0
    try:
        top = rows_to_dicts(q(
            'SELECT ' + ','.join(KEYS) + ' FROM sims '
            'WHERE fitness>=1.0 AND sharpe>=1.25 '
            'ORDER BY (sharpe+fitness) DESC LIMIT 15'))
        recent = rows_to_dicts(q(
            'SELECT ' + ','.join(KEYS) + ' FROM sims ORDER BY id DESC LIMIT 15'))
        hist = rows_to_dicts(q('SELECT ' + ','.join(KEYS) + ' FROM sims ORDER BY id'))
        nfail = q("SELECT COUNT(*) FROM sims WHERE status NOT IN "
                  "('COMPLETE')")[0][0]
    except Exception:
        top, recent, hist, nfail = [], [], [], 0
    try:
        status = json.load(open(STATUS))
    except Exception:
        status = {}
    try:
        log = open(LOG, errors='replace').read().splitlines()[-25:]
    except Exception:
        log = []
    best = status.get('best') or {}
    submitted = status.get('submitted', [])

    def row(h):
        sub = h.get('submit') or (h.get('message', '') or '')[-40:]
        return ('<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>'
                '<td style="font-family:monospace">{}</td><td>{}</td></tr>').format(
            html.escape(str(h.get('alpha') or '-')), h.get('sharpe', '-'),
            h.get('fitness', '-'), html.escape(str(h.get('status') or '-')),
            'd' + str(h.get('decay', '?')) + '/' + html.escape(str(h.get('neut') or '?')),
            html.escape((h.get('expr') or '')[:90]), html.escape(str(sub)[:40]))

    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="60">
<title>WQ Miner Dashboard</title>
<style>body{{background:#010409;color:#e6edf3;font-family:sans-serif;margin:24px}}
.cards{{display:flex;gap:12px;flex-wrap:wrap}} .card{{background:#0d1117;border:1px solid #30363d;
border-radius:8px;padding:12px 18px;min-width:140px}} .card b{{font-size:24px;color:#58a6ff}}
table{{border-collapse:collapse;width:100%;font-size:13px}} td,th{{border:1px solid #30363d;
padding:4px 8px;text-align:left}} th{{background:#161b22}} pre{{background:#0d1117;padding:12px;
border-radius:8px;overflow:auto;max-height:300px;font-size:12px}}</style></head><body>
<h2>⛏️ WorldQuant Mining Dashboard <small style="color:#8b949e">auto-refresh 60s · {status.get('ts', '-')}</small></h2>
<div class="cards">
<div class="card">总仿真<br><b>{total}</b></div>
<div class="card">双过线<br><b>{len(top) if total else 0}</b> (top15 shown)</div>
<div class="card">最佳Sharpe<br><b>{best.get('sharpe', '-')}</b></div>
<div class="card">已提交<br><b>{len(submitted)}</b></div>
<div class="card">非COMPLETE<br><b>{nfail}</b></div>
<div class="card">截止<br><b style="font-size:14px">{html.escape(str(status.get('deadline', '-'))[:16])}</b></div>
</div>
<p style="color:#8b949e">最佳: <span style="font-family:monospace">{html.escape(str(best.get('expr', '-'))[:120])}</span> · {html.escape(str(status.get('msg', '')))}</p>
<h3>Best-Sharpe 曲线</h3>{best_line(hist)}
<h3>Top 15（sharpe+fitness）</h3>
<table><tr><th>alpha</th><th>sharpe</th><th>fitness</th><th>status</th><th>decay/neut</th><th>expr</th><th>submit</th></tr>
{''.join(row(h) for h in top)}</table>
<h3>最近 15 条</h3>
<table><tr><th>alpha</th><th>sharpe</th><th>fitness</th><th>status</th><th>decay/neut</th><th>expr</th><th>submit</th></tr>
{''.join(row(h) for h in recent)}</table>
<h3>已提交</h3><pre>{html.escape(json.dumps(submitted, indent=1, ensure_ascii=False))}</pre>
<h3>日志尾</h3><pre>{html.escape(chr(10).join(log))}</pre>
</body></html>"""


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        body = page().encode()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


if __name__ == '__main__':
    HTTPServer(('127.0.0.1', PORT), H).serve_forever()
