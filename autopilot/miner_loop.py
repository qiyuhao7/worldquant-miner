#!/usr/bin/env python3
"""24/7 unattended WorldQuant mining: muse-spark generates, WQ simulates.

Results live in SQLite (results.db). Stdlib + requests only.
Usage: python3 miner_loop.py [--hours 0] [--auto-submit] [--max-submits 3]
  --hours 0 = run forever (24/7). Exit code 10 = time budget exhausted.
"""
import json
import re
import sqlite3
import sys
import time
import argparse
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

BASE = Path(__file__).resolve().parent
DB = BASE / 'results.db'
STATUS = BASE / 'daemon_status.json'
OPS = json.load(open(BASE / 'operators.json'))
FIELDS = json.load(open(BASE / 'fields.json'))
OP_NAMES = [o['name'] for o in OPS[:60]]
_CORE = ['cashflow_op', 'cashflow', 'ebit', 'ebitda', 'enterprise_value', 'assets',
         'capex', 'debt', 'debt_lt', 'cash', 'eps', 'bookvalue_ps', 'employee']
_PV = [k for k, v in FIELDS.items() if v.get('dataset') == 'pv1'
       and v.get('type') == 'MATRIX'][:10]
_M16 = [k for k, v in FIELDS.items() if v.get('dataset') == 'model16'][:16]
_M51 = [k for k, v in FIELDS.items() if v.get('dataset') == 'model51'][:8]
FIELD_IDS = []
for _id in _CORE + _PV + _M16 + _M51:
    if _id in FIELDS and _id not in FIELD_IDS:
        FIELD_IDS.append(_id)
FIELD_IDS = FIELD_IDS[:55]

REPO_ROOT = BASE.parent
SEEDS = BASE / 'seeds_forum.md'


def load_seeds():
    try:
        return open(SEEDS).read()[:3000]
    except Exception:
        return ''
CRED = json.load(open(REPO_ROOT / 'credential.txt'))
OCFG = json.load(open(Path.home() / '.config/opencode/opencode.json'))
OCG = OCFG['provider']['opencode-go-mgr']
OCG_URL = OCG['options']['baseURL'] + '/chat/completions'
OCG_KEY = OCG['options']['apiKey']
MODEL = 'muse-spark-1.3-contributor'

PAYLOAD = {'type': 'REGULAR', 'settings': {
    'instrumentType': 'EQUITY', 'region': 'USA', 'universe': 'TOP3000',
    'delay': 1, 'decay': 0, 'neutralization': 'INDUSTRY', 'truncation': 0.08,
    'pasteurization': 'ON', 'unitHandling': 'VERIFY', 'nanHandling': 'OFF',
    'language': 'FASTEXPR', 'visualization': False}}


# ---------------- storage ----------------

def db():
    con = sqlite3.connect(DB)
    con.execute("""CREATE TABLE IF NOT EXISTS sims(
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, expr TEXT,
        decay INT, neut TEXT, region TEXT, universe TEXT,
        status TEXT, alpha TEXT, sharpe REAL, fitness REAL,
        returns REAL, turnover REAL, message TEXT,
        submit TEXT, self_corr REAL)""")
    con.execute("CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_sims_sharpe ON sims(sharpe)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_sims_alpha ON sims(alpha)")
    return con


def kv_get(con, k, default=None):
    r = con.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
    if not r:
        return default
    try:
        return json.loads(r[0])
    except Exception:
        return default


def kv_set(con, k, v):
    con.execute("INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)", (k, json.dumps(v)))
    con.commit()


def log_sim(rec):
    con = db()
    con.execute("""INSERT INTO sims(ts,expr,decay,neut,region,universe,status,alpha,
        sharpe,fitness,returns,turnover,message,submit,self_corr)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), rec.get('expr'),
        rec.get('decay', 0), rec.get('neut', 'INDUSTRY'),
        rec.get('region', 'USA'), rec.get('universe', 'TOP3000'),
        rec.get('status'), rec.get('alpha'), rec.get('sharpe'),
        rec.get('fitness'), rec.get('returns'), rec.get('turnover'),
        rec.get('message'), rec.get('submit'), rec.get('self_corr')))
    con.commit()
    rowid = con.execute('SELECT last_insert_rowid()').fetchone()[0]
    con.close()
    return rowid


def update_sim(rowid, rec):
    con = db()
    con.execute("""UPDATE sims SET ts=?,status=?,alpha=?,sharpe=?,fitness=?,
        returns=?,turnover=?,message=?,submit=?,self_corr=? WHERE id=?""", (
        time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), rec.get('status'),
        rec.get('alpha'), rec.get('sharpe'), rec.get('fitness'),
        rec.get('returns'), rec.get('turnover'), rec.get('message'),
        rec.get('submit'), rec.get('self_corr'), rowid))
    con.commit()
    con.close()


def get_history(limit=400):
    con = db()
    rows = con.execute(
        "SELECT expr,decay,neut,region,universe,status,alpha,sharpe,fitness,"
        "returns,turnover,message,submit FROM sims ORDER BY id").fetchall()
    con.close()
    keys = ('expr', 'decay', 'neut', 'region', 'universe', 'status', 'alpha',
            'sharpe', 'fitness', 'returns', 'turnover', 'message', 'submit')
    hist = [dict(zip(keys, r)) for r in rows]
    return hist[-limit:]


def heartbeat(history, submitted, deadline, msg=''):
    try:
        passed = [h for h in history if (h.get('fitness') or 0) >= 1.0
                  and (h.get('sharpe') or 0) >= 1.25]
        best = max((h for h in history if h.get('sharpe') is not None),
                   key=lambda x: x['sharpe'], default=None)
        json.dump({
            'ts': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'total': len(history)
            + (db().execute("SELECT COUNT(*) FROM sims").fetchone()[0]
               - len(history) if len(history) < 400 else len(history)),
            'passed': len(passed),
            'best': ({'expr': best['expr'], 'sharpe': best['sharpe'],
                      'alpha': best.get('alpha')} if best else None),
            'submitted': submitted,
            'deadline': (time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(deadline))
                         if deadline else 'NEVER (24/7)'),
            'msg': msg}, open(STATUS, 'w'), indent=1)
    except Exception as e:
        print('heartbeat fail:', e, flush=True)


# ---------------- worldquant ----------------

def wq_session():
    s = requests.Session()
    s.auth = HTTPBasicAuth(CRED[0], CRED[1])
    r = s.post('https://api.worldquantbrain.com/authentication')
    assert r.status_code == 201, f'auth failed: {r.text[:200]}'
    return s


def fetch_checks(s, alpha_id):
    try:
        return s.get(f'https://api.worldquantbrain.com/alphas/{alpha_id}',
                     timeout=30).json().get('is', {}).get('checks', [])
    except Exception:
        return []


def try_submit(s, con, rec, max_submits):
    aid = rec.get('alpha')
    if not aid or rec.get('status') != 'COMPLETE':
        return rec
    checks = fetch_checks(s, aid)
    if not checks:
        return rec
    bad = [c['name'] for c in checks
           if c.get('result') == 'FAIL' and c.get('name') != 'SELF_CORRELATION']
    if bad:
        rec['submit'] = None
        rec['message'] = (rec.get('message') or '') + f' | submit_skip: {bad}'
        return rec
    day = time.strftime('%Y-%m-%d', time.gmtime())
    submits = kv_get(con, 'submits_by_day', {})
    if submits.get(day, 0) >= max_submits:
        rec['message'] = (rec.get('message') or '') + ' | submit_skip: daily cap'
        return rec
    try:
        r = s.post(f'https://api.worldquantbrain.com/alphas/{aid}/submit', timeout=30)
    except Exception as e:
        rec['message'] = (rec.get('message') or '') + f' | submit_exc: {e}'[:150]
        return rec
    if r.status_code == 404:
        rec['submit'] = 'already-submitted'
        return rec
    if r.status_code not in (200, 201, 202, 204):
        rec['message'] = (rec.get('message') or '') + f' | submit {r.status_code}'[:150]
        return rec
    submits[day] = submits.get(day, 0) + 1
    kv_set(con, 'submits_by_day', submits)
    for _ in range(30):
        time.sleep(10)
        try:
            g = s.get(f'https://api.worldquantbrain.com/alphas/{aid}/submit', timeout=30)
        except Exception:
            continue
        if g.status_code == 404:
            rec['submit'] = 'already-submitted'
            return rec
        try:
            j = g.json()
        except Exception:
            continue
        sc = [c for c in j.get('is', {}).get('checks', [])
              if c.get('name') == 'SELF_CORRELATION']
        if sc and sc[0].get('result') in ('PASS', 'FAIL'):
            rec['submit'] = 'SUBMITTED' if sc[0]['result'] == 'PASS' else 'SELF_CORR_FAIL'
            rec['self_corr'] = sc[0].get('value')
            if sc[0]['result'] == 'PASS':
                submitted = kv_get(con, 'submitted', [])
                submitted.append({'alpha': aid, 'expr': rec['expr'],
                                  'sharpe': rec.get('sharpe')})
                kv_set(con, 'submitted', submitted)
            return rec
    rec['submit'] = 'POLL_TIMEOUT'
    return rec


# ---------------- llm ----------------

def gen_batch(history, submitted, temperature=0.85):
    tried = {h['expr'] for h in history}
    tried_short = [h['expr'] for h in history[-35:]]
    scored = sorted([h for h in history if h.get('sharpe') is not None],
                    key=lambda x: x['sharpe'], reverse=True)
    negs = sorted([h for h in history if (h.get('sharpe') or 0) < -0.9],
                  key=lambda x: x['sharpe'])
    top = '\n'.join(f"- {h['expr']} -> sharpe {h['sharpe']}, fitness {h.get('fitness')}"
                    for h in scored[:8])
    neg_txt = '\n'.join(
        f"- {h['expr']} -> sharpe {h['sharpe']} (NEGATE: reverse()/minus may flip)"
        for h in negs[:4])
    errs = '\n'.join(f"- {h['expr']} -> {h.get('status')}: {h.get('message','')[:120]}"
                     for h in history if h.get('status') in ('ERROR', 'FAILED'))
    best = scored[0] if scored else None
    sub_txt = '\n'.join(f"- {x['expr']} (sharpe {x.get('sharpe')})"
                        for x in submitted[-6:])
    prompt = f"""WorldQuant FASTEXPR evolution, USA/TOP3000/delay1. Pass line: sharpe>1.25, fitness>1.0.

BEST SO FAR: {best['expr'] + ' sharpe ' + str(best['sharpe']) if best else 'none'}
ALREADY SUBMITTED (must be LOW-correlation vs these, self-corr limit 0.7):
{sub_txt if submitted else 'none yet'}
SELF-CORR LESSON: rank-preserving transforms (quantile/group_rank/SUBINDUSTRY-neut of the SAME signal) keep self-corr ~0.94-0.99 and FAIL. Combos sharing the value leg usually fail (~0.73+) UNLESS the other leg is strong and independent (overnight-gap+value PASSED at 2.33). Prefer pairing value leg with strong independent legs.
STRONG NEGATIVES TO FLIP:
{neg_txt if negs else 'none yet'}
FITNESS: needs >1.0, rewards lower turnover/higher margin. Prefer decay 1-10, smoother windows.
COMBO STRATEGY (highest ROI): add()/subtract() pairing a value winner (cashflow_op/EV) with a reversal winner (volume/returns/correlation flips). Also try SECTOR/MARKET neut on proven exprs.
FORUM SEEDS (verified Alpha101/playbook patterns, prioritize variants of these):
{load_seeds()}
TOP RESULTS:
{top}
FAILURES TO AVOID:
{errs if errs else 'none yet'}
- arithmetic ops AND densify() on analyst event fields (anl4_*/actual_*) cause ERROR.
- account is USA/TOP3000 ONLY. Always USA.

ALREADY TRIED (recent 35 shown; never repeat these exact expr strings):
{chr(10).join(tried_short)}

OPS: {', '.join(OP_NAMES)}
FIELDS: {', '.join(FIELD_IDS)}
Rules: commas for params, no region prefix, max 5 ops, balanced parens.
Return ONLY a JSON array of 5 objects: {{"expr": "...", "decay": <0-10 int>, "neut": "INDUSTRY"}}. JSON array only."""
    for attempt in range(6):
        try:
            r = requests.post(OCG_URL, headers={'Authorization': f'Bearer {OCG_KEY}'},
                json={'model': MODEL, 'messages': [{'role': 'user', 'content': prompt}],
                      'temperature': temperature, 'max_completion_tokens': 8000},
                timeout=240)
            d = r.json()
            if 'choices' in d and d['choices'][0]['message']['content'].strip():
                c = d['choices'][0]['message']['content']
                m = re.search(r'\[.*\]', c, re.DOTALL)
                raw = json.loads(m.group(0) if m else c)
                batch = []
                for b in raw:
                    if isinstance(b, str):
                        if b not in tried:
                            batch.append({'expr': b, 'decay': 0, 'neut': 'INDUSTRY'})
                    elif isinstance(b, dict) and isinstance(b.get('expr'), str):
                        if b['expr'] not in tried:
                            try:
                                dec = max(0, min(20, int(b.get('decay', 0))))
                            except Exception:
                                dec = 0
                            neut = b.get('neut', 'INDUSTRY')
                            if neut not in ('INDUSTRY', 'MARKET', 'SECTOR', 'COUNTRY'):
                                neut = 'INDUSTRY'
                            batch.append({'expr': b['expr'], 'decay': dec, 'neut': neut})
                if batch:
                    return batch
                print('all duplicates, retry', flush=True)
            else:
                print(f"ocg attempt {attempt}: {str(d)[:200]}", flush=True)
        except Exception as e:
            print(f'ocg attempt {attempt} exc: {e}', flush=True)
        time.sleep(30)
    return []


def sim_one(s, item):
    expr = item['expr'] if isinstance(item, dict) else item
    decay = item.get('decay', 0) if isinstance(item, dict) else 0
    neut = item.get('neut', 'INDUSTRY') if isinstance(item, dict) else 'INDUSTRY'
    p = json.loads(json.dumps(PAYLOAD))
    p['regular'] = expr
    p['settings']['decay'] = decay
    p['settings']['neutralization'] = neut
    rec0 = {'expr': expr, 'decay': decay, 'neut': neut,
            'region': 'USA', 'universe': 'TOP3000'}
    try:
        r = s.post('https://api.worldquantbrain.com/simulations', json=p)
    except Exception as e:
        return {**rec0, 'status': 'SUBMIT_EXC', 'message': str(e)[:200]}
    if r.status_code == 401:
        try:
            s = wq_session()
        except Exception as e:
            return {**rec0, 'status': 'AUTH_FAIL', 'message': str(e)[:200]}
        r = s.post('https://api.worldquantbrain.com/simulations', json=p)
    if r.status_code == 429:
        return {**rec0, 'status': 'RATE_LIMITED', 'message': r.text[:200]}
    if r.status_code != 201:
        return {**rec0, 'status': 'SUBMIT_FAIL', 'message': r.text[:300]}
    loc = r.headers.get('Location')
    if not loc:
        return {**rec0, 'status': 'NO_LOCATION', 'message': r.text[:200]}
    for _ in range(100):
        time.sleep(8)
        try:
            pr = s.get(loc)
        except Exception:
            continue
        try:
            ra = float(pr.headers.get('Retry-After', 0) or 0)
        except Exception:
            ra = 0
        if ra > 0:
            time.sleep(min(ra, 20))
            continue
        try:
            j = pr.json()
        except Exception:
            return {**rec0, 'status': 'POLL_FAIL', 'message': pr.text[:200]}
        if j.get('status') in ('COMPLETE', 'FAILED', 'ERROR'):
            rec = {**rec0, 'status': j.get('status'),
                   'alpha': j.get('alpha'), 'message': str(j.get('message', ''))[:300]}
            if j.get('status') == 'COMPLETE' and j.get('alpha'):
                try:
                    a = s.get(
                        f"https://api.worldquantbrain.com/alphas/{j['alpha']}",
                        timeout=30).json().get('is', {})
                    rec['sharpe'] = round(a.get('sharpe', 0), 3)
                    rec['fitness'] = round(a.get('fitness', 0) or 0, 3)
                    rec['returns'] = round(a.get('returns', 0), 4)
                    rec['turnover'] = round(a.get('turnover', 0), 3)
                except Exception as e:
                    rec['message'] = (rec.get('message') or '') + f' | metrics: {e}'[:150]
            return rec
    return {**rec0, 'status': 'TIMEOUT'}


# ---------------- main ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--rounds', type=int, default=0)
    ap.add_argument('--hours', type=float, default=0,
                    help='time budget; 0 = run forever (24/7)')
    ap.add_argument('--auto-submit', action='store_true')
    ap.add_argument('--max-submits', type=int, default=3)
    args = ap.parse_args()

    con = db()
    deadline = kv_get(con, 'deadline', None)
    if args.hours > 0 and (not deadline or deadline < time.time()):
        deadline = time.time() + args.hours * 3600
        kv_set(con, 'deadline', deadline)
    if deadline:
        dl_txt = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(deadline))
    else:
        dl_txt = 'NEVER (24/7)'
    print(f"FIELD SLATE: {len(FIELD_IDS)} fields", flush=True)
    print(f"deadline: {dl_txt} auto_submit={args.auto_submit}", flush=True)

    def expired():
        return deadline is not None and time.time() >= deadline

    try:
        s = wq_session()
    except Exception as e:
        print('FATAL auth:', e, flush=True)
        return 1

    history = get_history()
    heartbeat(history, kv_get(con, 'submitted', []), deadline, 'started')
    rnd = 0
    while True:
        if expired():
            print('TIME BUDGET EXHAUSTED, clean exit', flush=True)
            heartbeat(history, kv_get(con, 'submitted', []), deadline, 'deadline')
            return 10
        if args.rounds and rnd >= args.rounds:
            break
        rnd += 1
        history = get_history()
        print(f'===== ROUND {rnd} (db sims {len(history)}) =====', flush=True)
        batch = gen_batch(history, kv_get(con, 'submitted', []))
        if not batch:
            print('generation failed, backing off 10 min', flush=True)
            heartbeat(history, kv_get(con, 'submitted', []), deadline, 'gen backoff')
            time.sleep(600)
            continue
        print('BATCH:', batch, flush=True)
        for item in batch:
            if expired():
                print('TIME BUDGET EXHAUSTED mid-batch, clean exit', flush=True)
                return 10
            print('SUBMIT:', item, flush=True)
            # crash-safe: pending row first, updated on completion
            pending = {'expr': item['expr'] if isinstance(item, dict) else item,
                       'decay': item.get('decay', 0) if isinstance(item, dict) else 0,
                       'neut': item.get('neut', 'INDUSTRY') if isinstance(item, dict) else 'INDUSTRY',
                       'region': 'USA', 'universe': 'TOP3000',
                       'status': 'SUBMITTED', 'message': 'in-flight'}
            rowid = log_sim(pending)
            rec = sim_one(s, item)
            if rec.get('status') in ('RATE_LIMITED', 'SUBMIT_EXC', 'AUTH_FAIL'):
                fails = kv_get(con, 'consec_fail', 0) + 1
                kv_set(con, 'consec_fail', fails)
                wait = min(3600, 300 * fails)
                print(f"{rec['status']}, consec={fails}, sleep {wait}s", flush=True)
                update_sim(rowid, rec)
                heartbeat(get_history(), kv_get(con, 'submitted', []),
                          deadline, f"{rec['status']} cooldown")
                time.sleep(wait)
                if rec.get('status') == 'AUTH_FAIL':
                    try:
                        s = wq_session()
                    except Exception as e:
                        print('reauth fail:', e, flush=True)
                continue
            kv_set(con, 'consec_fail', 0)
            if rec.get('status') == 'COMPLETE' and args.auto_submit:
                rec = try_submit(s, con, rec, args.max_submits)
                print('SUBMIT-CHECK:', rec.get('submit', 'skipped'), flush=True)
            print('RESULT:', json.dumps(rec)[:400], flush=True)
            update_sim(rowid, rec)
            day = time.strftime('%Y-%m-%d', time.gmtime())
            sims = kv_get(con, 'sims_by_day', {})
            sims[day] = sims.get(day, 0) + 1
            kv_set(con, 'sims_by_day', sims)
            heartbeat(get_history(), kv_get(con, 'submitted', []),
                      deadline, f'round {rnd}')
            time.sleep(10)
        scored = sorted([h for h in get_history()
                         if h.get('sharpe') is not None],
                        key=lambda x: x['sharpe'], reverse=True)
        if scored:
            print(f"--- best: {scored[0]['expr']} sharpe {scored[0]['sharpe']} ---",
                  flush=True)
    print('DONE rounds', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
