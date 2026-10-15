'use client';

import { useCallback, useEffect, useRef, useState } from 'react';

const HISTORY = 120; // seconds of sparkline
// Grafana sits behind the same Caddy at /grafana/ on the live site. In local
// dev it's on its own port: NEXT_PUBLIC_GRAFANA_URL=http://localhost:3002/
const GRAFANA = process.env.NEXT_PUBLIC_GRAFANA_URL || '/grafana/';

const rupees = (paise) =>
  (paise / 100).toLocaleString('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 2 });
const count = (n) => (n ?? 0).toLocaleString('en-IN');
const ago = (iso, now) => {
  if (!iso) return '—';
  const s = Math.max(0, Math.round((now - new Date(iso)) / 1000));
  return s < 60 ? `${s}s ago` : `${Math.floor(s / 60)}m ${s % 60}s ago`;
};
const uptime = (iso, now) => {
  if (!iso || iso.startsWith('0001')) return '';
  const s = Math.max(0, Math.round((now - new Date(iso)) / 1000));
  return s < 120 ? `up ${s}s` : `up ${Math.floor(s / 60)}m`;
};

async function post(path, body) {
  const r = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  if (!r.ok) throw new Error(`${path}: ${r.status} ${await r.text()}`);
}

function Spark({ points, label }) {
  const w = 240, h = 48, pad = 3;
  if (points.length < 2) return <svg className="spark" viewBox={`0 0 ${w} ${h}`} role="img" aria-label={label} />;
  const max = Math.max(1, ...points);
  const x = (i) => pad + (i / (HISTORY - 1)) * (w - 2 * pad);
  const y = (v) => h - pad - (v / max) * (h - 2 * pad);
  const offset = HISTORY - points.length;
  const line = points.map((v, i) => `${i ? 'L' : 'M'}${x(i + offset).toFixed(1)},${y(v).toFixed(1)}`).join(' ');
  const area = `${line} L${x(HISTORY - 1)},${h - pad} L${x(offset)},${h - pad} Z`;
  const last = points[points.length - 1];
  return (
    <svg className="spark" viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" role="img" aria-label={`${label}: ${last}, peak ${max}`}>
      <line className="base" x1={pad} x2={w - pad} y1={h - pad} y2={h - pad} />
      <path className="area" d={area} />
      <path className="line" d={line} vectorEffect="non-scaling-stroke" />
      <circle className="end" cx={x(HISTORY - 1)} cy={y(last)} r="2.5" />
    </svg>
  );
}

const PAYMENT_COLORS = {
  captured: 'var(--good)', refunded: 'var(--accent)', authorized: 'var(--accent-soft)',
  pending: 'var(--warn)', created: 'var(--warn-soft)', failed: 'var(--bad)',
};

function PaymentBar({ payments }) {
  const order = ['captured', 'refunded', 'authorized', 'pending', 'created', 'failed'];
  const total = order.reduce((s, k) => s + (payments[k] || 0), 0) || 1;
  return (
    <>
      <div className="bar" aria-hidden="true">
        {order.map((k) => payments[k] ? <span key={k} style={{ width: `${(100 * payments[k]) / total}%`, background: PAYMENT_COLORS[k] }} /> : null)}
      </div>
      <div className="legend">
        {order.filter((k) => payments[k]).map((k) => (
          <span key={k}><i style={{ background: PAYMENT_COLORS[k] }} />{k} <span className="num">{count(payments[k])}</span></span>
        ))}
      </div>
    </>
  );
}

const BANK = [
  ['healthy', 'Healthy', 'Answers in 5–30 ms, one callback each.'],
  ['flaky', 'Flaky', '20% declines, 10% timeouts, 20% decided later, up to 3 duplicate callbacks.'],
  ['late', 'Late', 'Half the answers arrive 15 s later; 30% of replies time out.'],
  ['outage', 'Outage', 'Every call fails with 503 and nothing is recorded.'],
];
const MERCHANT = [
  ['ok', 'Up', 'Answers 200 after checking the signature.'],
  ['fail', '500s', 'Every webhook fails, so Kosh backs off and retries.'],
  ['hang', 'Hangs', 'Never answers; each attempt times out after 3 s.'],
];
const RATES = [0, 5, 20, 50];

export default function Lab() {
  const [state, setState] = useState(null);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState('');
  const [now, setNow] = useState(Date.now());
  const [logsFor, setLogsFor] = useState('');
  const hist = useRef({ outbox: [], backlog: [], sales: [], retries: [], last: null });
  const [, force] = useState(0);

  const poll = useCallback(async () => {
    try {
      const r = await fetch(`/lab/state${logsFor ? '?logs=1' : ''}`, { cache: 'no-store' });
      if (!r.ok) throw new Error(`lab answered ${r.status}`);
      const s = await r.json();
      const h = hist.current;
      const push = (arr, v) => { arr.push(v); if (arr.length > HISTORY) arr.shift(); };
      push(h.outbox, s.outbox?.unpublished ?? 0);
      push(h.backlog, s.webhooks?.backlog ?? 0);
      if (h.last) {
        push(h.sales, Math.max(0, s.traffic.completed - h.last.completed));
        push(h.retries, Math.max(0, s.traffic.retries - h.last.retries));
      }
      h.last = s.traffic;
      setState(s);
      setError('');
      force((n) => n + 1);
    } catch (e) {
      setError(`Can't reach the lab (${e.message}). Start it with: go run ./cmd/lab`);
    }
    setNow(Date.now());
  }, [logsFor]);

  // Poll only while the tab is visible: the lab pauses traffic when no one
  // has polled for 10 minutes, and a forgotten background tab shouldn't count.
  useEffect(() => {
    const tick = () => { if (!document.hidden) poll(); };
    tick();
    const t = setInterval(tick, 1000);
    document.addEventListener('visibilitychange', tick);
    return () => { clearInterval(t); document.removeEventListener('visibilitychange', tick); };
  }, [poll]);

  const act = async (key, path, body) => {
    setBusy(key);
    try { await post(path, body); await poll(); } catch (e) { setError(e.message); }
    setBusy('');
  };

  const s = state;
  const h = hist.current;
  const inv = s?.invariants;
  const balanced = s?.ledger?.balanced;
  const backlog = s?.webhooks?.backlog ?? 0;
  const unpublished = s?.outbox?.unpublished ?? 0;
  const proc = s?.processes?.find((p) => p.name === logsFor);

  return (
    <main className="shell">
      <header className="top">
        <div>
          <h1>Kosh Failure Lab</h1>
          <p>Kill processes, break the bank, take the merchant down. The ledger has to stay balanced and every rule has to keep holding.</p>
          <p className="links">
            <a href={GRAFANA} target="_blank" rel="noreferrer">Live metrics in Grafana ↗</a>
            <span>API latency, ledger lock waits, outbox lag, webhook delivery</span>
          </p>
        </div>
        <div className="pills" aria-live="polite">
          {s ? (
            <>
              <span className={`pill ${balanced ? 'good' : 'bad'}`}><span className="dot" />
                {balanced ? 'Σ debits = Σ credits' : 'Ledger out of balance'}
              </span>
              <span className={`pill ${inv?.ok ? 'good' : inv?.checked_at ? 'bad' : ''}`}><span className="dot" />
                {inv?.checked_at ? `${inv.held}/${inv.total} invariants hold · ${ago(inv.checked_at, now)}` : 'Checking invariants…'}
              </span>
            </>
          ) : <span className="pill warn"><span className="dot" />Connecting…</span>}
        </div>
      </header>

      {error && <div className="banner" role="alert">{error}</div>}
      {inv && !inv.ok && inv.checked_at && (
        <div className="banner" role="alert">
          An invariant is broken. This should never happen; it means a bug.
          <ul>{(inv.failures || [inv.error]).map((f) => <li key={f}>{f}</li>)}</ul>
        </div>
      )}

      <div className="grid">
        <aside className="stack" aria-label="Controls">
          <section className="panel">
            <h2>Processes</h2>
            <div className="procs">
              {(s?.processes || []).map((p) => (
                <div className="proc" key={p.name}>
                  <div><span className="name">{p.name}</span><span className={`state ${p.state}`}>{p.state}</span></div>
                  <div className="meta">
                    {p.state === 'running' ? `pid ${p.pid} · ${uptime(p.started_at, now)}` : p.last_exit}
                    {p.restarts ? ` · ${p.restarts} restart${p.restarts > 1 ? 's' : ''}` : ''}
                  </div>
                  <div className="act">
                    {p.state === 'running'
                      ? <button className="danger" disabled={busy === p.name} onClick={() => act(p.name, `/lab/process/${p.name}/kill`)}>Kill</button>
                      : <button disabled={busy === p.name} onClick={() => act(p.name, `/lab/process/${p.name}/start`)}>Start</button>}
                  </div>
                </div>
              ))}
            </div>
            <p className="hint">Kill sends SIGKILL: no shutdown, no flush. A killed process restarts on its own after 3 minutes.</p>
          </section>

          <section className="panel">
            <h2>Bank</h2>
            <div className="seg" role="group" aria-label="Bank behaviour">
              {BANK.map(([k, label]) => (
                <button key={k} aria-pressed={s?.bank === k} disabled={busy === `bank-${k}`} onClick={() => act(`bank-${k}`, '/lab/bank', { preset: k })}>{label}</button>
              ))}
            </div>
            <p className="hint">{BANK.find(([k]) => k === s?.bank)?.[2] || ''}{s && s.bank !== 'healthy' ? ' Back to Healthy on its own after 15 minutes.' : ''}</p>
          </section>

          <section className="panel">
            <h2>Merchant webhook endpoint</h2>
            <div className="seg" role="group" aria-label="Merchant endpoint">
              {MERCHANT.map(([k, label]) => (
                <button key={k} aria-pressed={s?.merchant_endpoint === k} disabled={busy === `m-${k}`} onClick={() => act(`m-${k}`, '/lab/merchant', { mode: k })}>{label}</button>
              ))}
            </div>
            <p className="hint">{MERCHANT.find(([k]) => k === s?.merchant_endpoint)?.[2] || ''}{s && s.merchant_endpoint !== 'ok' ? ' Back to Up on its own after 15 minutes.' : ''}</p>
          </section>

          <section className="panel">
            <h2>Customer traffic</h2>
            <div className="seg" role="group" aria-label="Sales per second">
              {RATES.map((r) => (
                <button key={r} aria-pressed={s?.traffic?.rate === r} disabled={busy === `t-${r}`} onClick={() => act(`t-${r}`, '/lab/traffic', { rate: r })}>{r ? `${r}/s` : 'Stop'}</button>
              ))}
            </div>
            <p className="hint">Each sale is order → payment → capture; 15% get a partial refund. Clients retry with the same Idempotency-Key. Traffic pauses when no one is watching, and the lab starts fresh every night at 04:00 IST.</p>
          </section>
        </aside>

        <section className="stack" aria-label="Live state">
          <div className="panel">
            <h2>Ledger</h2>
            <div className="ledger">
              <div className="figure"><span className="label">Σ debits, posted</span><span className="value">{s ? rupees(s.ledger.posted_dr) : '—'}</span></div>
              <div className="figure"><span className="label">Σ credits, posted</span><span className="value">{s ? rupees(s.ledger.posted_cr) : '—'}</span>
                <span className={`sub ${balanced ? 'equals' : 'unequal'}`}>{s ? (s.ledger.posted_dr === s.ledger.posted_cr ? '= debits, to the paisa' : `off by ${rupees(Math.abs(s.ledger.posted_dr - s.ledger.posted_cr))}`) : ''}</span></div>
              <div className="figure"><span className="label">On hold (refunds, payouts)</span><span className="value">{s ? rupees(s.ledger.pending_dr) : '—'}</span>
                <span className="sub">{s ? `${count(s.ledger.entries)} journal entries` : ''}</span></div>
            </div>
          </div>

          <div className="tiles">
            <div className="panel tile">
              <div className="row"><h2>Sales completed / s</h2><span className="big">{h.sales.at(-1) ?? 0}</span></div>
              <Spark points={h.sales} label="Sales per second" />
              <dl className="kv">
                <dt>Started</dt><dd>{count(s?.traffic?.started)}</dd>
                <dt>Completed</dt><dd>{count(s?.traffic?.completed)}</dd>
                <dt>Gave up after retries</dt><dd>{count(s?.traffic?.failed)}</dd>
                <dt>Client retries</dt><dd>{count(s?.traffic?.retries)}</dd>
              </dl>
            </div>

            <div className="panel tile">
              <div className="row"><h2>Events waiting to publish</h2><span className={`big ${unpublished > 100 ? 'warn' : ''}`}>{count(unpublished)}</span></div>
              <Spark points={h.outbox} label="Unpublished outbox rows" />
              <dl className="kv">
                <dt>Oldest waiting</dt><dd>{s ? `${(s.outbox.oldest_seconds ?? 0).toFixed(1)} s` : '—'}</dd>
                <dt>Published</dt><dd>{count(s?.outbox?.published)}</dd>
              </dl>
              <p className="hint">Kill the relay: events pile up here, safely in Postgres, and drain on restart.</p>
            </div>

            <div className="panel tile">
              <div className="row"><h2>Webhooks not yet delivered</h2><span className={`big ${backlog > 100 ? 'warn' : ''}`}>{count(backlog)}</span></div>
              <Spark points={h.backlog} label="Undelivered webhooks" />
              <dl className="kv">
                <dt>Delivered</dt><dd>{count(s?.webhooks?.delivered)}</dd>
              </dl>
              <p className="hint">Break the merchant endpoint: deliveries back off and retry, then catch up.</p>
            </div>

            <div className="panel tile">
              <h2>Payments</h2>
              {s && <PaymentBar payments={s.payments} />}
              <dl className="kv">
                <dt>Payouts made</dt><dd>{count(s?.payouts?.processed)}</dd>
                <dt>Recon lines matched</dt><dd>{count(s?.recon?.matched)}</dd>
                <dt>Recon exceptions open</dt><dd>{count(s?.recon?.open)}</dd>
              </dl>
            </div>
          </div>

          <div className="tiles">
            <div className="panel">
              <h2>What you did</h2>
              <ol className="log" reversed>
                {(s?.events || []).slice().reverse().map((e, i) => (
                  <li key={`${e.at}-${i}`}><time>{new Date(e.at).toLocaleTimeString('en-IN', { hour12: false })}</time>{e.what}</li>
                ))}
              </ol>
            </div>
            <div className="panel">
              <div className="row" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 }}>
                <h2>Process output</h2>
                <select id="log-proc" aria-label="Process" value={logsFor} onChange={(e) => setLogsFor(e.target.value)}>
                  <option value="">Choose a process</option>
                  {(s?.processes || []).map((p) => <option key={p.name} value={p.name}>{p.name}</option>)}
                </select>
              </div>
              <pre className="tail">{logsFor ? (proc?.logs || []).join('\n') || 'No output yet.' : 'Pick a process to see its last 30 log lines.'}</pre>
            </div>
          </div>
        </section>
      </div>
    </main>
  );
}
