// Consola del operador: carga alertas recientes, recibe nuevas por SSE y permite validarlas.
const state = { alerts: new Map(), filter: 'all' };
const fmtTime = iso => iso ? new Date(iso).toLocaleTimeString('es-GT', { hour12: false }) : '';
const TYPE_LABELS = {
  ROBO_EN_CURSO: 'Robo en curso', ACCIDENTE_VIAL: 'Accidente vial', DISPAROS_CONFIRMADOS: 'Disparos confirmados',
  INCENDIO_ESTRUCTURAL: 'Incendio estructural', VEHICULO_REPORTADO: 'Vehiculo con reporte de robo',
  VEHICULO_EN_FUGA: 'Vehiculo sospechoso en fuga', EMERGENCIA_MEDICA: 'Emergencia medica',
  POSIBLE_FALSO_POSITIVO: 'Posible falso positivo', DISPOSITIVO_SOSPECHOSO: 'Dispositivo sospechoso',
  SLO_INCUMPLIDO: 'Degradacion del servicio'
};
const STATUS_LABELS = { NEW: 'Nueva', ESCALATED: 'Escalada', UPDATED: 'Actualizada (nuevo testigo)', AUTO_CLASSIFIED: 'Clasificada automaticamente',
  CONFIRMED: 'Confirmada', FALSE_POSITIVE: 'Falso positivo', EXPIRED: 'Expirada' };

document.getElementById('grafana-link').href = `${location.protocol}//${location.hostname}:3000`;

function matchesFilter(a) {
  switch (state.filter) {
    case 'open': return ['NEW', 'ESCALATED', 'UPDATED'].includes(a.status);
    case 'critical': return a.severity === 'CRITICAL';
    case 'fp': return a.alertType === 'POSIBLE_FALSO_POSITIVO' || a.status === 'FALSE_POSITIVE';
    case 'validated': return !!a.validatedBy;
    default: return true;
  }
}

function render(freshId) {
  const container = document.getElementById('alerts');
  const list = [...state.alerts.values()].filter(matchesFilter)
    .sort((a, b) => new Date(b.updatedAt) - new Date(a.updatedAt)).slice(0, 150);
  container.replaceChildren();
  if (list.length === 0) {
    container.innerHTML = '<p class="empty">Sin alertas para este filtro.</p>';
    return;
  }
  const template = document.getElementById('alert-template');
  for (const a of list) {
    const node = template.content.firstElementChild.cloneNode(true);
    node.classList.add(`sev-${a.severity}`);
    if (a.alertId === freshId) node.classList.add('fresh');
    node.querySelector('.badge').textContent = a.severity;
    node.querySelector('.title').textContent = TYPE_LABELS[a.alertType] || a.title;
    node.querySelector('.zone').textContent = a.zoneName;
    node.querySelector('.time').textContent = `${fmtTime(a.createdAt)}${a.revision > 1 ? ' · actualizada ' + fmtTime(a.updatedAt) : ''}`;
    node.querySelector('.description').textContent = a.description;
    const meta = node.querySelector('.meta');
    const parts = [`Estado: ${STATUS_LABELS[a.status] || a.status}`, `Confianza: ${(a.confidence ?? 0).toFixed(2)}`,
      `Revision ${a.revision}`, `Regla ${a.ruleId}`];
    if (a.latencyMs != null) parts.push(`Latencia ${a.latencyMs} ms`);
    if (a.plateMasked) parts.push(`Placa ${a.plateMasked}`);
    meta.replaceChildren(...parts.map(t => Object.assign(document.createElement('span'), { textContent: t })));
    const ul = node.querySelector('.evidence');
    for (const ev of (a.evidence || [])) {
      const li = document.createElement('li');
      li.textContent = `${fmtTime(ev.eventTime)} · ${ev.summary} (${ev.sensorId}, peso ${ev.weight})`;
      ul.appendChild(li);
    }
    const validation = node.querySelector('.validation');
    if (a.validatedBy) {
      node.querySelectorAll('.actions button').forEach(b => b.remove());
      validation.textContent = `Validada por ${a.validatedBy} a las ${fmtTime(a.validatedAt)}`;
    } else if (a.zoneId === 0) {
      node.querySelectorAll('.actions button').forEach(b => b.remove());
    } else {
      node.querySelector('.confirm').onclick = () => validate(a.alertId, 'CONFIRMED');
      node.querySelector('.false-positive').onclick = () => validate(a.alertId, 'FALSE_POSITIVE');
    }
    container.appendChild(node);
  }
}

async function validate(id, resolution) {
  const notes = prompt(resolution === 'CONFIRMED' ? 'Notas (unidad enviada, etc.):' : 'Motivo del falso positivo:') ?? '';
  const res = await fetch(`api/alerts/${id}/validation`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ resolution, notes })
  });
  if (res.ok) { const a = await res.json(); state.alerts.set(a.alertId, a); render(); loadSummary(); }
  else alert('No se pudo validar la alerta');
}

async function loadAlerts() {
  const since = new Date(Date.now() - 6 * 3600 * 1000).toISOString();
  const res = await fetch(`api/alerts?since=${encodeURIComponent(since)}&limit=500`);
  if (!res.ok) return;
  for (const a of await res.json()) state.alerts.set(a.alertId, a);
  render();
}

async function loadSummary() {
  const res = await fetch('api/stats/summary?minutes=60');
  if (!res.ok) return;
  const s = await res.json();
  document.getElementById('s-total').textContent = s.total;
  document.getElementById('s-critical').textContent = s.critical;
  document.getElementById('s-open').textContent = s.open;
  document.getElementById('s-fp').textContent = s.possible_false_positives;
  document.getElementById('s-p95').textContent = s.p95_latency_ms == null ? '-' : `${Math.round(s.p95_latency_ms)} ms`;
}

function connect() {
  const live = document.getElementById('live');
  const source = new EventSource('api/alerts/stream');
  source.onopen = () => { live.textContent = 'En vivo'; live.className = 'live on'; };
  source.addEventListener('alert', e => {
    const a = JSON.parse(e.data);
    state.alerts.set(a.alertId, a);
    render(a.alertId);
    loadSummary();
  });
  source.onerror = () => { live.textContent = 'Reconectando…'; live.className = 'live off'; };
}

document.getElementById('filters').addEventListener('click', e => {
  if (e.target.tagName !== 'BUTTON') return;
  document.querySelectorAll('#filters button').forEach(b => b.classList.toggle('active', b === e.target));
  state.filter = e.target.dataset.filter;
  render();
});

loadAlerts();
loadSummary();
connect();
setInterval(loadSummary, 15000);
