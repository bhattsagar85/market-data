const BASE_URL = "http://127.0.0.1:8000";

export async function getHealth() {
  const res = await fetch(`${BASE_URL}/health/symbols`);
  return res.json();
}

export async function getAlerts() {
  const res = await fetch(`${BASE_URL}/alerts/active`);
  return res.json();
}

export async function getSymbolHistory(symbol) {
  const res = await fetch(`${BASE_URL}/symbols/${symbol}/history`);
  return res.json();
}

export async function getAlertHistory() {
  const res = await fetch("http://127.0.0.1:8000/alerts/history");
  return res.json();
}

export async function ackAlert(alertId, user = "sagar") {
  const res = await fetch(
    `http://127.0.0.1:8000/alerts/${alertId}/ack?user=${user}`,
    { method: "POST" }
  );
  return res.json();
}

export async function resolveAlert(
  alertId,
  user = "sagar",
  note = ""
) {
  const res = await fetch(
    `http://127.0.0.1:8000/alerts/${alertId}/resolve?user=${user}&note=${encodeURIComponent(note)}`,
    { method: "POST" }
  );
  return res.json();
}

