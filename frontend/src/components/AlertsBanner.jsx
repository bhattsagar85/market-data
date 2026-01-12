export default function AlertsBanner({ alerts }) {
  const alertList = Array.isArray(alerts) ? alerts : [];

  if (alertList.length === 0) {
    return (
      <div style={{ padding: 12, background: "#e6fffa" }}>
        ✅ No active alerts
      </div>
    );
  }

  return (
    <div style={{ padding: 12, background: "#ffe6e6" }}>
      🔴 {alertList.length} active alert(s)
    </div>
  );
}
