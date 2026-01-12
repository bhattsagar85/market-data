export default function StatusBadge({ status }) {
  if (status === null) {
    return <span style={{ color: "#999" }}>—</span>;
  }

  if (status === "PASS" || status === "COMPLETE") {
    return <span style={{ color: "green" }}>✅</span>;
  }

  if (status === "PARTIAL") {
    return <span style={{ color: "orange" }}>🟡</span>;
  }

  return <span style={{ color: "red" }}>❌</span>;
}
