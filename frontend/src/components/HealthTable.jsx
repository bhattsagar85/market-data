import StatusBadge from "./StatusBadge";

export default function HealthTable({ data, onSelect }) {
  // Determine row highlight color
  const getRowStyle = (row) => {
    // 🔴 ALERT has highest priority
    if (row.auto_backfill === "ALERT") {
      return { backgroundColor: "#ffe6e6" };
    }

    // ❌ FAIL (daily or intraday)
    if (row.daily_coverage === "FAIL") {
      return { backgroundColor: "#fff4e6" };
    }

    const hasFailFreshness = Object.values(
      row.freshness || {}
    ).some((v) => v === "FAIL");

    if (hasFailFreshness) {
      return { backgroundColor: "#fff4e6" };
    }

    // 🟡 PARTIAL backfill
    if (row.auto_backfill === "PARTIAL") {
      return { backgroundColor: "#fffbe6" };
    }

    return {};
  };

  return (
    <table
      border="1"
      cellPadding="8"
      style={{
        width: "100%",
        borderCollapse: "collapse",
      }}
    >
      <thead>
        <tr style={{ background: "#f5f5f5" }}>
          <th align="left">Symbol</th>
          <th>Daily</th>
          <th>Backfill</th>
          <th>5M</th>
          <th>15M</th>
          <th></th>
        </tr>
      </thead>

      <tbody>
        {data.map((row) => (
          <tr
            key={row.symbol}
            style={getRowStyle(row)}
          >
            <td>{row.symbol}</td>
            <td>
              <StatusBadge status={row.daily_coverage} />
            </td>
            <td>
              <StatusBadge status={row.auto_backfill} />
            </td>
            <td>
              <StatusBadge status={row.freshness["5M"]} />
            </td>
            <td>
              <StatusBadge status={row.freshness["15M"]} />
            </td>
            <td>
              <button
                onClick={() => onSelect(row.symbol)}
                style={{ cursor: "pointer" }}
              >
                View
              </button>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
