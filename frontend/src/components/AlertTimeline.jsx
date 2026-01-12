import { ackAlert, resolveAlert } from "../api/client";

export default function AlertTimeline({ events, onAction }) {
  if (!Array.isArray(events) || events.length === 0) {
    return (
      <div style={{ color: "#666" }}>
        No alert history
      </div>
    );
  }

  const handleAck = async (id) => {
    await ackAlert(id);
    onAction?.();
  };

  const handleResolve = async (id) => {
    const note = prompt("Resolution note (optional):") || "";
    await resolveAlert(id, "sagar", note);
    onAction?.();
  };

  return (
    <div style={{ marginTop: 12 }}>
      {events.map((e) => (
        <div
          key={e.id}
          style={{
            borderLeft: "3px solid",
            borderColor:
              e.status === "RAISED"
                ? "#d33"
                : e.status === "ACKED"
                  ? "#e6a700"
                  : "#2a7",
            paddingLeft: 12,
            marginBottom: 12,
            background: "var(--panel)",
          }}
        >
          <div style={{ fontSize: 12, opacity: 0.7 }}>
            {new Date(e.run_ts).toLocaleString()}
          </div>

          <div style={{ fontWeight: "bold" }}>
            {e.symbol}
          </div>

          <div>Status: {e.status}</div>

          {/* ACTION BUTTONS */}
          <div style={{ marginTop: 6 }}>
            {e.status === "RAISED" && (
              <button
                onClick={() => handleAck(e.id)}
                style={{ marginRight: 8 }}
              >
                ACK
              </button>
            )}

            {e.status === "ACKED" && (
              <button
                onClick={() => handleResolve(e.id)}
              >
                RESOLVE
              </button>
            )}
          </div>

          {/* DETAILS */}
          {e.details && (
            <pre
              style={{
                fontSize: 11,
                background: "#f8f8f8",
                padding: 6,
                marginTop: 6,
                overflowX: "auto",
              }}
            >
              {JSON.stringify(e.details, null, 2)}
            </pre>
          )}
        </div>
      ))}
    </div>
  );
}
