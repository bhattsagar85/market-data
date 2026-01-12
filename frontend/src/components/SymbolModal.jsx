import { useEffect, useState } from "react";
import { getSymbolHistory } from "../api/client";
import StatusBadge from "./StatusBadge";

export default function SymbolModal({ symbol, onClose }) {
  const [events, setEvents] = useState([]);

  useEffect(() => {
    if (symbol) {
      getSymbolHistory(symbol).then(setEvents);
    }
  }, [symbol]);

  if (!symbol) return null;

  return (
    <div style={overlayStyle}>
      <div style={modalStyle}>
        <h3>Symbol History: {symbol}</h3>

        <table width="100%" border="1" cellPadding="6">
          <thead>
            <tr>
              <th>Time</th>
              <th>Check</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {events.map((e, i) => (
              <tr key={i}>
                <td>{new Date(e.run_ts).toLocaleString()}</td>
                <td>{e.check_type}</td>
                <td><StatusBadge status={e.status} /></td>
              </tr>
            ))}
          </tbody>
        </table>

        <button style={{ marginTop: 12 }} onClick={onClose}>
          Close
        </button>
      </div>
    </div>
  );
}

const overlayStyle = {
  position: "fixed",
  top: 0,
  left: 0,
  right: 0,
  bottom: 0,
  background: "rgba(0,0,0,0.4)",
  display: "flex",
  justifyContent: "center",
  alignItems: "center",
};

const modalStyle = {
  background: "#fff",
  padding: 20,
  width: "70%",
  maxHeight: "80%",
  overflowY: "auto",
  borderRadius: 6,
};
