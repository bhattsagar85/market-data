export default function SpinnerOverlay({ visible }) {
  if (!visible) return null;

  return (
    <div style={overlayStyle}>
      <div style={spinnerBoxStyle}>
        <div style={spinnerStyle} />
        <div style={{ marginTop: 8, fontSize: 12, color: "#555" }}>
          Refreshing…
        </div>
      </div>
    </div>
  );
}

const overlayStyle = {
  position: "absolute",
  top: 0,
  left: 0,
  right: 0,
  bottom: 0,
  background: "rgba(255,255,255,0.7)",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  zIndex: 10,
};

const spinnerBoxStyle = {
  display: "flex",
  flexDirection: "column",
  alignItems: "center",
};

const spinnerStyle = {
  width: 32,
  height: 32,
  border: "4px solid #ddd",
  borderTop: "4px solid #333",
  borderRadius: "50%",
  animation: "spin 1s linear infinite",
};
