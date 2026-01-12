import { useEffect, useMemo, useState } from "react";

import {
  getHealth,
  getAlerts,
  getAlertHistory,
} from "../api/client";

import AlertsBanner from "../components/AlertsBanner";
import HealthTable from "../components/HealthTable";
import SymbolModal from "../components/SymbolModal";
import SpinnerOverlay from "../components/SpinnerOverlay";
import AlertTimeline from "../components/AlertTimeline";

const REFRESH_INTERVAL_MS = 30_000; // 30 seconds

// --------------------------------------
// URL helpers (FILTER + HISTORY)
// --------------------------------------
const getInitialFilter = () => {
  const params = new URLSearchParams(window.location.search);
  const f = params.get("filter");
  return ["ALL", "FAIL", "ALERT"].includes(f) ? f : "ALL";
};

const getInitialHistory = () => {
  const params = new URLSearchParams(window.location.search);
  return params.get("history") === "1";
};

export default function Dashboard() {
  // --------------------------------------
  // Theme (Dark / Light)
  // --------------------------------------
  const [darkMode, setDarkMode] = useState(
    () => localStorage.getItem("theme") === "dark"
  );

  useEffect(() => {
    document.body.classList.toggle("dark", darkMode);
    localStorage.setItem("theme", darkMode ? "dark" : "light");
  }, [darkMode]);

  // --------------------------------------
  // State
  // --------------------------------------
  const [health, setHealth] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [alertHistory, setAlertHistory] = useState([]);

  const [selectedSymbol, setSelectedSymbol] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [isRefreshing, setIsRefreshing] = useState(false);

  const [filter, setFilter] = useState(getInitialFilter);
  const [showHistory, setShowHistory] = useState(getInitialHistory);

  // --------------------------------------
  // Fetch dashboard data (DEFENSIVE)
  // --------------------------------------
  const fetchData = async () => {
    try {
      setIsRefreshing(true);

      // Critical APIs
      const [healthData, alertData] = await Promise.all([
        getHealth(),
        getAlerts(),
      ]);

      setHealth(Array.isArray(healthData) ? healthData : []);
      setAlerts(Array.isArray(alertData) ? alertData : []);
      setLastUpdated(new Date());

      // Non-critical API (best effort)
      try {
        const historyData = await getAlertHistory();
        setAlertHistory(
          Array.isArray(historyData) ? historyData : []
        );
      } catch (err) {
        console.warn("Alert history unavailable", err);
      }
    } catch (err) {
      console.error("Dashboard refresh failed", err);
    } finally {
      setIsRefreshing(false);
    }
  };

  // --------------------------------------
  // Auto-refresh lifecycle
  // --------------------------------------
  useEffect(() => {
    fetchData();

    // Pause auto-refresh when modal open
    if (selectedSymbol !== null) return;

    const intervalId = setInterval(fetchData, REFRESH_INTERVAL_MS);
    return () => clearInterval(intervalId);
  }, [selectedSymbol]);

  // --------------------------------------
  // Persist filter + history → URL
  // --------------------------------------
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    params.set("filter", filter);
    params.set("history", showHistory ? "1" : "0");

    const newUrl = `${window.location.pathname}?${params.toString()}`;
    window.history.replaceState(null, "", newUrl);
  }, [filter, showHistory]);

  // --------------------------------------
  // Derived helpers
  // --------------------------------------
  const alertSymbols = useMemo(() => {
    if (!Array.isArray(alerts)) return new Set();
    return new Set(alerts.map((a) => a.symbol));
  }, [alerts]);

  const isFailRow = (row) => {
    if (row.daily_coverage === "FAIL") return true;
    return Object.values(row.freshness || {}).some(
      (v) => v === "FAIL"
    );
  };

  const filteredHealth = useMemo(() => {
    if (filter === "FAIL") {
      return health.filter(isFailRow);
    }

    if (filter === "ALERT") {
      return health.filter((row) =>
        alertSymbols.has(row.symbol)
      );
    }

    return health;
  }, [filter, health, alertSymbols]);

  // --------------------------------------
  // Render
  // --------------------------------------
  return (
    <div style={{ padding: 24 }}>
      <h2>Market Data Governance Dashboard</h2>

      {/* Status bar */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 12,
          fontSize: 12,
          marginBottom: 8,
        }}
      >
        <span>
          Last updated:{" "}
          {lastUpdated
            ? lastUpdated.toLocaleTimeString()
            : "—"}
          {selectedSymbol && (
            <span style={{ marginLeft: 6, opacity: 0.7 }}>
              (paused)
            </span>
          )}
        </span>

        <button
          onClick={fetchData}
          disabled={isRefreshing}
        >
          {isRefreshing ? "Refreshing…" : "Refresh now"}
        </button>

        {/* Dark mode toggle */}
        <button
          onClick={() => setDarkMode(!darkMode)}
        >
          {darkMode ? "☀️ Light" : "🌙 Dark"}
        </button>
      </div>

      {/* Sticky alerts */}
      <div
        style={{
          position: "sticky",
          top: 0,
          zIndex: 20,
          marginBottom: 12,
        }}
      >
        <AlertsBanner alerts={alerts} />
      </div>

      {/* Filters */}
      <div style={{ display: "flex", gap: 8 }}>
        {["ALL", "FAIL", "ALERT"].map((f) => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            style={{
              fontWeight:
                filter === f ? "bold" : "normal",
            }}
          >
            {f}
          </button>
        ))}
      </div>

      {/* Health table */}
      <div style={{ marginTop: 16, position: "relative" }}>
        <HealthTable
          data={filteredHealth}
          onSelect={setSelectedSymbol}
        />
        <SpinnerOverlay visible={isRefreshing} />
      </div>

      {/* Alert history */}
      <div style={{ marginTop: 32 }}>
        <button
          onClick={() =>
            setShowHistory(!showHistory)
          }
        >
          {showHistory
            ? "Hide"
            : "Show"} Alert History
        </button>

        {showHistory && (
          <AlertTimeline
            events={alertHistory}
            onAction={fetchData}
          />
        )}
      </div>

      {/* Drill-down modal */}
      <SymbolModal
        symbol={selectedSymbol}
        onClose={() => setSelectedSymbol(null)}
      />
    </div>
  );
}
