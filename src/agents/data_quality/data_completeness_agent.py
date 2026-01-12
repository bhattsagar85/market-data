from datetime import datetime, timedelta, date, time, timezone
from typing import Dict, Any
import pytz

from psycopg2.extras import Json

from agents.calendar.market_holiday_agent import MarketHolidayAgent
from agents.backfill.backfill_agent import BackfillAgent
from agents.backfill.intraday_backfill_agent import IntradayBackfillAgent
from data_ingestion.timeframe_mapper import TIMEFRAMES


IST = pytz.timezone("Asia/Kolkata")

MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)

TIMEFRAME_MINUTES = {
    tf: meta["minutes"]
    for tf, meta in TIMEFRAMES.items()
}


class DataCompletenessAgent:
    """
    Data Governance Agent.

    Responsibilities:
    - Daily coverage checks (holiday-aware)
    - Daily auto-backfill with escalation
    - Intraday freshness checks
    - Intraday completeness detection
    - Intraday auto-backfill (safe & throttled)
    - Intraday escalation if healing repeatedly fails
    """

    MAX_PARTIAL_DAYS = 3
    MAX_INTRADAY_PARTIAL_RUNS = 3

    # ─────────────────────────────────────────────
    # ENTRY POINT
    # ─────────────────────────────────────────────
    def run(self, conn):
        cur = conn.cursor()

        cur.execute("""
            SELECT DISTINCT symbol, timeframe
            FROM candles
        """)
        rows = cur.fetchall()

        for row in rows:
            symbol = row["symbol"]
            timeframe = row["timeframe"]

            if timeframe == "1D":
                self._check_daily_coverage(conn, symbol)
            else:
                self._check_intraday_freshness(conn, symbol, timeframe)
                self._check_intraday_completeness(conn, symbol, timeframe)

    # ------------------------------------------------------------------
    # DAILY COVERAGE (EXISTING)
    # ------------------------------------------------------------------
    def _check_daily_coverage(self, conn, symbol: str):
        cur = conn.cursor()

        cur.execute("""
            SELECT
                MIN(ts)::date AS first_day,
                MAX(ts)::date AS last_day
            FROM candles
            WHERE symbol = %s
              AND timeframe = '1D'
        """, (symbol,))

        row = cur.fetchone()
        if not row or not row["first_day"]:
            return

        first_day: date = row["first_day"]
        last_day: date = row["last_day"]

        expected_days = 0
        d = first_day
        while d <= last_day:
            if MarketHolidayAgent.is_trading_day(d):
                expected_days += 1
            d += timedelta(days=1)

        cur.execute("""
            SELECT COUNT(*) AS cnt
            FROM candles
            WHERE symbol = %s
              AND timeframe = '1D'
        """, (symbol,))
        actual_days = cur.fetchone()["cnt"]

        status = "PASS" if actual_days >= expected_days else "FAIL"

        self.persist_report(
            conn,
            symbol,
            "1D",
            "daily_coverage",
            status,
            {
                "expected_trading_days": expected_days,
                "actual_days": actual_days,
                "coverage_pct": round(
                    (actual_days / expected_days) * 100, 2
                ) if expected_days else 0.0,
                "range": {
                    "from": first_day.isoformat(),
                    "to": last_day.isoformat()
                }
            }
        )

        if status == "FAIL":
            self._auto_backfill_and_alert(conn, symbol, first_day, last_day)

    # ------------------------------------------------------------------
    # DAILY AUTO-BACKFILL + ESCALATION (EXISTING)
    # ------------------------------------------------------------------
    def _auto_backfill_and_alert(
        self,
        conn,
        symbol: str,
        start_day: date,
        end_day: date
    ):
        backfill_agent = BackfillAgent()

        missing_days = backfill_agent.get_missing_trading_days(
            conn, symbol, start_day, end_day
        )

        if not missing_days:
            return

        healed_days = backfill_agent.backfill_daily(
            symbol=symbol,
            missing_days=missing_days
        )

        backfill_status = (
            "COMPLETE"
            if len(healed_days) == len(missing_days)
            else "PARTIAL"
        )

        self.persist_report(
            conn,
            symbol,
            "1D",
            "auto_backfill",
            backfill_status,
            {
                "healed_days": [d.isoformat() for d in healed_days],
                "healed_count": len(healed_days),
                "remaining_count": len(missing_days) - len(healed_days),
                "throttle_limit": backfill_agent.MAX_DAYS_PER_RUN
            }
        )

        if backfill_status == "PARTIAL":
            partial_days = self._count_consecutive_partial_days(conn, symbol)
            if partial_days > self.MAX_PARTIAL_DAYS:
                self.persist_report(
                    conn,
                    symbol,
                    "1D",
                    "auto_backfill_alert",
                    "RAISED",
                    {
                        "consecutive_partial_days": partial_days,
                        "threshold": self.MAX_PARTIAL_DAYS,
                        "message": "Auto-backfill stuck in PARTIAL state"
                    }
                )

    def _count_consecutive_partial_days(self, conn, symbol: str) -> int:
        cur = conn.cursor()
        cur.execute("""
            SELECT status
            FROM data_quality_reports
            WHERE symbol = %s
              AND check_type = 'auto_backfill'
            ORDER BY run_ts DESC
            LIMIT %s
        """, (symbol, self.MAX_PARTIAL_DAYS + 5))

        count = 0
        for row in cur.fetchall():
            if row["status"] == "PARTIAL":
                count += 1
            else:
                break
        return count

    # ------------------------------------------------------------------
    # INTRADAY FRESHNESS (EXISTING)
    # ------------------------------------------------------------------
    def _check_intraday_freshness(self, conn, symbol: str, timeframe: str):
        cur = conn.cursor()

        cur.execute("""
            SELECT MAX(ts) AS last_ts
            FROM candles
            WHERE symbol = %s
              AND timeframe = %s
        """, (symbol, timeframe))

        row = cur.fetchone()
        if not row or not row["last_ts"]:
            return

        last_ts: datetime = row["last_ts"]
        now = datetime.now(timezone.utc)

        lag_min = (now - last_ts).total_seconds() / 60
        lag_min = max(lag_min, 0.0)

        thresholds = {"1M": 5, "5M": 15, "15M": 30}
        max_allowed = thresholds.get(timeframe, 60)

        status = "PASS" if lag_min <= max_allowed else "FAIL"

        self.persist_report(
            conn,
            symbol,
            timeframe,
            "freshness",
            status,
            {
                "last_candle_ts": last_ts.isoformat(),
                "lag_minutes": round(lag_min, 2),
                "threshold_minutes": max_allowed
            }
        )

    # ------------------------------------------------------------------
    # 🆕 INTRADAY COMPLETENESS + BACKFILL + ESCALATION
    # ------------------------------------------------------------------
    def _check_intraday_completeness(self, conn, symbol: str, timeframe: str):
        if timeframe not in TIMEFRAME_MINUTES:
            return

        cur = conn.cursor()
        holiday_agent = MarketHolidayAgent()

        cur.execute("""
            SELECT DISTINCT ts::date AS trade_date
            FROM candles
            WHERE symbol = %s
              AND timeframe = %s
            ORDER BY trade_date DESC
            LIMIT 5
        """, (symbol, timeframe))

        trade_days = [row["trade_date"] for row in cur.fetchall()]

        for trade_date in trade_days:

            if not holiday_agent.is_trading_day(trade_date):
                continue

            expected = self._expected_intraday_candles(trade_date, timeframe)
            actual = self._fetch_intraday_candles(
                conn,
                symbol,
                timeframe,
                expected[0],
                expected[-1] + timedelta(minutes=TIMEFRAME_MINUTES[timeframe])
            )

            missing = sorted(set(expected) - set(actual))
            completeness_status = "PASS" if not missing else "FAIL"

            # 1️⃣ DETECTION
            self.persist_report(
                conn,
                symbol,
                timeframe,
                "intraday_completeness",
                completeness_status,
                {
                    "trade_date": trade_date.isoformat(),
                    "expected_count": len(expected),
                    "actual_count": len(actual),
                    "missing_count": len(missing),
                }
            )

            if not missing:
                self._resolve_intraday_alert_if_any(conn, symbol, timeframe)
                continue

            # 2️⃣ HEALING
            backfill_agent = IntradayBackfillAgent()
            result = backfill_agent.backfill_missing_candles(
                symbol=symbol,
                timeframe=timeframe,
                missing_candles=missing,
            )

            self.persist_report(
                conn,
                symbol,
                timeframe,
                "intraday_backfill",
                result["status"],
                result
            )

            # 3️⃣ ESCALATION
            if result["status"] == "PARTIAL":
                partial_runs = self._count_intraday_partial_runs(
                    conn, symbol, timeframe
                )

                if partial_runs >= self.MAX_INTRADAY_PARTIAL_RUNS:
                    self.persist_report(
                        conn,
                        symbol,
                        timeframe,
                        "intraday_backfill_alert",
                        "RAISED",
                        {
                            "consecutive_partial_runs": partial_runs,
                            "threshold": self.MAX_INTRADAY_PARTIAL_RUNS,
                            "trade_date": trade_date.isoformat(),
                            "message": "Intraday auto-backfill repeatedly failing"
                        }
                    )

    def _count_intraday_partial_runs(self, conn, symbol: str, timeframe: str) -> int:
        cur = conn.cursor()
        cur.execute("""
            SELECT status
            FROM data_quality_reports
            WHERE symbol = %s
              AND timeframe = %s
              AND check_type = 'intraday_backfill'
            ORDER BY run_ts DESC
            LIMIT %s
        """, (symbol, timeframe, self.MAX_INTRADAY_PARTIAL_RUNS + 2))

        count = 0
        for row in cur.fetchall():
            if row["status"] == "PARTIAL":
                count += 1
            else:
                break
        return count

    def _resolve_intraday_alert_if_any(self, conn, symbol: str, timeframe: str):
        cur = conn.cursor()
        cur.execute("""
            UPDATE data_quality_reports
            SET status = 'RESOLVED'
            WHERE symbol = %s
              AND timeframe = %s
              AND check_type = 'intraday_backfill_alert'
              AND status IN ('RAISED', 'ACKED')
        """, (symbol, timeframe))
        conn.commit()

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------
    def _expected_intraday_candles(self, trade_date: date, timeframe: str):
        step = timedelta(minutes=TIMEFRAME_MINUTES[timeframe])

        start = IST.localize(datetime.combine(trade_date, MARKET_OPEN))
        end = IST.localize(datetime.combine(trade_date, MARKET_CLOSE))

        candles = []
        current = start
        while current < end:
            candles.append(current)
            current += step

        return candles

    def _fetch_intraday_candles(
        self, conn, symbol, timeframe, start_ts, end_ts
    ):
        cur = conn.cursor()
        cur.execute("""
            SELECT ts
            FROM candles
            WHERE symbol = %s
              AND timeframe = %s
              AND ts BETWEEN %s AND %s
            ORDER BY ts
        """, (
            symbol,
            timeframe,
            start_ts.astimezone(timezone.utc),
            end_ts.astimezone(timezone.utc),
        ))

        return [row["ts"].astimezone(IST) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # PERSISTENCE
    # ------------------------------------------------------------------
    def persist_report(
        self,
        conn,
        symbol: str,
        timeframe: str,
        check_type: str,
        status: str,
        details: Dict[str, Any]
    ):
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO data_quality_reports (
                run_ts,
                symbol,
                timeframe,
                check_type,
                status,
                details
            )
            VALUES (NOW(), %s, %s, %s, %s, %s)
        """, (
            symbol,
            timeframe,
            check_type,
            status,
            Json(details)
        ))
        conn.commit()
