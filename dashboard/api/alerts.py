from fastapi import APIRouter, Depends, Query
from datetime import datetime

from dashboard.db import get_db

router = APIRouter(prefix="/alerts", tags=["Alerts"])


# ─────────────────────────────────────────────
# ACTIVE ALERTS
# ─────────────────────────────────────────────
@router.get("/active")
def get_active_alerts(conn=Depends(get_db)):
    """
    Return currently active alerts (RAISED or ACKED).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                run_ts,
                symbol,
                status,
                details
            FROM data_quality_reports
            WHERE check_type = 'auto_backfill_alert'
              AND status IN ('RAISED', 'ACKED')
            ORDER BY run_ts DESC
            """
        )
        return cur.fetchall()


# ─────────────────────────────────────────────
# ALERT HISTORY
# ─────────────────────────────────────────────
@router.get("/history")
def get_alert_history(
    limit: int = Query(100, ge=1, le=1000),
    conn=Depends(get_db),
):
    """
    Return historical alert lifecycle events.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                run_ts,
                symbol,
                status,
                details
            FROM data_quality_reports
            WHERE check_type = 'auto_backfill_alert'
            ORDER BY run_ts DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


# ─────────────────────────────────────────────
# ACKNOWLEDGE ALERT
# ─────────────────────────────────────────────
@router.post("/{alert_id}/ack")
def acknowledge_alert(
    alert_id: int,
    user: str = Query("unknown"),
    conn=Depends(get_db),
):
    """
    Acknowledge an alert (human ownership).
    """
    acknowledged_at = datetime.utcnow().isoformat()

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE data_quality_reports
            SET
              status = 'ACKED',
              details = jsonb_set(
                COALESCE(details, '{}'),
                '{acknowledged_by,acknowledged_at}',
                to_jsonb(%s::text || '|' || %s::text)
              )
            WHERE id = %s
              AND status = 'RAISED'
            RETURNING id
            """,
            (user, acknowledged_at, alert_id),
        )

        updated = cur.fetchone()
        conn.commit()

        if not updated:
            return {
                "message": "Alert not found or already acknowledged"
            }

        return {
            "alert_id": alert_id,
            "status": "ACKED",
            "acknowledged_by": user,
            "acknowledged_at": acknowledged_at,
        }


# ─────────────────────────────────────────────
# RESOLVE ALERT
# ─────────────────────────────────────────────
@router.post("/{alert_id}/resolve")
def resolve_alert(
    alert_id: int,
    user: str = Query("unknown"),
    note: str = Query(""),
    conn=Depends(get_db),
):
    """
    Resolve an alert (close incident).
    """
    resolved_at = datetime.utcnow().isoformat()

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE data_quality_reports
            SET
              status = 'RESOLVED',
              details = COALESCE(details, '{}') || jsonb_build_object(
                'resolved_by', %s,
                'resolved_at', %s,
                'resolution_note', %s
              )
            WHERE id = %s
              AND status IN ('RAISED', 'ACKED')
            RETURNING id
            """,
            (user, resolved_at, note, alert_id),
        )

        updated = cur.fetchone()
        conn.commit()

        if not updated:
            return {
                "message": "Alert not found or already resolved"
            }

        return {
            "alert_id": alert_id,
            "status": "RESOLVED",
            "resolved_by": user,
            "resolved_at": resolved_at,
            "note": note,
        }
