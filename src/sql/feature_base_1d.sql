WITH returns AS (
    SELECT
        symbol,
        ts,
        open::DOUBLE PRECISION   AS open,
        high::DOUBLE PRECISION   AS high,
        low::DOUBLE PRECISION    AS low,
        close::DOUBLE PRECISION  AS close,
        volume::BIGINT           AS volume,

        -- log returns
        LN(close / LAG(close, 1) OVER w)  AS ret_15m,
        LN(close / LAG(close, 4) OVER w)  AS ret_1h,
        LN(close / LAG(close, 26) OVER w) AS ret_1d

    FROM candles
    WHERE timeframe = '1D'

    WINDOW w AS (PARTITION BY symbol ORDER BY ts)
),

base AS (
    SELECT
        symbol,
        ts,
        open,
        high,
        low,
        close,
        volume,

        ret_15m,
        ret_1h,
        ret_1d,

        -- range
        (high - low) / NULLIF(close, 0) AS range_15m,

        -- volatility
        STDDEV(ret_15m) OVER w30 AS vol_30m,
        STDDEV(ret_15m) OVER w1d AS vol_1d,

        -- volume stats
        AVG(volume) OVER w20    AS vol_mean,
        STDDEV(volume) OVER w20 AS vol_std,

        -- VWAP
        SUM(close * volume) OVER w20 /
        NULLIF(SUM(volume) OVER w20, 0) AS vwap_20

    FROM returns

    WINDOW
        w20 AS (PARTITION BY symbol ORDER BY ts ROWS 19 PRECEDING),
        w30 AS (PARTITION BY symbol ORDER BY ts ROWS 29 PRECEDING),
        w1d AS (PARTITION BY symbol ORDER BY ts ROWS 95 PRECEDING)
)

SELECT
    symbol,
    ts,
    close::DOUBLE PRECISION        AS close,   -- ✅ FORCE numeric

    ret_15m::DOUBLE PRECISION,
    ret_1h::DOUBLE PRECISION,
    ret_1d::DOUBLE PRECISION,
    range_15m::DOUBLE PRECISION,

    vol_30m::DOUBLE PRECISION,
    vol_1d::DOUBLE PRECISION,

    -- volume z-score
    CASE
        WHEN vol_std > 0
        THEN ((volume - vol_mean) / vol_std)::DOUBLE PRECISION
        ELSE 0.0
    END AS vol_zscore,

    -- VWAP distance
    ((close - vwap_20) / NULLIF(vwap_20, 0))::DOUBLE PRECISION AS vwap_dist,

    -- close position
    ((close - low) / NULLIF(high - low, 0))::DOUBLE PRECISION AS close_pos

FROM base
WHERE
    ret_15m IS NOT NULL
    AND vol_30m IS NOT NULL;
