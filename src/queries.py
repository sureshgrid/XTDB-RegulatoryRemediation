
# These queries demonstrate several important concepts in market manipulation detection:

# Temporal Predicates:

# IMMEDIATELY SUCCEEDS for finding sequential patterns
# OVERLAPS for finding concurrent activities
# Time window analysis using BETWEEN
# Valid time vs System time considerations


# Pattern-Specific Features:

# Layering: Order sequence analysis and cancellation timing
# Wash Trading: Related party detection and price/quantity matching
# Spoofing: Order size analysis and price impact calculation
# Momentum Ignition: Price movement and volume analysis


# Market Microstructure Analysis:

# Price impact calculation
# Volume profile analysis
# Trade frequency monitoring
# Order book pressure measurement


# Risk Factors:

# Counterparty relationships
# Historical behavior patterns
# Account types and risk ratings
# Trade size relative to normal activity

# TO DO
# How to create visualization queries that help analysts understand 
# these patterns more intuitively.

# queries.py
MANIPULATION_DETECTION_QUERIES = r"""
-- Layering Pattern Detection
-- This query identifies potential layering by looking for:
-- 1. Multiple small orders on one side
-- 2. Followed by a larger order on the opposite side
-- 3. Followed by rapid cancellations

WITH layering_sequence AS (
    -- First identify clusters of same-side orders
    SELECT 
        t1._id as sequence_id,
        t1.symbol,
        t1.side as layer_side,
        t1.counterparty_id,
        t1._valid_from as sequence_start,
        t1._valid_to as sequence_end,
        COUNT(*) OVER (
            PARTITION BY t1.symbol, t1.counterparty_id
            ORDER BY t1._valid_from
            RANGE BETWEEN 
                INTERVAL '2 minutes' PRECEDING AND 
                CURRENT ROW
        ) as num_orders_in_sequence
    FROM trades FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP t1
    WHERE t1.status = 'executed'
),
potential_layers AS (
    -- Find sequences with multiple orders
    SELECT 
        ls.*,
        t2._id as opposing_trade_id,
        t2.quantity as opposing_quantity,
        t2._valid_from as opposing_trade_time
    FROM layering_sequence ls
    -- Look for larger opposing trades that IMMEDIATELY SUCCEED the layer sequence
    JOIN trades FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP t2 
        ON t2.symbol = ls.symbol
        AND t2.counterparty_id = ls.counterparty_id
        AND t2.side != ls.layer_side
        AND t2._valid_from > ls.sequence_start
        AND t2._valid_from < ls.sequence_end + INTERVAL '5 minutes'
    WHERE ls.num_orders_in_sequence >= 4  -- Suspicious number of layers
)
SELECT 
    pl.*,
    c.risk_rating,
    c.account_type,
    -- Calculate price pressure created by layers
    AVG(t.price) OVER (
        PARTITION BY pl.sequence_id 
        ORDER BY t._valid_from
    ) as avg_layer_price
FROM potential_layers pl
JOIN trades FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP t 
    ON t.symbol = pl.symbol 
    AND t._valid_from BETWEEN pl.sequence_start AND pl.sequence_end
JOIN counterparties FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP c 
    ON pl.counterparty_id = c._id
WHERE EXISTS (
    -- Verify rapid cancellations followed
    SELECT 1 
    FROM trades t_cancel 
    WHERE t_cancel.status = 'cancelled'
    AND t_cancel.symbol = pl.symbol
    AND t_cancel.counterparty_id = pl.counterparty_id
    AND t_cancel._valid_from BETWEEN 
        pl.opposing_trade_time AND 
        pl.opposing_trade_time + INTERVAL '1 minute'
);

-- Wash Trading Detection
-- Identifies potential wash trading by looking for:
-- 1. Offsetting trades between related parties
-- 2. Trades that happen within a short time window
-- 3. Similar prices and quantities

WITH related_trades AS (
    SELECT 
        t1._id as buy_trade_id,
        t2._id as sell_trade_id,
        t1.symbol,
        t1.quantity as buy_quantity,
        t2.quantity as sell_quantity,
        t1.price as buy_price,
        t2.price as sell_price,
        t1._valid_from as buy_time,
        t2._valid_from as sell_time,
        t1.counterparty_id as buyer_id,
        t2.counterparty_id as seller_id,
        ABS(t1.price - t2.price) / t1.price as price_diff_pct
    FROM trades FOR VALID_TIME AS OF CURRENT_TIMESTAMP t1
    JOIN trades FOR VALID_TIME AS OF CURRENT_TIMESTAMP t2 
        ON t1.symbol = t2.symbol
        AND t1.side = 'B' 
        AND t2.side = 'S'
        -- Ensure trades are temporally close
        AND t2._valid_from > t1._valid_from
        AND t2._valid_from < t1._valid_from + INTERVAL '5 minutes'
    WHERE t1.status = 'executed' 
    AND t2.status = 'executed'
)
SELECT 
    rt.*,
    c1.beneficial_owner_id as buyer_owner,
    c2.beneficial_owner_id as seller_owner,
    c1.risk_rating as buyer_risk,
    c2.risk_rating as seller_risk
FROM related_trades rt
JOIN counterparties FOR VALID_TIME AS OF CURRENT_TIMESTAMP c1 
    ON rt.buyer_id = c1._id
JOIN counterparties FOR VALID_TIME AS OF CURRENT_TIMESTAMP c2 
    ON rt.seller_id = c2._id
WHERE (
    -- Same beneficial owner
    c1.beneficial_owner_id = c2.beneficial_owner_id
    -- or related parties
    OR EXISTS (
        SELECT 1 
        FROM counterparty_relationships cr
        WHERE (cr.party_a_id = c1._id AND cr.party_b_id = c2._id)
        OR (cr.party_b_id = c1._id AND cr.party_a_id = c2._id)
    )
)
AND rt.price_diff_pct < 0.001  -- Very similar prices
AND ABS(rt.buy_quantity - rt.sell_quantity) < 10;  -- Similar quantities

-- Spoofing Pattern Detection
-- Looks for:
-- 1. Large orders that are quickly cancelled
-- 2. Smaller executions on the opposite side
-- 3. Price impact analysis

WITH large_orders AS (
    SELECT 
        t._id,
        t.symbol,
        t.side,
        t.quantity,
        t.price,
        t.counterparty_id,
        t._valid_from as order_time,
        t._valid_to as cancel_time,
        -- Calculate average order size for the symbol
        AVG(t.quantity) OVER (
            PARTITION BY t.symbol
            ORDER BY t._valid_from 
            RANGE BETWEEN 
                INTERVAL '1 hour' PRECEDING AND 
                CURRENT ROW
        ) as avg_order_size,
        -- Track price movement
        FIRST_VALUE(t.price) OVER (
            PARTITION BY t.symbol 
            ORDER BY t._valid_from
        ) as initial_price,
        LAST_VALUE(t.price) OVER (
            PARTITION BY t.symbol 
            ORDER BY t._valid_from
        ) as final_price
    FROM trades FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP t
    WHERE t.status IN ('pending', 'cancelled')
)
SELECT 
    lo.*,
    c.risk_rating,
    c.account_type,
    -- Count opposite side executions during spoof
    COUNT(t_exec.id) as num_opposite_executions,
    SUM(t_exec.quantity) as total_opposite_quantity,
    -- Calculate price impact
    (lo.final_price - lo.initial_price) / lo.initial_price as price_impact
FROM large_orders lo
JOIN counterparties FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP c 
    ON lo.counterparty_id = c._id
LEFT JOIN trades FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP t_exec
    ON t_exec.symbol = lo.symbol
    AND t_exec.counterparty_id = lo.counterparty_id
    AND t_exec.side != lo.side
    AND t_exec.status = 'executed'
    AND t_exec._valid_from BETWEEN 
        lo.order_time AND 
        COALESCE(lo.cancel_time, CURRENT_TIMESTAMP)
WHERE 
    lo.quantity > 5 * lo.avg_order_size  -- Significantly larger than average
    AND (
        -- Order was cancelled
        lo.cancel_time IS NOT NULL
        -- Cancelled quickly
        AND (lo.cancel_time - lo.order_time) < INTERVAL '5 minutes'
    )
GROUP BY lo.id, lo.symbol, lo.side, lo.quantity, lo.price,
         lo.counterparty_id, lo.order_time, lo.cancel_time,
         lo.avg_order_size, lo.initial_price, lo.final_price,
         c.risk_rating, c.account_type
HAVING COUNT(t_exec.id) > 0;  -- Only include if opposite executions exist

-- Momentum Ignition Detection
-- Identifies potential momentum ignition by looking for:
-- 1. Rapid price movements caused by aggressive orders
-- 2. Increased market participation
-- 3. Subsequent profit taking in opposite direction

WITH price_movements AS (
    SELECT 
        symbol,
        _valid_from as time,
        price,
        -- Calculate price changes
        LAG(price) OVER (
            PARTITION BY symbol 
            ORDER BY _valid_from
        ) as prev_price,
        -- Calculate volume profile
        SUM(quantity) OVER (
            PARTITION BY symbol 
            ORDER BY _valid_from
            RANGE BETWEEN 
                INTERVAL '5 minutes' PRECEDING AND 
                CURRENT ROW
        ) as rolling_volume,
        -- Track trade frequency
        COUNT(*) OVER (
            PARTITION BY symbol 
            ORDER BY _valid_from
            RANGE BETWEEN 
                INTERVAL '5 minutes' PRECEDING AND 
                CURRENT ROW
        ) as trade_frequency
    FROM trades FOR VALID_TIME AS OF CURRENT_TIMESTAMP
    WHERE status = 'executed'
),
momentum_periods AS (
    SELECT 
        symbol,
        time,
        price,
        (price - prev_price) / prev_price as price_change_pct,
        rolling_volume,
        trade_frequency,
        -- Flag periods of unusual activity
        CASE 
            WHEN ABS(price_change_pct) > 0.02  -- Sharp price movement
            AND trade_frequency > 
                2 * AVG(trade_frequency) OVER (
                    PARTITION BY symbol
                )  -- Increased activity
            THEN 1 
            ELSE 0 
        END as momentum_flag
    FROM price_movements
    WHERE prev_price IS NOT NULL
)
SELECT 
    t.counterparty_id,
    c.account_type,
    c.risk_rating,
    mp.symbol,
    mp.time as momentum_start,
    mp.price_change_pct,
    mp.rolling_volume,
    -- Look for profit taking
    SUM(CASE 
        WHEN t.side != LAG(t.side) OVER (
            PARTITION BY t.counterparty_id, t.symbol 
            ORDER BY t._valid_from
        ) THEN t.quantity 
        ELSE 0 
    END) as reversal_quantity
FROM momentum_periods mp
JOIN trades FOR VALID_TIME AS OF CURRENT_TIMESTAMP t 
    ON t.symbol = mp.symbol
    AND t._valid_from BETWEEN 
        mp.time AND 
        mp.time + INTERVAL '10 minutes'
JOIN counterparties FOR VALID_TIME AS OF CURRENT_TIMESTAMP c 
    ON t.counterparty_id = c._id
WHERE mp.momentum_flag = 1
GROUP BY t.counterparty_id, c.account_type, c.risk_rating,
         mp.symbol, mp.time, mp.price_change_pct, mp.rolling_volume
HAVING COUNT(*) > 5;  -- Multiple trades during momentum period
"""