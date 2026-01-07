-- sql/21_rollup_policies.sql
-- Add cagg refresh policies safely (ignore "already exists")

DO $$
BEGIN
  PERFORM add_continuous_aggregate_policy('ohlcv_5m',
    start_offset => INTERVAL '30 days',
    end_offset => INTERVAL '10 minutes',
    schedule_interval => INTERVAL '5 minutes'
  );
EXCEPTION
  WHEN others THEN
    -- Timescale throws a specific error message; we treat it as idempotent.
    IF SQLERRM LIKE '%refresh policy already exists%' THEN
      NULL;
    ELSE
      RAISE;
    END IF;
END $$;

DO $$
BEGIN
  PERFORM add_continuous_aggregate_policy('ohlcv_30m',
    start_offset => INTERVAL '90 days',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '10 minutes'
  );
EXCEPTION WHEN others THEN
  IF SQLERRM LIKE '%refresh policy already exists%' THEN NULL; ELSE RAISE; END IF;
END $$;

DO $$
BEGIN
  PERFORM add_continuous_aggregate_policy('ohlcv_1h',
    start_offset => INTERVAL '180 days',
    end_offset => INTERVAL '2 hours',
    schedule_interval => INTERVAL '15 minutes'
  );
EXCEPTION WHEN others THEN
  IF SQLERRM LIKE '%refresh policy already exists%' THEN NULL; ELSE RAISE; END IF;
END $$;

DO $$
BEGIN
  PERFORM add_continuous_aggregate_policy('ohlcv_4h',
    start_offset => INTERVAL '365 days',
    end_offset => INTERVAL '6 hours',
    schedule_interval => INTERVAL '1 hour'
  );
EXCEPTION WHEN others THEN
  IF SQLERRM LIKE '%refresh policy already exists%' THEN NULL; ELSE RAISE; END IF;
END $$;

DO $$
BEGIN
  PERFORM add_continuous_aggregate_policy('ohlcv_1d',
    start_offset => INTERVAL '10 years',
    end_offset => INTERVAL '2 days',
    schedule_interval => INTERVAL '6 hours'
  );
EXCEPTION WHEN others THEN
  IF SQLERRM LIKE '%refresh policy already exists%' THEN NULL; ELSE RAISE; END IF;
END $$;

DO $$
BEGIN
  PERFORM add_continuous_aggregate_policy('ohlcv_1w',
    start_offset => INTERVAL '20 years',
    end_offset => INTERVAL '2 weeks',
    schedule_interval => INTERVAL '1 day'
  );
EXCEPTION WHEN others THEN
  IF SQLERRM LIKE '%refresh policy already exists%' THEN NULL; ELSE RAISE; END IF;
END $$;
