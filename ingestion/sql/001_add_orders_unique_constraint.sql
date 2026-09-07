DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_raw_orders_order_id'
          AND conrelid = 'raw.orders'::regclass
    ) THEN
        ALTER TABLE raw.orders
        ADD CONSTRAINT uq_raw_orders_order_id
        UNIQUE (order_id);
    END IF;
END
$$;