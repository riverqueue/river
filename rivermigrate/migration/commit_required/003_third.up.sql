CREATE OR REPLACE FUNCTION /* TEMPLATE: schema */foobar_in_bitmask(bitmask BIT(8), val /* TEMPLATE: schema */foobar)
RETURNS boolean
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE val
        WHEN 'foo' THEN get_bit(bitmask, 7)
        WHEN 'bar' THEN get_bit(bitmask, 6)
        -- Because the enum value 'baz' was added in migration 2 and not part
        -- of the original enum, we can't use it in an immutable SQL function
        -- unless the new enum value migration has been committed.
        WHEN 'baz' THEN get_bit(bitmask, 5)
        ELSE 0
    END = 1;
$$;
