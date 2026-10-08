# frozen_string_literal: true

# Like Go's DBPoolWithYugabyteVersion, this exercises product/setting detection
# on Postgres, not Yugabyte's storage or transaction semantics. The caller must
# own an isolated schema and put it ahead of pg_catalog in every connection's
# search_path. An unavailable pg_notify deliberately raises instead of no-oping.
module YugabyteTestDatabase
  def self.simulate(driver, notifications: nil, product: "PostgreSQL 15.12-YB-2025.2.3.0-b1")
    setting = if notifications.nil?
      "NULL::text"
    else
      notifications ? "'on'::text" : "'off'::text"
    end
    driver.send(:runtime_execute, <<~SQL)
      CREATE FUNCTION version() RETURNS text LANGUAGE sql AS $$
        SELECT #{driver.send(:runtime_quote, product)}::text
      $$;
      CREATE FUNCTION current_setting(setting_name text, missing_ok boolean) RETURNS text LANGUAGE sql AS $$
        SELECT CASE WHEN setting_name = 'yb_enable_listen_notify' THEN #{setting}
        ELSE pg_catalog.current_setting(setting_name, missing_ok) END
      $$;
    SQL
    unless notifications
      driver.send(:runtime_execute, <<~SQL)
        CREATE FUNCTION pg_notify(text, text) RETURNS void LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'LISTEN/NOTIFY is unavailable'; END
        $$;
      SQL
    end
  end
end
