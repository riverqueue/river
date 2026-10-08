# frozen_string_literal: true

require "spec_helper"
require "timeout"

RSpec.describe River::Driver::PostgresCapabilities do
  [
    ["PostgreSQL 15.12", 150_012, false, true, :xmax, "(xmax != 0)"],
    ["PostgreSQL 18.0", 180_000, false, true, :returning_old, "(OLD.id IS NOT NULL)"],
    ["PostgreSQL 19beta1", 190_000, false, true, :returning_old, "(OLD.id IS NOT NULL)"],
    ["PostgreSQL 15.12-YB-2025.2.1.0-b1", 150_012, false, false, :metadata_nonce, "false"],
    ["PostgreSQL 15.12-YB-2025.2.3.0-b1", 150_012, true, true, :metadata_nonce, "false"],
    ["YugabyteDB", 180_000, false, false, :metadata_nonce, "false"]
  ].each do |product, version, notify, expected_notify, mode, sql|
    it "detects #{product} with notifications #{notify}" do
      result = described_class.new(product: product, version_num: version, yb_listen_notify_enabled: notify)
      expect(result).to have_attributes(supports_listen_notify: expected_notify, unique_insert_mode: mode, unique_insert_sql: sql)
      expect(Ractor.shareable?(result)).to be true
    end
  end
end

RSpec.describe River::Driver::PostgresCapabilities::Cache do
  let(:cache) { described_class.new }
  let(:detected) { River::Driver::PostgresCapabilities.new(product: "PostgreSQL 18", version_num: 180_000, yb_listen_notify_enabled: false) }

  it "caches successful detection separately for each connection pool" do
    first, second = Object.new, Object.new
    expect(cache.fetch(first) { detected }).to equal(detected)
    expect(cache.fetch(first) { raise "already detected" }).to equal(detected)
    expect { cache.fetch(second) { raise "different pool" } }.to raise_error("different pool")
  end

  it "does not cache failed detection" do
    expect { cache.fetch(:pool) { raise "query failed" } }.to raise_error("query failed")
    expect(cache.fetch(:pool) { detected }).to equal(detected)
  end

  it "does not hold its lock across I/O and keeps the first successful detection" do
    entered, release = Queue.new, Queue.new
    thread = Thread.new do
      cache.fetch(:pool) do
        entered << true
        release.pop
        River::Driver::PostgresCapabilities.new(product: "YugabyteDB", version_num: 150_012, yb_listen_notify_enabled: false)
      end
    end
    Timeout.timeout(5) { entered.pop }
    expect(Timeout.timeout(5) { cache.fetch(:pool) { detected } }).to equal(detected)
    release << true
    expect(Timeout.timeout(5) { thread.value }).to equal(detected)
  ensure
    release << true
    thread&.join
  end
end
