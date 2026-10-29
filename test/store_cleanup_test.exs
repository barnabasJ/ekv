defmodule EKV.StoreCleanupTest do
  use ExUnit.Case, async: false

  test "maintenance purges expired CAS values while retaining live values and ballot promises" do
    id = System.unique_integer([:positive])
    name = :"cleanup_#{id}"
    directory = Path.join(System.tmp_dir!(), "ekv-cleanup-#{id}")
    on_exit(fn -> File.rm_rf!(directory) end)

    start_supervised!(
      {EKV,
       name: name,
       data_dir: directory,
       shards: 1,
       cluster_size: 1,
       node_id: "cleanup",
       log: false,
       gc_interval: :timer.hours(1)}
    )

    assert {:ok, _} = EKV.put(name, "expired/cas", "old", if_vsn: nil, ttl: 1)
    assert :ok = EKV.put(name, "expired/lww", "old", ttl: 1)
    assert {:ok, _} = EKV.put(name, "live/cas", "current", if_vsn: nil, ttl: 60_000)
    assert {:ok, _} = EKV.put(name, "live/permanent", "current", if_vsn: nil)

    now = System.system_time(:nanosecond) + 1_000_000_000
    shard = EKV.Replica.shard_name(name, 0)
    send(shard, {:gc, now, now - 500_000_000})
    state = :sys.get_state(shard)

    assert EKV.get(name, "expired/cas") == nil
    assert EKV.get(name, "expired/lww") == nil
    assert EKV.get(name, "live/cas") == "current"
    assert EKV.get(name, "live/permanent") == "current"

    assert {:ok, []} =
             EKV.Sqlite3.fetch_all(state.db, "SELECT key FROM kv WHERE key = 'expired/cas'", [])

    assert {:ok, [[^now]]} =
             EKV.Sqlite3.fetch_all(
               state.db,
               "SELECT deleted_at FROM kv WHERE key = 'expired/lww'",
               []
             )

    assert {:ok, [[promised]]} =
             EKV.Sqlite3.fetch_all(
               state.db,
               "SELECT promised_counter FROM kv_paxos WHERE key = 'expired/cas'",
               []
             )

    assert promised > 0
  end

  test "opening an existing store reconciles unpinned legacy references and preserves replay" do
    directory =
      Path.join(System.tmp_dir!(), "ekv-legacy-keyrefs-#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf!(directory) end)
    {:ok, db} = EKV.Store.open(directory, 0, :timer.hours(24 * 7), 1, :timer.hours(1))

    :ok =
      EKV.Sqlite3.execute(
        db,
        "INSERT INTO kv_keyrefs(key,oplog_refs) VALUES('legacy-unpinned',0),('legacy-pinned',0)"
      )

    :ok =
      EKV.Sqlite3.execute(db, """
      INSERT INTO kv_oplog(key_id,value,timestamp,origin_node,origin_seq)
      SELECT id,NULL,1,'legacy',1 FROM kv_keyrefs WHERE key='legacy-pinned'
      """)

    :ok = EKV.Sqlite3.close(db)

    {:ok, reopened} = EKV.Store.open(directory, 0, :timer.hours(24 * 7), 1, :timer.hours(1))

    try do
      assert {:ok, [["legacy-pinned", 1]]} =
               EKV.Sqlite3.fetch_all(
                 reopened,
                 "SELECT key,oplog_refs FROM kv_keyrefs ORDER BY key",
                 []
               )

      assert {:ok, [["ok"]]} = EKV.Sqlite3.fetch_all(reopened, "PRAGMA integrity_check", [])
    after
      :ok = EKV.Sqlite3.close(reopened)
    end
  end
end
