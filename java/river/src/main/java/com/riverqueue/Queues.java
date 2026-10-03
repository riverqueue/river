package com.riverqueue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import tools.jackson.databind.JsonNode;

/** Persistent queue controls, including transactional pause and resume. */
public final class Queues {
  private final Client river;

  Queues(Client river) {
    this.river = river;
  }

  public Queue get(String name) {
    return river.transaction(c -> get(c, name));
  }

  public Queue get(Connection connection, String name) {
    return river.transaction(
        connection,
        transaction -> {
          try (var statement =
                  Sql.prepare(transaction, Sql.query(river.database(), "queue_get"), name);
              var rows = statement.executeQuery()) {
            if (!rows.next())
              throw new RiverException(RiverException.Code.NOT_FOUND, "Queue not found: " + name);
            return read(rows);
          } catch (SQLException e) {
            throw Client.databaseError("Get queue", e);
          }
        });
  }

  /** Lists up to 100 queues. */
  public List<Queue> list() {
    return list(100);
  }

  /** Lists up to 100 queues using the caller's connection. */
  public List<Queue> list(Connection connection) {
    return list(connection, 100);
  }

  public List<Queue> list(int limit) {
    return river.transaction(c -> list(c, limit));
  }

  public List<Queue> list(Connection connection, int limit) {
    return river.transaction(
        connection,
        transaction -> {
          if (limit < 1 || limit > 10000) throw new IllegalArgumentException("Invalid queue limit");
          try (var statement =
                  Sql.prepare(transaction, Sql.query(river.database(), "queue_list"), limit);
              var rows = statement.executeQuery()) {
            var result = new ArrayList<Queue>();
            while (rows.next()) result.add(read(rows));
            return List.copyOf(result);
          } catch (SQLException e) {
            throw Client.databaseError("List queues", e);
          }
        });
  }

  public void pause(String name) {
    river.transaction(
        c -> {
          pause(c, name);
          return null;
        });
  }

  public void pause(Connection connection, String name) {
    river.transaction(
        connection,
        c -> {
          control(c, name, true);
          return null;
        });
  }

  public void resume(String name) {
    river.transaction(
        c -> {
          resume(c, name);
          return null;
        });
  }

  public void resume(Connection connection, String name) {
    river.transaction(
        connection,
        c -> {
          control(c, name, false);
          return null;
        });
  }

  public Queue update(String name, Object metadata) {
    return river.transaction(c -> update(c, name, metadata));
  }

  public Queue update(Connection connection, String name, Object metadata) {
    return river.transaction(connection, c -> updateWithinTransaction(c, name, metadata));
  }

  private Queue updateWithinTransaction(Connection connection, String name, Object metadata) {
    if (metadata == null) return get(connection, name);
    var value = Json.tree(metadata);
    if (!value.isObject()) throw new IllegalArgumentException("Queue metadata must be an object");
    try (var statement =
            Sql.prepare(
                connection,
                Sql.query(river.database(), "queue_update"),
                Json.encode(metadata),
                river.database().timestamp(Instant.now()),
                name);
        var rows = statement.executeQuery()) {
      if (!rows.next())
        throw new RiverException(RiverException.Code.NOT_FOUND, "Queue not found: " + name);
      var queue = read(rows);
      river.notify(connection, Protocol.queueMetadataChanged(name, metadata));
      return queue;
    } catch (SQLException e) {
      throw Client.databaseError("Update queue", e);
    }
  }

  private void control(Connection connection, String name, boolean pause) {
    var now = river.database().timestamp(Instant.now());
    try (var statement =
        Sql.prepare(
            connection,
            Sql.query(river.database(), pause ? "queue_pause" : "queue_resume"),
            now,
            name,
            name)) {
      if (statement.executeUpdate() == 0 && !name.equals("*"))
        throw new RiverException(RiverException.Code.NOT_FOUND, "Queue not found: " + name);
      river.notify(connection, Protocol.queuePause(name, pause));
    } catch (SQLException e) {
      throw Client.databaseError("Control queue", e);
    }
  }

  private static Queue read(ResultSet rows) throws SQLException {
    return new Queue(
        rows.getString("name"),
        Database.instant(rows.getString("created_at")),
        Json.parse(rows.getString("metadata")),
        Database.instant(rows.getString("paused_at")),
        Database.instant(rows.getString("updated_at")));
  }

  /** A snapshot of a persistent queue. */
  public record Queue(
      String name, Instant createdAt, JsonNode metadata, Instant pausedAt, Instant updatedAt) {
    public Queue {
      metadata = metadata.deepCopy();
    }
  }
}
