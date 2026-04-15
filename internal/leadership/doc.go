// Package leadership implements leader election for River clients sharing a
// database schema.
//
// The database records at most one current leadership term at a time. The
// elected client runs distributed maintenance work such as queue management,
// job scheduling, and reindexing that should not be duplicated across clients.
//
// # Overview
//
// Leadership is modeled as a database-backed lease with an explicit term
// identity.
//
// A term is identified by:
//   - `leader_id`: the stable client identity
//   - `elected_at`: the database-issued timestamp for that specific term
//
// The database is authoritative for:
//   - which client currently holds the leadership row
//   - whether a term can be renewed
//   - whether a term has already been replaced
//
// The process uses local time only to bound how long it trusts its last
// successful elect or reelect result. If it cannot renew a term in time, it
// steps down conservatively instead of continuing to act as leader on stale
// information.
//
// # State Model
//
// At a high level, an elector alternates between follower and leader states:
//
//	Start
//	  │
//	  ▼
//	┌─────────────────────────────────────┐
//	│              Follower               │
//	│                                     │
//	│ Retries election on timer or wakeup │
//	└──────────────────┬──────────────────┘
//	                   │ won election
//	                   ▼
//	┌─────────────────────────────────────┐
//	│               Leader                │
//	│                                     │
//	│ Renews before trust window expires  │
//	└──────────────────┬──────────────────┘
//	                   │ replaced / expired / resign requested /
//	                   │ renewal failed for too long / shutdown
//	                   ▼
//	               Follower
//
// Followers attempt election periodically and can wake early when they learn
// that the previous leader resigned. Leaders renew their current term
// periodically. If renewal fails, the term is replaced, or the local trust
// window expires, the process stops acting as leader and returns to follower
// behavior.
//
// # Trust Window
//
// After each successful election or renewal, the elector computes a local
// trust deadline:
//
//	trustedUntil = attemptStarted + TTL - safetyMargin
//
// This trust window has two important properties:
//   - it is anchored to when the elect or reelect attempt started, so a slow
//     successful database round trip cannot stretch leadership longer than the
//     attempt budget allows
//   - it ends before the database lease should expire, giving the process time
//     to step down before it risks acting on a stale term
//
// The local trust window is a conservative stop condition, not an alternative
// source of truth. A client may step down while the database row is still
// present, but it should not continue acting as leader after it no longer
// trusts its last successful renewal.
//
// # Term-Scoped Operations
//
// Renewing and resigning are scoped to the exact term identified by
// `(leader_id, elected_at)`.
//
// That means:
//   - an old term cannot accidentally renew a newer term for the same client
//   - a delayed resign from an old term cannot delete a newer term for the
//     same client
//   - when the database says a term is gone, the elector can step down without
//     ambiguity about which term it held
//
// # Notifications and Subscribers
//
// When a notifier is available, the elector listens for leadership-related
// events so followers can wake promptly and leaders can honor explicit
// resignation requests.
//
// Notification delivery is intentionally non-blocking:
//   - wakeups may coalesce, because multiple rapid resignations only need to
//     prompt another election attempt
//   - polling remains the fallback when notifications are unavailable or missed
//
// Consumers inside the process can subscribe to leadership transitions. Those
// subscriptions preserve ordered `true`/`false` transitions so downstream
// maintenance components can reliably start and stop work, while still keeping
// slow subscribers from blocking the elector itself.
//
// # Failure Handling
//
// The system is intentionally conservative under failures:
//   - if renewal errors persist until the trust window is exhausted, the leader
//     steps down
//   - if the database reports that the current term no longer exists, the
//     leader steps down immediately
//   - if resignation fails during shutdown or after a local timeout, the
//     database lease expiry remains the safety net that eventually allows a new
//     election
//
// This design keeps leadership decisions centered on the database while using
// local time only to stop trusting stale state sooner rather than later.
package leadership
