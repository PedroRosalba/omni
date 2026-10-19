-- Opt-in ingest access verdict on the journal + access-filtered durable consumers.
--
-- When OMNI_JOURNAL_ACCESS_DECISION=true, event persistence records the
-- instance's access verdict for inbound (not-from-me) message.received and
-- reaction.received/removed rows, so a durable consumer can follow only the
-- senders the instance allows (or only the denied ones) without re-running
-- the access check per row. With the flag unset nothing writes the column and
-- every existing consumer keeps the default 'all' filter: behaviour is
-- unchanged for everyone already using the journal.
--
--   omni_events.access_decision      varchar(10), nullable, no default:
--                                    'disabled' | 'allowed' | 'denied', frozen
--                                    at ingest; NULL when not evaluated
--   durable_consumers.access_filter  varchar(10) NOT NULL DEFAULT 'all':
--                                    'all' | 'allowed' | 'denied'
--
-- Numbering collision: open PRs #1294 and #1295 also claim 0080. Per the
-- parallel-worktree rule, whichever PR merges later renumbers its OWN
-- migration to the next free number (file name, journal idx/tag, and a
-- `when` above the new previous entry); the one that landed is never touched.
--
-- Hand-written following the 0043/0044/0052 precedent (additive, idempotent).
-- No explicit BEGIN/COMMIT — the boot migrator executes this file on a pooled
-- postgres-js connection, which rejects raw transaction control.

ALTER TABLE "omni_events"
  ADD COLUMN IF NOT EXISTS "access_decision" varchar(10);
--> statement-breakpoint
ALTER TABLE "durable_consumers"
  ADD COLUMN IF NOT EXISTS "access_filter" varchar(10) NOT NULL DEFAULT 'all';
