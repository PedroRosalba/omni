-- Opt-in instance scoping + ownership for durable event consumers.
--
-- Durable consumers are global: they page every instance's journal rows and
-- any key with events:write can pull, ack or delete one by name, regardless
-- of the key's instance restriction. This adds an OPT-IN scoped mode
-- (POST /events/consumers with scoped: true); existing rows and every create
-- without the option keep today's behaviour exactly (scoped defaults false,
-- the other two columns stay NULL).
--
--   durable_consumers.scoped         default false (global, unchanged)
--   durable_consumers.instance_ids   jsonb array of instance UUIDs, NULL unless scoped
--   durable_consumers.owner_key_id   uuid of the registering api_keys row, NULL unless scoped
--
-- owner_key_id carries no foreign key on purpose: Postgres has no
-- ADD CONSTRAINT IF NOT EXISTS, and a revoked or deleted key must not
-- cascade into (or block deleting) the consumer row; an orphaned scoped
-- consumer stays reachable by instance-unrestricted keys.
--
-- Numbering: open PRs #1294, #1295, #1299 and #1300 also add a 0080
-- migration. Agreed merge order: #1294, #1295, #1298 (this one), #1299; each
-- later PR renumbers its OWN migration when it merges (rename, journal
-- idx/tag, `when` above the previous entry) per the parallel-worktree rule in
-- .claude/CLAUDE.md.
--
-- Hand-written following the 0043/0044/0052 precedent (additive, idempotent).
-- No explicit BEGIN/COMMIT — the boot migrator executes this file on a pooled
-- postgres-js connection, which rejects raw transaction control.

ALTER TABLE "durable_consumers"
  ADD COLUMN IF NOT EXISTS "scoped" boolean NOT NULL DEFAULT false;
--> statement-breakpoint
ALTER TABLE "durable_consumers"
  ADD COLUMN IF NOT EXISTS "instance_ids" jsonb;
--> statement-breakpoint
ALTER TABLE "durable_consumers"
  ADD COLUMN IF NOT EXISTS "owner_key_id" uuid;
