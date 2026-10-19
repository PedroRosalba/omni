-- Per-instance opt-in for typed inbound reply context (PR #1297).
--
-- The WhatsApp (Baileys) plugin can add a typed `replyTo` to message.received
-- and a `target` ({ fromMe, participant? }) to reaction.received and
-- reaction.removed. Existing consumers must see exactly today's events, so the
-- fields are emitted only when the instance opts in:
--
--   instances.inbound_reply_context  boolean NOT NULL DEFAULT false
--
-- false (the default for every existing and new row) keeps every event
-- byte-identical to the previous release. The plugin reads it at connect.
--
-- Numbering: #1294, #1295, #1298, #1299 and #1300 also claim 0080; merge order
-- decides which PR renumbers (whoever merges second renumbers its own file).
--
-- Hand-written following the 0043/0044/0052 precedent (additive, idempotent).
-- No explicit BEGIN/COMMIT — the boot migrator executes this file on a pooled
-- postgres-js connection, which rejects raw transaction control.

ALTER TABLE "instances" ADD COLUMN IF NOT EXISTS "inbound_reply_context" boolean NOT NULL DEFAULT false;
