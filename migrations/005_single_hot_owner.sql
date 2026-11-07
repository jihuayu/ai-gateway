-- A PG schema owns one financial Redis namespace. Renaming it must never
-- bootstrap a second budget from a potentially lagging asynchronous ledger.
CREATE UNIQUE INDEX hot_runtime_single_owner ON hot_runtime_manifests ((true));
