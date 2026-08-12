# 2026-08-12 — The per-source read run: the deposit pays out

The read half of the `AS FACT ON SOURCE` ruling, run for real on the
same workspace the morning's onboarding built: a second dataset
(`fin_ap`, the AP lane) declared from the same `erp_export` source,
reading the banked conventions **before its first probe** — the
scenario the proposal fixture (`feedback/flow-source-conventions.md`)
was written for.

## What happened, in order

1. `DECLARE DATASET fin_ap; USE fin_ap;` — no source declaration
   needed: sources live outside datasets, `erp_export` already
   stands.
2. `SELECT value FROM GLOSSARY(erp_export) WHERE aspect =
   'conventions'` — **the seven conventions deposited during `fin`'s
   onboarding served immediately from the new dataset**: ISO dates,
   single-currency USD, the sign identity, leaf-only postings, the
   trial-balance naming wart, the document flow, empty-cell nulls.
3. **One probe instead of six.** The deposit covered every typing
   question except one new shape (`bank_transactions.reconciled`, a
   boolean) — that got the run's single probe. Three recipes then
   landed typed straight from the conventions: `ap_invoices`
   (16,817), `ap_payments` (14,928), `bank_transactions` (26,655) —
   all casts clean, zero format-discovery probes.
4. **Workspace-wide supersession, proven both directions**: the
   conventions re-spoken from `fin_ap` (adding the measured boolean
   convention) superseded the `fin`-era slot, and the `fin` channel
   read the updated deposit back. One deposit, one slot, every
   dataset.
5. The lane verified: all three grains exact, zero orphans on
   `ap_payments -> ap_invoices` and on the bank-to-payment link;
   entity glosses landed through the workspace vocabulary — aspects
   declared during `fin`'s onboarding admit glosses in `fin_ap`
   without re-declaration.

## Found on the way

- **Two datasets break the model app's sole-dataset binding** — by
  design (the refusal names the cure: "2 datasets in the workspace —
  pin one in app.toml"). The documented forking path handled it: the
  built-in copied to `apps/model/` with `dataset = "fin"` pinned,
  hot-loaded, no restart. A workspace that grows past one dataset
  forks its apps; the built-in stays the sole-dataset zero-config
  case.
- **The actor gap sharpened**: even through a real MCP client the
  agent slots land as `rmcp` — after a server restart, calls arrive
  without their session's initialize and `peer_info` serves the
  transport default. Not a stateless-curl artifact; every reconnected
  client is in this state until it re-initializes. Kept open, under
  observation (project lead, 2026-08-12).

## Standing

The proposal fixture's gate — "a real run: the next dataset from a
known system reading conventions before its first probe" — is met.
Corpus entry is the project lead's call.
