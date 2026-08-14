# 21 · Source conventions: the per-source deposit — TRANSCRIBES (`AS FACT ON SOURCE`)

Source: our own runs, both halves on 2026-08-12 in one workspace
(`reports/2026-08-12-onboarding-run-pin-queue.md` deposited during the
fresh-workspace onboarding; `reports/2026-08-12-per-source-read-run.md`
read it back from a second dataset before that dataset's first probe).
The fork record is `feedback/flow-source-conventions.md` — Fork B
ruled in 2026-08-12: no new construct, the SOURCE grain added to the
aspect `ON` list; `DECLARE SOURCE` is the whole definition of what a
source is. Source-grain slots read, supersede, and disclose across
every dataset in the workspace — a convention is a fact about the
source *system*, not about any one dataset.

## 1. The deposit — learned once, spoken at source grain

Declared and spoken during the first dataset's onboarding, after the
conventions were confirmed against the data (never before):

```glossql
USE fin;

DECLARE ASPECT conventions WITH $${"type": "object"}$$ AS FACT ON SOURCE;

GLOSS conventions ON erp_export AS $${
  "dates": "ISO YYYY-MM-DD throughout; try_cast(x AS DATE) suffices",
  "currency": "single-currency USD on every monetary table",
  "sign": "journal net_amount = debit - credit (debit-positive), an exact identity",
  "naming_wart": "trial_balance carries monthly activity totals, not cumulative balances, despite its name",
  "nulls": "absent values are empty CSV cells; no null sentinel tokens observed"
}$$;
```

Dataset-local evidence (orphan populations, grain verdicts) stays in
dataset glosses; only what the next export from the same system will
also carry belongs here.

## 2. The read — the next dataset, before its first probe

The source stands outside datasets, so a new dataset from the same
system reads the deposit immediately — this is the add-source skill's
"read the source's conventions before probing", and in the real run it
replaced six format-discovery probes with one (a boolean column the
deposit did not yet know):

```glossql
DECLARE DATASET fin_ap SET (purpose: 'AP lane from the same source');
USE fin_ap;

SELECT value FROM GLOSSARY(erp_export) WHERE aspect = 'conventions';

PROBE erp_export AS $$SELECT count(*) AS n, count(p_rec) AS reconciled_parsed
FROM (SELECT try_cast(reconciled AS BOOLEAN) AS p_rec
      FROM read_csv('bank_transactions.csv'))$$;

DECLARE RECIPE bank_transactions ON fin_ap FROM erp_export AS $$
  SELECT txn_id, try_cast(account_id AS BIGINT) AS account_id,
         try_cast("date" AS DATE) AS date,
         try_cast(amount AS DOUBLE) AS amount, currency, reference,
         counterparty, try_cast(reconciled AS BOOLEAN) AS reconciled,
         payment_id
  FROM read_csv('bank_transactions.csv')$$;
```

## 3. Supersession is workspace-wide — one slot, every dataset

What the second onboarding learns joins the deposit by an ordinary
re-speak — the supersession key holds one slot per actor kind at
source grain, whichever dataset speaks. The first dataset's channel
reads the updated deposit with no further act (verified in the run:
the `booleans` convention spoken from `fin_ap`, read back from `fin`):

```glossql
GLOSS conventions ON erp_export AS $${
  "dates": "ISO YYYY-MM-DD throughout; try_cast(x AS DATE) suffices",
  "currency": "single-currency USD on every monetary table",
  "sign": "journal net_amount = debit - credit (debit-positive), an exact identity",
  "naming_wart": "trial_balance carries monthly activity totals, not cumulative balances, despite its name",
  "nulls": "absent values are empty CSV cells; no null sentinel tokens observed",
  "booleans": "lowercase true/false text; try_cast(x AS BOOLEAN) parses all rows"
}$$;

USE fin;
SELECT json_get_str(value, 'booleans') AS booleans
FROM GLOSSARY(erp_export) WHERE aspect = 'conventions';
```

Verdict: TRANSCRIBES. Promotion is an ordinary re-speak at source
grain; disclosure (`unassessed`) bounds to declared sources through
the grain; the Iceberg persistence of the deposit rides the storage
integration (proposal §5 in the fork record).
