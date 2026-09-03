# Sources, probes, recipes

A **source** names where data comes from. A **recipe** materializes a
table from a source. The landed table is the typed table — there is no
raw twin, no derived cleaning layer, no typing machinery. **Typing is
authored**: the recipe carries the casts, and the author probes first
to write them.

## Sources

```glossql
DECLARE SOURCE erp_export SET (type: parquet, location: 'lake/erp');
DECLARE SOURCE crm SET (type: relational_db, location: 'postgres://crm.internal/prod', via: crm_prod);
```

`type` is `relational_db | parquet | csv | json`; any other spelling
is refused at the declaration. For file sources,
`location` is the root directory recipe paths resolve under; for a
relational source it is the connection URI the recipe executes over.
A file type describes the export, it does not constrain the recipe:
the recipe names its own reader — `read_parquet`, `read_csv`,
`read_json` — and all three resolve under the location whatever the
source was declared as. The type decides where the SQL runs: a relational source executes it,
a file source has the server run it.
Every other `SET` pair — `via` above — rides the source's stored
settings.

## Probes rehearse, recipes land

What a recipe can name is a read: `SELECT path, size, modified FROM
source_files('erp_export')` lists every file under the source's
location, subdirectories included, through the same object store a
`read_*` glob resolves through ([reads](../reference/reads.md)).

`PROBE source AS $$sql$$` runs recipe-shaped SQL at the source and
lands nothing. It is the recipe rehearsal: the same SQL surface, the
same path resolution, and the result always carries its schema — a
`LIMIT 0` probe of the final SQL rehearses exactly the identity the
recipe will stamp. At a file source a table is a file: a probe or
recipe that names a plain table is refused with the source's files
and the `read_*` call that reads one.

```glossql
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

Recipe SQL runs **at the source**: a relational source executes it in
its own dialect; at a file source the server runs it, with
`read_parquet` / `read_csv` / `read_json` resolving under the source's
location and `try_to_date` / `try_to_timestamp` registered. The
default recipe is `SELECT *`.

**Cast accounting.** The engine keeps one number per import —
`dropped_rows_count`, source rows minus landed rows — in the
statement's outcome and in the `imports` relation for history. Which
rows were dropped is the author's question, answered at the source.
There are no sentinel lists: cast accounting surfaces candidates, and
closure is an authored recipe amendment.

## Identity and correction

Statement identity is content — the recipe SQL and the schema it
produces. An unchanged re-declaration is a no-op. A changed one
supersedes and re-lands: the table lands fresh and its record starts
over with it. Glosses stay — no machinery deletes knowledge; their
snapshot ids disclose their age against the fresh landing. This
supersede-and-reland is the correction path: fix a source wart by
re-declaring the recipe, never by editing data.

`DROP TABLE` removes a table and refuses while it holds data or
glosses. Substrate DDL that would alter schema or data directly is
closed — tables come from recipes.

## The source deposit

What an onboarding learns about a source *system* — date formats, sign
conventions, naming warts — belongs to the source, not to any one
dataset. An aspect declared `AS FACT ON SOURCE` attaches to the
declared source and its slots read, supersede, and disclose across
every dataset in the workspace:

```glossql
DECLARE ASPECT conventions WITH $${"type": "object"}$$ AS FACT ON SOURCE;

GLOSS conventions ON erp_export AS $${
  "dates": "ISO YYYY-MM-DD throughout; try_cast(x AS DATE) suffices",
  "currency": "single-currency USD on every monetary table",
  "sign": "journal net_amount = debit - credit (debit-positive), an exact identity"
}$$;
```

The next dataset from the same system reads the deposit before its
first probe; what it learns lands in the deposit by an ordinary
re-speak.
Dataset-local evidence (orphan populations, grain verdicts) stays in
dataset glosses — only what the next export will also carry belongs at
source grain.
