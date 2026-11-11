# Source conventions

A source system has conventions that hold for every export from it:
date format, currency, sign, null markers. The example annotates them
on the source (`AS FACT ON SOURCE`), reads them from a second dataset
before its first preview, and adds a convention that both datasets then
read.

## Annotate the source

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

## Read from a second dataset

```glossql
DECLARE DATASET fin_ap SET (purpose: 'AP lane from the same source');
USE fin_ap;

SELECT value FROM GLOSSARY(erp_export) WHERE aspect = 'conventions';

PREVIEW erp_export AS $$SELECT count(*) AS n, count(p_rec) AS reconciled_parsed
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

## Add a convention

One value is current per source, whichever dataset writes it.

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
