# A view's definition: additivity, row identity and dimensions

A reader that aggregates a view's rows needs to know how the measure
combines over time, which columns identify a row and which columns are
dimensions. The examples show a flow, a stock and a ratio, first with
those keys in the definition's body and then, as SPEC.md §5.2 states
it, as annotations on the view and its columns.

## Keys in the body

A budget, a flow, names its additivity and its dimensions in the body:

```glossql
GLOSS revised_budget ON brla AS $${
  "sql": "SELECT fiscalmonth, revisedbudget AS value, fund, segment2 FROM expense_budget",
  "additivity": "additive",
  "axes": ["fund", "segment2"]
}$$;
```

A kit can declare a schema for the keys of a body:

```glossql
DECLARE ASPECT kpi_definition WITH $${
  "x-kind": "definition",
  "type": "object", "additionalProperties": false,
  "properties": {
    "sql": {"type": "string"}, "stopped": {"type": "string"},
    "assumptions": {"type": "array"},
    "additivity": {"enum": ["semi-additive", "additive", "ratio"]},
    "grain": {"type": "array", "items": {"type": "string"}},
    "axes": {"type": "array", "items": {"type": "string"}}
  }
}$$ AS FACT ON DATASET;
```

A stock as a running balance, with one row per event. Rows of the same
date repeat the day's closing level:

```glossql
GLOSS ar_balance ON fin AS $${
  "sql": "SELECT date, sum(delta) OVER (ORDER BY date) AS value FROM events",
  "additivity": "semi-additive"
}$$;
```

The same stock with one row per date, and that row identity named:

```glossql
GLOSS ar_balance ON fin AS $${
  "sql": "WITH daily AS (SELECT date, sum(delta) AS delta FROM events GROUP BY date) SELECT date, sum(delta) OVER (ORDER BY date) AS value FROM daily",
  "additivity": "semi-additive",
  "grain": ["date"]
}$$;
```

## Annotations on the view's columns

The body holds the SQL. The period, the measure and its additivity, the
dimensions and the row identity are annotations on the view and its
columns:

```glossql
GLOSS revised_budget ON brla AS $${
  "sql": "SELECT fiscalmonth AS period, revisedbudget AS value, fund, segment2 FROM expense_budget"
}$$;

GLOSS role ON revised_budget.period AS $${"value": "timestamp"}$$;
GLOSS role ON revised_budget.value AS $${"value": "measure"}$$;
GLOSS additivity ON revised_budget.value AS $${
  "value": "additive",
  "basis": "a row is the amount booked in its month; the year is the sum"
}$$;
GLOSS role ON revised_budget.fund AS $${"value": "dimension"}$$;
GLOSS dimension ON revised_budget.fund AS $${"value": "primary"}$$;
GLOSS role ON revised_budget.segment2 AS $${"value": "dimension"}$$;
GLOSS dimension ON revised_budget.segment2 AS $${"value": "primary"}$$;
GLOSS entity ON revised_budget AS $${
  "value": "a budget line's month",
  "grain": ["period", "fund", "segment2"]
}$$;
```

A stock:

```glossql
GLOSS role ON ar_balance.date AS $${"value": "timestamp"}$$;
GLOSS additivity ON ar_balance.value AS $${"value": "semi-additive"}$$;
GLOSS entity ON ar_balance AS $${
  "value": "the receivable at a day's end",
  "grain": ["date"]
}$$;
```

A ratio returns its numerator and denominator as columns and names
them, so a total divides the summed parts:

```glossql
GLOSS service_level ON demo AS $${
  "sql": "SELECT system, count(*) FILTER (WHERE required_kept) AS kept, count(*) AS total, count(*) FILTER (WHERE required_kept) / count(*) AS value FROM harmonised GROUP BY system"
}$$;

GLOSS role ON service_level.value AS $${"value": "measure"}$$;
GLOSS additivity ON service_level.value AS $${
  "value": "ratio", "numerator": "kept", "denominator": "total"
}$$;
GLOSS role ON service_level.system AS $${"value": "dimension"}$$;
```

The additivity aspect with the ratio and its parts:

```glossql
DECLARE ASPECT additivity WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"enum": ["semi-additive", "additive", "ratio", "none"]},
                 "numerator": {"type": "string"},
                 "denominator": {"type": "string"},
                 "basis": {"type": "string"}}
}$$ AS FACT ON COLUMN WHEN role = 'measure';
```
