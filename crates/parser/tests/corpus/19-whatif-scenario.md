# 19 · What-if: the declared scenario — TRANSCRIBES (lever = FACT aspect on the column; scenario = FACT aspect per scenario, a factor per period; the scenario world on `metric_series`)

Source: our own evaluation runs — the counterfactual walk across
lever-varied support worlds (effect recovery 1.011 in support), its
independent reproduction (the no-support regime measured and closed —
bare history cannot carry the read), and the what-if route's tests on
thirty real tables (GK-47: the direction holds where the history shows
an effect; the size is a guess between two estimates).
The product shape: a lever is declared on its column, a scenario is
declared as a factor per period on declared levers, and the engine
reads the scenario as a world beside the actual and the forecast
worlds — the arithmetic replayed through the groundings over the
data, the estimate from the what-if service ahead of it.
Ruled: a lever is its own FACT aspect on the column (Fork A; a sixth
`role` value died on the kit's conditions over `role`); each scenario
is its own FACT aspect (Fork A; the shared-aspect form died on the
supersession key, the arguments-at-the-door form on the bare-call
rule); the factor per period (Fork A; one factor from a start month
died on the wire); the scenario as a world on `metric_series` (Fork A;
the operation door `whatif.<scenario>()` retired: one read serves every
world).

## 1. The lever declares on its column

A lever is a column someone sets — a list price, a discount rate, a
promotion flag. The kit ships the aspect; the gloss says who sets it.
A column nobody glossed is no lever, and a column examined and found
none is glossed `false`.

```glossql
USE fin;

GLOSS lever ON sales_order_lines.unit_price AS $${
  "value": true, "grounds": "the list price, set by sales operations each quarter"
}$$;
GLOSS lever ON sales_order_lines.units AS $${
  "value": false, "grounds": "demand — nobody sets it"
}$$;
```

## 2. The scenario declares like a metric — one aspect per scenario

The same placement metrics have (fixture 16 §1: one QUERY aspect per
concept, served by `read.<name>()`): one FACT aspect per scenario,
`x-kind: scenario`. The flavor rides `x-kind`, never the syntax (the
`read.` ruling, fixture 16).

```glossql
DECLARE ASPECT price_hike WITH $${
  "title": "Price +15% from Jan 2027",
  "description": "List prices raised 15% across the board",
  "x-kind": "scenario",
  "type": "object",
  "required": ["levers"],
  "properties": {"levers": {"type": "array", "items": {
    "type": "object",
    "required": ["column", "factor", "basis"],
    "properties": {
      "column": {"type": "string"},
      "factor": {"type": "object", "additionalProperties": {"type": "number"}},
      "basis":  {"type": "string"}
    }
  }}}
}$$ AS FACT ON DATASET;
```

## 3. The scenario body — a factor per period on declared levers

Each lever names a declared column, a factor per period and its
basis — the assumptions discipline the grounding glosses already
carry (fixture 16 §2). A period is `YYYY-MM`, a calendar month, or
`YYYY-MM-DD`, a day; a period not named keeps a factor of one. A
column without a current `lever` gloss refuses the scenario before it
lands. A behavioral response the history never saw (demand
elasticity) is not declared here: it is what the estimate reads from
the history, with its basis, and where the history shows none the
cell says so.

```glossql
GLOSS price_hike ON fin AS $${
  "levers": [
    {"column": "sales_order_lines.unit_price",
     "factor": {"2027-01": 1.15, "2027-02": 1.15, "2027-03": 1.15,
                "2027-04": 1.15, "2027-05": 1.15, "2027-06": 1.15},
     "basis": "the price list the board approved in October"}
  ]
}$$;
```

## 4. The read — a world on the cube's read, narrowed by WHERE

The scenario is a set of numbers like the forecast is, and it is read
the same way: `metric_series(world => 'scenario', scenario =>
'<name>')` serves every concept's cells under the scenario at its own
resolution, beside `world => 'forecast'` and the default actual world.
`scenario_points(scenario => '<name>')` says per cell which pass made
it — `replayed`, the grounding over the data with the factors, or
`estimated`, the what-if's reading ahead of the last actual — with the
estimate's 80% band, how often such a band held at that grain and
step, and the service's `sign` and `basis`; a concept the service
cannot estimate gets the reason, never a silent guess. Sweeps are
`WHERE` clauses over these relations, never a special form (the
`ATTEST` rule).

```glossql
SELECT metric, period, value FROM metric_series(world => 'scenario', scenario => 'price_hike');
SELECT period, pass, value, low, high, held_80, held_from, sign, basis
FROM scenario_points(scenario => 'price_hike') WHERE metric = 'revenue';
```

## 5. Versioning and governance fall out of the store

Supersession key (subject, aspect, actor kind): re-glossing updates
the scenario, a human's numbers retire an agent's. A witness makes
scenario authorship a policy, with no new construct:

```glossql
DECLARE WITNESS scenario_gate ON price_hike BY (HUMAN);

GLOSS price_hike ON fin AS $${
  "levers": [
    {"column": "sales_order_lines.unit_price",
     "factor": {"2027-01": 1.10, "2027-02": 1.10, "2027-03": 1.10},
     "basis": "the declared lever, revised down after review"}
  ]
}$$;
```

Removal is ordinary glossary SQL, as everywhere.

## 6. The forks that died

- **One shared `scenario` aspect, each scenario one gloss** — dies on
  the store's own key: (subject, aspect, actor kind) keeps one
  current body, so a second scenario silently retires the first; and
  the scenario's name lives inside the body, invisible to the read.
- **Overrides as arguments at the door** — dies on the bare-call rule
  (settings are context, never call arguments) and
  leaves no record: not superseded, not attested, not reproducible.
- **A lever as a sixth `role` value** — dies on the kit's own
  conditions: `behavior` and `unit` are owed where `role = 'measure'`,
  so a price glossed `lever` would stop being a measure. A lever is a
  further fact about a column, beside its role.
- **One factor from a start month** — dies on the wire: the what-if
  takes the input per period, as planned and under the change, and a
  plan is values per period. A factor per period carries both.
- **An operation-named door, `whatif.<scenario>()`** — retired when the
  forecast became a world on `metric_series` (fixture 26 §4, the
  `world` column): a scenario is another set of numbers over the same
  cells, and one read serves every world; a second door would carry
  the same columns under another name.

## 7. Machinery, not language

None of the following appears in any statement; it is the server's
job, recorded here so the fixture stays honest about what the read
does:

- **Two passes.** Where a concept's grounding scans a lever's table,
  the engine replays the grounding with the factors per period over
  the rows it holds — every scan of the table gains a projection over
  the column, the SQL runs unchanged — and the cells are `replayed`,
  exact as far as the data reaches. Ahead of the concept's last
  actual, up to the last period the scenario names, the what-if
  service estimates every concept from its own series and the
  lever's, the lever as planned (its last value held) and under the
  scenario (that value times the period's factor); the cells are
  `estimated`, with the model's band and the coverage the forecast
  record gives such a band at that grain and step.
- **Reach is a scan.** An override moves the concepts whose groundings
  scan the overridden table, through any view they nest; the
  arithmetic never crosses to a concept that does not.
- **Placed in time.** A lever's rows are placed by its table's own
  date column, else by a declared edge to a table that carries one —
  a fact table keyed into a date dimension — the key against the keys
  whose dates fall in each period; a lever placed neither way is
  refused when the scenario is glossed.
- **Several changed inputs as one change.** The what-if reads every
  lever of the scenario in one request, the history adding what each
  does; `scenario_levers()` serves each lever's part, and an
  interaction between levers is not read.
- **Nothing recorded, nothing cached.** The scenario is a read; the
  service is called at each one.
