# 20 · Misfit: the declared sample frame — TRANSCRIBES (frame = QUERY aspect; the `misfit.` door)

Source: our own evaluation runs in `../tfmeval` (FINDINGS.md, E1
series). The row-grain adjudication is the proven read: on shuffled
invoice-payment pairings — every individual value legal, nothing
deterministic reaches — the in-context density ranked the wrong rows
at 0.93 on the joined surface, composing as the judge pattern
prescribes (the measurement optimizes recall, the judge removes false
positives). Two more measured facts shape the form: the model reads
the same whether fit on a clean reference or on the whole frame
(protocol-robust, unlike the classical density), and some tables
carry too little numeric surface for a density read at all — the
read must abstain there, never serve noise. Presented and ruled
2026-08-11: the frame is an ordinary QUERY aspect, one frame ranked
against itself; the dead forks in §5.

## 1. The frame declares like a metric — one QUERY aspect

The same placement metrics have (fixture 16 §1): a QUERY aspect,
glossed with SQL, superseded and witnessed like any other. The flavor
rides `x-kind`, never the syntax (the `read.` ruling, fixture 16;
reaffirmed fixture 19): `"sample"` marks a frame authored to be
ranked, for the census and the app listings — the door itself serves
any current QUERY grounding, so the microscope drops onto a metric's
own extract with no re-declaration.

```glossql
USE fin;

DECLARE ASPECT payment_pairs WITH $${
  "title": "Payment rows against their invoice, six months and March",
  "description": "Authored after the relationship check went red on March",
  "x-kind": "sample"
}$$ AS QUERY ON DATASET;
```

## 2. The frame body — one SQL, ranked against itself

The gloss body is the standard grounding schema, unchanged: `sql`
plus `assumptions`. The frame carries history and suspects in one
SELECT — the author picks the surface, and on a relationship
suspicion the surface is the join (the eval's lesson: the marginal
table is structurally blind there). The assumptions say why this
surface, this span.

```glossql
GLOSS payment_pairs ON fin AS $${
  "sql": "SELECT p.amount, i.amount AS invoiced, p.paid_date - i.invoice_date AS day_delta, i.terms_days FROM payments p JOIN invoices i ON p.invoice_id = i.invoice_id WHERE p.paid_date >= DATE '2025-09-01'",
  "assumptions": [
    {"assumption": "joined surface: the suspicion is the pairing, not either table alone",
     "basis": "the relationship check that fired on March"}
  ]
}$$;
```

## 3. The read — the frame's own columns, plus the score

`misfit.<frame>()` serves the frame's rows with two columns added:
`misfit` (higher = fits the rest of the frame worse; log scale) and
`basis` (which columns were ranked on, which were excluded and why).
Narrowing and ordering are `WHERE` and `ORDER BY` over this relation,
never a special form (the `ATTEST` rule).

```glossql
SELECT * FROM misfit.payment_pairs() ORDER BY misfit DESC LIMIT 20;

SELECT day_delta, misfit, basis FROM misfit.payment_pairs()
WHERE day_delta < 0 ORDER BY misfit DESC;
```

What the read refuses, by name: a frame past the stated row cap
(with the number and the fix — narrow the SQL); a frame with too
little numeric surface once ids and constants are excluded (the
abstention the eval measured on `sales_orders`); a frame whose
grounding is not current. A misfit refusal refuses the read — the
relation's shape *is* the frame, so there is no fixed schema to
carry refusal rows the way `whatif.` does.

## 4. Versioning and governance fall out of the store

Supersession key (subject, aspect, actor kind): re-glossing narrows
or replaces the frame; a human's frame retires an agent's. A witness
makes frame authorship a policy, with no new construct:

```glossql
DECLARE WITNESS frame_gate ON payment_pairs BY (HUMAN, AGENT);
```

The ranking itself persists nothing. What the judge concludes lands
as an ordinary gloss on the subject it concerns (the attest flow,
fixture 12) — the finding is durable, the machinery stateless.

## 5. The forks that died (ruled 2026-08-11)

- **A first-class kind (`AS SAMPLE`)** — dies on the ruling already
  made twice: flavor rides `x-kind`, never syntax (fixtures 16, 19).
  A frame is exactly a SQL-grounded gloss — same validation, same
  supersession, same witnesses; a fourth kind forks the grammar and
  the store for zero semantic difference.
- **An optional `reference_sql`** ("rank these rows against those")
  — dies on the closed grounding schema (SPEC §5.2 admits `sql` and
  `assumptions`; widening it touches every metric gloss) and on the
  measurement: the model is protocol-robust, so one frame ranked
  against itself carries the question — the reference is the frame's
  own `WHERE` clause.
- **Frame SQL as a door argument** — dies on the bare-call rule
  (settings are context, never call arguments, ruled 2026-08-04) and
  leaves no record: not superseded, not attested, not reproducible.

## 6. Machinery, not language

None of the following appears in any statement; it is the server's
job, recorded here so the fixture stays honest about what the door
does:

- **Signal-triggered**: the read runs when a reader asks — after a
  band breach, a red check, a doubt — never per import. Nothing is
  evaluated before a reader asks.
- **Self-fit chain-rule density**: the model fits on the frame's
  rows and scores the same rows; per-feature conditional log
  densities sum, averaged over feature orderings; log space end to
  end (exp space destroys the information — measured). `misfit` is
  the negated mean log density.
- **The numeric surface is discovered, exclusions named in `basis`**:
  non-numeric columns, constants, and id-like columns (all-distinct
  integers) are excluded — key-like columns would separate any two
  row sets trivially, the eval's structural lesson.
- **Stated caps, never silent ones**: the row cap refuses with the
  number; fewer than two usable columns abstains with the reason.
  An investigation frame is small by construction.
- **No cache**: the read recomputes — seconds at investigation size;
  the durable artifact is the judge's gloss, never the ranking.
