# An annotation required only where it applies

Additivity is a property of a measure. It is not required of a key or
a text column. The example declares the aspect with a condition
(`WHEN`): the annotation counts as missing on a column only while that
column's `role` is `measure`.

```glossql
DECLARE ASPECT additivity WITH $${
  "type": "object", "required": ["value"],
  "properties": {"value": {"enum": ["semi-additive", "additive", "none"]}}
}$$ AS FACT ON COLUMN WHEN role = 'measure';
```
