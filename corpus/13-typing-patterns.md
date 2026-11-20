# Type patterns and null markers

A data team keeps a list of the text patterns that mark a date or a
number, and a list of the strings that mean null. The example stores
both as annotations on the dataset, where the author of a load query
reads them before writing the casts.

```glossql
USE fin;

DECLARE PROPERTY null_values WITH $${
  "type": "object",
  "properties": {
    "values": {"type": "array", "items": {"type": "object",
      "properties": {"value": {"type": "string"},
                     "case_sensitive": {"type": "boolean"},
                     "category": {"type": "string"}},
      "required": ["value"]}}},
  "required": ["values"],
  "additionalProperties": false
}$$ AS FACT;

GLOSS null_values ON fin AS $${"values": [
  {"value": "", "category": "standard"},
  {"value": "NULL", "case_sensitive": false, "category": "standard"},
  {"value": "#N/A", "category": "spreadsheet"},
  {"value": "TBD", "category": "missing_indicator"}
]}$$;

DECLARE PROPERTY type_patterns WITH $${
  "type": "object",
  "properties": {
    "min_confidence": {"type": "number"},
    "patterns": {"type": "array", "items": {"type": "object",
      "properties": {"name": {"type": "string"},
                     "pattern": {"type": "string"},
                     "inferred_type": {"type": "string"},
                     "ambiguous": {"type": "boolean"},
                     "standardization_expr": {"type": "string"},
                     "examples": {"type": "array"}},
      "required": ["name", "pattern", "inferred_type"]}}},
  "required": ["patterns"],
  "additionalProperties": false
}$$ AS FACT;

GLOSS type_patterns ON fin AS $${"min_confidence": 0.85, "patterns": [
  {"name": "iso_date", "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
   "inferred_type": "DATE", "examples": ["2024-01-15"]},
  {"name": "eu_date", "pattern": "^\\d{1,2}\\.\\d{1,2}\\.\\d{2,4}$",
   "inferred_type": "DATE",
   "standardization_expr": "try_strptime(\"{col}\", '%d.%m.%Y')::DATE"},
  {"name": "us_date", "pattern": "^\\d{1,2}/\\d{1,2}/\\d{2,4}$",
   "inferred_type": "DATE", "ambiguous": true,
   "standardization_expr": "try_strptime(\"{col}\", '%m/%d/%Y')::DATE"}
]}$$;
```

A person adds a null marker by annotating the whole list again:

```glossql
GLOSS null_values ON fin AS $${"values": [
  {"value": "", "category": "standard"},
  {"value": "NULL", "case_sensitive": false, "category": "standard"},
  {"value": "#N/A", "category": "spreadsheet"},
  {"value": "TBD", "category": "missing_indicator"},
  {"value": "~~~~~", "category": "taught"}
]}$$;
```
