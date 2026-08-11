//! The two fixed schemas the language nails down: the standard grounding
//! schema (SPEC.md §5.2, validates every QUERY gloss) and the standard
//! attest contract (SPEC.md §7.2, validates every detector's output).

use serde_json::Value;

/// The standard grounding schema, verbatim from SPEC.md §5.2.
pub const GROUNDING_SCHEMA: &str = r#"{
  "type": "object",
  "required": ["sql"],
  "additionalProperties": false,
  "properties": {
    "sql": {"type": "string"},
    "behavior": {"enum": ["stock", "flow"]},
    "assumptions": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["assumption"],
        "properties": {
          "dimension": {"type": "string"},
          "assumption": {"type": "string"},
          "basis": {"type": "string"},
          "confidence": {"type": "number", "minimum": 0, "maximum": 1}
        }
      }
    }
  }
}"#;

pub fn grounding_schema() -> Value {
    serde_json::from_str(GROUNDING_SCHEMA).expect("SPEC §5.2 schema is valid JSON")
}

/// The standard attest schema, verbatim from SPEC.md §7.2 — the engine's
/// contract for detector output. A detector is a function without RETURNS
/// (role by shape, ruled 2026-08-04); nobody authors this shape.
pub const ATTEST_CONTRACT: &str = r#"{
  "type": "object",
  "required": ["subject", "aspect", "witness", "band", "score", "computed_at"],
  "properties": {
    "subject": {"type": "string"},
    "aspect": {"type": "string"},
    "witness": {"type": "string"},
    "band": {"enum": ["green", "yellow", "orange", "red"]},
    "score": {"type": "number", "minimum": 0, "maximum": 1},
    "computed_at": {"type": "string"}
  }
}"#;

pub fn attest_contract() -> Value {
    serde_json::from_str(ATTEST_CONTRACT).expect("SPEC §7.2 schema is valid JSON")
}

/// Validate `instance` against `schema`, first violation as the message.
pub fn validate_instance(schema: &Value, instance: &Value) -> Result<(), String> {
    let validator = jsonschema::validator_for(schema).map_err(|e| e.to_string())?;
    validator.validate(instance).map_err(|e| e.to_string())
}
