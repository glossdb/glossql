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

/// The standard attest contract, verbatim from SPEC.md §7.2 — the
/// engine's contract for each row a detector's query returns. A detector
/// is a function without RETURNS (role by shape); the engine completes
/// the attest row with witness, aspect, and its own clock, so nobody
/// authors those.
pub const ATTEST_CONTRACT: &str = r#"{
  "type": "object",
  "required": ["subject", "band", "score"],
  "properties": {
    "subject": {"type": "string"},
    "band": {"enum": ["green", "yellow", "orange", "red"]},
    "score": {"type": "number", "minimum": 0, "maximum": 1}
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
