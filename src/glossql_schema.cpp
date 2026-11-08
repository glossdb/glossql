// JSON Schema admission: every gloss body is a JSON document validated by a
// JSON Schema (SPEC §1, §5.2). DuckDB has no validator, so this is one — the
// subset the glossql schemas use — over the yyjson DuckDB ships, exposed as
// the scalar function json_schema_check(schema, document): NULL when the
// document conforms, otherwise the first violation as text.
//
// Keywords: type, enum, const, properties, required, additionalProperties,
// items, minItems, maxItems, minimum, maximum, minLength, maxLength, oneOf,
// anyOf, allOf, not. Everything else (title, description, x-*) is an
// annotation and ignored.

#include "glossql_grammar.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

static string TypeName(yyjson_val *value) {
	switch (yyjson_get_type(value)) {
	case YYJSON_TYPE_NULL:
		return "null";
	case YYJSON_TYPE_BOOL:
		return "boolean";
	case YYJSON_TYPE_NUM:
		return yyjson_is_int(value) || yyjson_is_uint(value) ? "integer" : "number";
	case YYJSON_TYPE_STR:
		return "string";
	case YYJSON_TYPE_ARR:
		return "array";
	case YYJSON_TYPE_OBJ:
		return "object";
	default:
		return "unknown";
	}
}

static bool HasType(yyjson_val *value, const string &expected) {
	auto actual = TypeName(value);
	if (expected == "number") {
		return actual == "number" || actual == "integer";
	}
	if (expected == "integer" && actual == "number") {
		auto number = yyjson_get_num(value);
		return number == static_cast<double>(static_cast<int64_t>(number));
	}
	return actual == expected;
}

static string Check(yyjson_val *schema, yyjson_val *value, const string &path);

static string Violation(const string &path, const string &message) {
	return "at " + path + ": " + message;
}

static string CheckType(yyjson_val *type, yyjson_val *value, const string &path) {
	if (yyjson_is_str(type)) {
		return HasType(value, yyjson_get_str(type))
		           ? string()
				   : Violation(path, string("expected ") + yyjson_get_str(type) + ", got " + TypeName(value));
	}
	if (yyjson_is_arr(type)) {
		vector<string> names;
		size_t idx, max;
		yyjson_val *entry;
		yyjson_arr_foreach(type, idx, max, entry) {
			if (yyjson_is_str(entry)) {
				if (HasType(value, yyjson_get_str(entry))) {
					return string();
				}
				names.push_back(yyjson_get_str(entry));
			}
		}
		return Violation(path, "expected one of " + StringUtil::Join(names, ", ") + ", got " + TypeName(value));
	}
	return string();
}

static string Describe(yyjson_val *value) {
	auto text = yyjson_val_write(value, 0, nullptr);
	string result = text ? text : "?";
	free(text);
	return result;
}

static string CheckObject(yyjson_val *schema, yyjson_val *value, const string &path) {
	auto properties = yyjson_obj_get(schema, "properties");
	auto required = yyjson_obj_get(schema, "required");
	auto additional = yyjson_obj_get(schema, "additionalProperties");
	if (required && yyjson_is_arr(required)) {
		size_t idx, max;
		yyjson_val *name;
		yyjson_arr_foreach(required, idx, max, name) {
			if (yyjson_is_str(name) && !yyjson_obj_get(value, yyjson_get_str(name))) {
				return Violation(path, string("missing required property '") + yyjson_get_str(name) + "'");
			}
		}
	}
	size_t idx, max;
	yyjson_val *key, *member;
	yyjson_obj_foreach(value, idx, max, key, member) {
		auto name = yyjson_get_str(key);
		auto property = properties && yyjson_is_obj(properties) ? yyjson_obj_get(properties, name) : nullptr;
		auto child_path = path + "." + name;
		if (property) {
			auto error = Check(property, member, child_path);
			if (!error.empty()) {
				return error;
			}
			continue;
		}
		if (!additional) {
			continue;
		}
		if (yyjson_is_false(additional)) {
			return Violation(path, string("property '") + name + "' is not allowed");
		}
		if (yyjson_is_obj(additional)) {
			auto error = Check(additional, member, child_path);
			if (!error.empty()) {
				return error;
			}
		}
	}
	return string();
}

static string CheckArray(yyjson_val *schema, yyjson_val *value, const string &path) {
	auto size = yyjson_arr_size(value);
	auto min_items = yyjson_obj_get(schema, "minItems");
	if (min_items && yyjson_is_num(min_items) && size < static_cast<size_t>(yyjson_get_num(min_items))) {
		return Violation(path, "expected at least " + to_string(static_cast<idx_t>(yyjson_get_num(min_items))) +
		                           " items, got " + to_string(size));
	}
	auto max_items = yyjson_obj_get(schema, "maxItems");
	if (max_items && yyjson_is_num(max_items) && size > static_cast<size_t>(yyjson_get_num(max_items))) {
		return Violation(path, "expected at most " + to_string(static_cast<idx_t>(yyjson_get_num(max_items))) +
		                           " items, got " + to_string(size));
	}
	auto items = yyjson_obj_get(schema, "items");
	if (items && yyjson_is_obj(items)) {
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(value, idx, max, item) {
			auto error = Check(items, item, path + "[" + to_string(idx) + "]");
			if (!error.empty()) {
				return error;
			}
		}
	}
	return string();
}

static string CheckCombinators(yyjson_val *schema, yyjson_val *value, const string &path) {
	auto all_of = yyjson_obj_get(schema, "allOf");
	if (all_of && yyjson_is_arr(all_of)) {
		size_t idx, max;
		yyjson_val *entry;
		yyjson_arr_foreach(all_of, idx, max, entry) {
			auto error = Check(entry, value, path);
			if (!error.empty()) {
				return error;
			}
		}
	}
	auto any_of = yyjson_obj_get(schema, "anyOf");
	if (any_of && yyjson_is_arr(any_of)) {
		size_t idx, max;
		yyjson_val *entry;
		bool matched = false;
		yyjson_arr_foreach(any_of, idx, max, entry) {
			matched = matched || Check(entry, value, path).empty();
		}
		if (!matched) {
			return Violation(path, "matches none of anyOf");
		}
	}
	auto one_of = yyjson_obj_get(schema, "oneOf");
	if (one_of && yyjson_is_arr(one_of)) {
		size_t idx, max;
		yyjson_val *entry;
		idx_t matches = 0;
		yyjson_arr_foreach(one_of, idx, max, entry) {
			if (Check(entry, value, path).empty()) {
				matches++;
			}
		}
		if (matches != 1) {
			return Violation(path, "matches " + to_string(matches) + " of oneOf, not exactly one");
		}
	}
	auto negated = yyjson_obj_get(schema, "not");
	if (negated && yyjson_is_obj(negated) && Check(negated, value, path).empty()) {
		return Violation(path, "matches the schema under not");
	}
	return string();
}

static string Check(yyjson_val *schema, yyjson_val *value, const string &path) {
	if (yyjson_is_bool(schema)) {
		return yyjson_get_bool(schema) ? string() : Violation(path, "the schema admits nothing");
	}
	if (!yyjson_is_obj(schema)) {
		return Violation(path, "the schema is not an object");
	}
	auto type = yyjson_obj_get(schema, "type");
	if (type) {
		auto error = CheckType(type, value, path);
		if (!error.empty()) {
			return error;
		}
	}
	auto constant = yyjson_obj_get(schema, "const");
	if (constant && !yyjson_equals(constant, value)) {
		return Violation(path, "expected the constant " + Describe(constant) + ", got " + Describe(value));
	}
	auto options = yyjson_obj_get(schema, "enum");
	if (options && yyjson_is_arr(options)) {
		bool member = false;
		size_t idx, max;
		yyjson_val *option;
		yyjson_arr_foreach(options, idx, max, option) {
			member = member || yyjson_equals(option, value);
		}
		if (!member) {
			return Violation(path, Describe(value) + " is not one of " + Describe(options));
		}
	}
	if (yyjson_is_num(value)) {
		auto number = yyjson_get_num(value);
		auto minimum = yyjson_obj_get(schema, "minimum");
		if (minimum && yyjson_is_num(minimum) && number < yyjson_get_num(minimum)) {
			return Violation(path, Describe(value) + " is below the minimum " + Describe(minimum));
		}
		auto maximum = yyjson_obj_get(schema, "maximum");
		if (maximum && yyjson_is_num(maximum) && number > yyjson_get_num(maximum)) {
			return Violation(path, Describe(value) + " is above the maximum " + Describe(maximum));
		}
	}
	if (yyjson_is_str(value)) {
		auto length = yyjson_get_len(value);
		auto min_length = yyjson_obj_get(schema, "minLength");
		if (min_length && yyjson_is_num(min_length) && length < static_cast<size_t>(yyjson_get_num(min_length))) {
			return Violation(path, "shorter than minLength " + Describe(min_length));
		}
		auto max_length = yyjson_obj_get(schema, "maxLength");
		if (max_length && yyjson_is_num(max_length) && length > static_cast<size_t>(yyjson_get_num(max_length))) {
			return Violation(path, "longer than maxLength " + Describe(max_length));
		}
	}
	if (yyjson_is_obj(value)) {
		auto error = CheckObject(schema, value, path);
		if (!error.empty()) {
			return error;
		}
	}
	if (yyjson_is_arr(value)) {
		auto error = CheckArray(schema, value, path);
		if (!error.empty()) {
			return error;
		}
	}
	return CheckCombinators(schema, value, path);
}

string JsonSchemaCheck(const string &schema_text, const string &document_text) {
	auto schema = yyjson_read(schema_text.c_str(), schema_text.size(), 0);
	if (!schema) {
		return "the schema is not valid JSON";
	}
	auto document = yyjson_read(document_text.c_str(), document_text.size(), 0);
	if (!document) {
		yyjson_doc_free(schema);
		return "the document is not valid JSON";
	}
	auto result = Check(yyjson_doc_get_root(schema), yyjson_doc_get_root(document), "$");
	yyjson_doc_free(document);
	yyjson_doc_free(schema);
	return result;
}

//! Whether a schema pins `value` to an enum that does not hold the literal (SPEC §5.1, WHEN)
string JsonSchemaValueEnumCheck(const string &schema_text, const string &literal) {
	auto schema = yyjson_read(schema_text.c_str(), schema_text.size(), 0);
	if (!schema) {
		return string();
	}
	string result;
	auto properties = yyjson_obj_get(yyjson_doc_get_root(schema), "properties");
	auto value = properties ? yyjson_obj_get(properties, "value") : nullptr;
	auto options = value ? yyjson_obj_get(value, "enum") : nullptr;
	if (options && yyjson_is_arr(options)) {
		bool member = false;
		size_t idx, max;
		yyjson_val *option;
		yyjson_arr_foreach(options, idx, max, option) {
			member = member || (yyjson_is_str(option) && literal == yyjson_get_str(option));
		}
		if (!member) {
			result = "'" + literal + "' is not one of " + Describe(options);
		}
	}
	yyjson_doc_free(schema);
	return result;
}

string JsonStringField(const string &document_text, const string &field) {
	auto document = yyjson_read(document_text.c_str(), document_text.size(), 0);
	if (!document) {
		return string();
	}
	string result;
	auto value = yyjson_obj_get(yyjson_doc_get_root(document), field.c_str());
	if (value && yyjson_is_str(value)) {
		result = yyjson_get_str(value);
	}
	yyjson_doc_free(document);
	return result;
}

//! The metric definition contract: what the engine reads of a VIEW gloss — the sql it
//! serves as a view, or the stop that ends one — and the disclosure key every
//! reader may rely on. What a reader needs of the view's rows — the period, the
//! measure and its behavior, the slices — is glossed on the view's columns under
//! a kit's aspects (SPEC §5.2); a kit that needs a body key of its own declares
//! a schema for it, flavored `x-kind: grounding`, validated after this one.
const char *const GROUNDING_CONTRACT = R"JSON({
  "type": "object",
  "oneOf": [{"required": ["sql"]}, {"required": ["stopped"]}],
  "properties": {
    "sql": {"type": "string"},
    "stopped": {"type": "string"},
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
})JSON";

//===--------------------------------------------------------------------===//
// json_schema_check(schema, document): NULL when the document conforms
//===--------------------------------------------------------------------===//

static void JsonSchemaCheckExec(DataChunk &args, ExpressionState &state, Vector &result) {
	auto schemas = args.data[0].Values<string_t>();
	auto documents = args.data[1].Values<string_t>();
	auto writer = FlatVector::Writer<string_t>(result, args.size());
	for (idx_t i = 0; i < args.size(); i++) {
		auto schema = schemas[i];
		auto document = documents[i];
		if (!schema.IsValid() || !document.IsValid()) {
			writer.WriteNull();
			continue;
		}
		auto error = JsonSchemaCheck(schema.GetValue().GetString(), document.GetValue().GetString());
		if (error.empty()) {
			writer.WriteNull();
			continue;
		}
		writer.WriteValue(StringVector::AddString(result, error));
	}
}

void RegisterGlossSchema(ExtensionLoader &loader) {
	loader.RegisterFunction(ScalarFunction("json_schema_check", {LogicalType::JSON(), LogicalType::JSON()},
	                                       LogicalType::VARCHAR, JsonSchemaCheckExec));
}

} // namespace duckdb
