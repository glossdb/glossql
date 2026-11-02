// gloss_service(route, body): one call to the model service the instance names,
// from inside a query. The body is JSON — one table and a specification, as
// the service's contract spells it — and the answer comes back as JSON, one
// table and an answer object; a kit's function builds the one and reads the
// other in SQL, and what it lands is a measurement like any other. The service
// is named once, for the instance: gloss_service_url is its base, the route the
// argument; no URL named is a refusal that says so, before any call goes out.
// The bearer is gloss_service_token, sent as-is. The call goes through DuckDB's
// own HTTP client (HTTPUtil): the built-in one speaks GET alone, so a POST
// needs httpfs loaded, which also brings TLS; the refusal names that too.
// gloss_service_timeout bounds one call, whole, in seconds. A 4xx or 5xx is an
// error carrying the service's own text, so an agent reads what to repair.
// gloss_service(route, body, true) is the soft form: a service that refuses
// or does not answer comes back as {"error": "…"} instead, for a call whose
// failure a kit can stand without — a lever's own forecast, where the held
// value serves instead; the refusals before any call goes out stay loud.

#include "gloss_grammar.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/http/http_util.hpp"

namespace duckdb {

static string Setting(ClientContext &context, const char *name) {
	Value value;
	(void)context.TryGetCurrentSetting(name, value);
	return value.IsNull() ? string() : value.ToString();
}

static string ErrorJson(const string &text) {
	string out = "{\"error\": \"";
	for (auto c : text) {
		if (c == '"' || c == '\\') {
			out += '\\';
			out += c;
		} else if (c == '\n') {
			out += "\\n";
		} else if (static_cast<unsigned char>(c) < 0x20) {
			out += ' ';
		} else {
			out += c;
		}
	}
	return out + "\"}";
}

template <class EXEC>
static void ServiceRun(DataChunk &args, ExpressionState &state, Vector &result, EXEC run) {
	auto &context = state.GetContext();
	auto &db = DatabaseInstance::GetDatabase(context);
	auto base = Setting(context, "gloss_service_url");
	while (!base.empty() && base.back() == '/') {
		base.pop_back();
	}
	auto token = Setting(context, "gloss_service_token");
	auto timeout_text = Setting(context, "gloss_service_timeout");
	uint64_t timeout = timeout_text.empty() ? 600 : static_cast<uint64_t>(std::stoll(timeout_text));
	auto &util = HTTPUtil::Get(db);
	auto get_only = util.GetName() == "Built-In";

	run([&](string_t route_t, string_t body_t, bool soft) {
		    auto route = route_t.GetString();
		    if (base.empty()) {
			    throw InvalidInputException("no service is named for %s: set gloss_service_url (GLOSSQL_SERVICE_URL) "
				                            "to the model service, and the call goes out",
				                            route);
		    }
		    if (get_only) {
			    throw InvalidInputException("the HTTP client DuckDB carries speaks GET alone: LOAD httpfs, and the call "
			                                "to %s can go out",
			                                route);
		    }
		    auto url = base + route;
		    auto params = util.InitializeParameters(context, url);
		    params->timeout = timeout;
		    params->timeout_usec = 0;
		    HTTPHeaders headers;
		    headers.Insert("Content-Type", "application/json");
		    headers.Insert("Accept", "application/json");
		    if (!token.empty()) {
			    headers.Insert("Authorization", "Bearer " + token);
		    }
		    PostRequestInfo post(url, headers, *params, const_data_ptr_cast(body_t.GetData()), body_t.GetSize());
		    auto response = util.Request(post);
		    auto body = response->body.empty() ? post.buffer_out : response->body;
		    if (response->HasRequestError()) {
			    auto text = StringUtil::Format("the service at %s did not answer: %s", url, response->GetRequestError());
			    if (soft) {
				    return StringVector::AddString(result, ErrorJson(text));
			    }
			    throw IOException(text);
		    }
		    if (!response->Success()) {
			    auto text = StringUtil::Format("the service answered %d on %s: %s", static_cast<int>(response->status),
			                                   route, body.substr(0, 4000));
			    if (soft) {
				    return StringVector::AddString(result, ErrorJson(text));
			    }
			    throw InvalidInputException(text);
		    }
		    return StringVector::AddString(result, body);
	    });
}

static void ServiceExec(DataChunk &args, ExpressionState &state, Vector &result) {
	ServiceRun(args, state, result, [&](auto call) {
		BinaryExecutor::Execute<string_t, string_t, string_t>(
		    args.data[0], args.data[1], result, args.size(),
		    [&](string_t route_t, string_t body_t) { return call(route_t, body_t, false); });
	});
}

static void ServiceSoftExec(DataChunk &args, ExpressionState &state, Vector &result) {
	ServiceRun(args, state, result, [&](auto call) {
		TernaryExecutor::Execute<string_t, string_t, bool, string_t>(
		    args.data[0], args.data[1], args.data[2], result,
		    [&](string_t route_t, string_t body_t, bool soft) { return call(route_t, body_t, soft); });
	});
}

void RegisterGlossService(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("gloss_service_url",
	                          "The model service's base URL (http://host:port); empty names none, and every "
	                          "service call refuses by name",
	                          LogicalType::VARCHAR, Value(""), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("gloss_service_token", "The bearer sent on every service call; empty sends none",
	                          LogicalType::VARCHAR, Value(""), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("gloss_service_timeout", "Seconds one service call may take, whole", LogicalType::BIGINT,
	                          Value::BIGINT(600), nullptr, SetScope::GLOBAL);
	ScalarFunctionSet set("gloss_service");
	ScalarFunction service("gloss_service", {LogicalType::VARCHAR, LogicalType::JSON()}, LogicalType::JSON(),
	                       ServiceExec);
	service.SetVolatile();
	service.SetFallible();
	set.AddFunction(service);
	ScalarFunction soft("gloss_service", {LogicalType::VARCHAR, LogicalType::JSON(), LogicalType::BOOLEAN},
	                    LogicalType::JSON(), ServiceSoftExec);
	soft.SetVolatile();
	soft.SetFallible();
	set.AddFunction(soft);
	loader.RegisterFunction(set);
}

} // namespace duckdb
