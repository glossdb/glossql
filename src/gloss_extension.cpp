#define DUCKDB_EXTENSION_MAIN

#include "gloss_extension.hpp"
#include "gloss_grammar.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// The grammar is registered on the database and activated per connection,
	// which PRAGMA gloss does beside making the store
	GrammarExtension::Register(loader.GetDatabaseInstance(), make_shared_ptr<GlossGrammar>());
	RegisterGlossStore(loader);
	RegisterGlossRunner(loader);
	RegisterGlossSchema(loader);
	RegisterGlossLineage(loader);
	RegisterGlossService(loader);
}

void GlossExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string GlossExtension::Name() {
	return "gloss";
}

std::string GlossExtension::Version() const {
#ifdef EXT_VERSION_GLOSS
	return EXT_VERSION_GLOSS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(gloss, loader) {
	duckdb::LoadInternal(loader);
}
}
