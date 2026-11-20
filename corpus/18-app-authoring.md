# Data app authoring

A data app is a set of pages, queries and chart specs. The language has
no statement that declares an app or its parts. This is by design:
glossql describes data, and SPEC.md §9 holds no open question on it.
The two forms below, one statement per part and one statement for the
whole app, fail to parse.

```glossql-gap
DECLARE APP model SET (title: 'World model');

DECLARE FRAME travels ON model AS $$
SELECT q.property AS metric, '?metric=' || q.property AS link
FROM GLOSSARY(all => true) q
WHERE q.kind = 'query'
  AND strpos(json_get_str(q.body, 'sql'),
             arrow_cast(substr($subject, strpos($subject, '.') + 1), 'Utf8')) > 0
ORDER BY q.property
$$;

DECLARE PAGE index ON model AS $$
{% extends "shell.html" %}
… 16,832 bytes of tera, carried opaque …
$$;

DECLARE SPEC ranges ON model AS $$
{ "$schema": "https://vega.github.io/schema/vega-lite/v6.json", … }
$$;

PUBLISH APP model;
```

```glossql-gap
DECLARE APP model AS $$
title = "World model"

[pages.index]
… the whole directory in one body …

[frames.travels]
… every frame inline …
$$;
```

A release of an app is an annotation, which the language does express:

```glossql
GLOSS app_release ON model AS $${"note": "first cut"}$$;
GLOSS app_release ON model AS $${"at": "2026-09-23T09:00:00.000Z"}$$;
```
