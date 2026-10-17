{{define "container"}}(JSON_TYPE({{.source}}) = '{{.kind}}' AND {{.children}}){{end}}
{{define "member"}}(SELECT 1 FROM JSON_TABLE(JSON_EXTRACT({{.source}}, {{.param}}), '$' COLUMNS(value JSON PATH '$')) AS {{.alias}} WHERE {{.predicate}} LIMIT 1) IS NOT NULL{{end}}
{{define "element"}}(SELECT 1 FROM JSON_TABLE({{.source}}, '$[*]' COLUMNS(value JSON PATH '$')) AS {{.alias}} WHERE {{.predicate}} LIMIT 1) IS NOT NULL{{end}}
{{define "null"}}({{.source}} IS NULL OR JSON_TYPE({{.source}}) = 'NULL'){{end}}
{{define "scalar"}}(JSON_TYPE({{.source}}) NOT IN ('ARRAY', 'OBJECT') AND JSON_CONTAINS({{.source}}, CAST({{.param}} AS JSON))){{end}}
