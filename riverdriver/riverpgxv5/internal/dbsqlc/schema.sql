-- name: SchemaGetExpired :many
SELECT schema_name::text
FROM information_schema.schemata
WHERE schema_name LIKE @prefix
    AND schema_name < @before_name
ORDER BY schema_name;