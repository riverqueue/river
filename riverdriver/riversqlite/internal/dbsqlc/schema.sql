-- Built-in table that sqlc doesn't know about.
CREATE TABLE sqlite_master (
    type text,
    name text,
    tbl_name text,
    rootpage integer,
    sql text
);

-- name: IndexExists :one
SELECT EXISTS (
    SELECT 1
    FROM /* TEMPLATE: schema */sqlite_master WHERE type = 'index' AND name = cast(@index AS text)
);

-- name: TableExists :one
SELECT EXISTS (
    SELECT 1
    FROM /* TEMPLATE: schema */sqlite_master WHERE type = 'table' AND name = cast(@table AS text)
);
