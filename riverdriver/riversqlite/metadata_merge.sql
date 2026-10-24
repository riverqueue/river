-- Apply each supplied top-level value with jsonb_set, preserving JSON nulls.
-- Materialize the update list once; reevaluating it per step is costly on Turso.
(WITH RECURSIVE
    original(doc) AS (
        SELECT CASE WHEN typeof(metadata) = 'text' AND NOT json_valid(metadata)
                    THEN '{}' ELSE metadata END
    ),
    updates AS MATERIALIZED (
        SELECT row_number() OVER () AS num, fullkey AS path,
               @metadata_merge_updates -> fullkey AS value
        FROM json_each(@metadata_merge_updates)
    ),
    merged(num, doc) AS (
        SELECT 0, doc FROM original
        UNION ALL
        SELECT updates.num, jsonb_set(merged.doc, updates.path, jsonb(updates.value))
        FROM merged JOIN updates ON updates.num = merged.num + 1
    )
    SELECT CASE json_type(original.doc)
        WHEN 'object' THEN merged.doc
        WHEN 'array' THEN jsonb(json_insert(json(original.doc), '$[#]', json(@metadata_merge_updates)))
        ELSE jsonb_array(json(original.doc), jsonb(@metadata_merge_updates))
    END
    FROM original, merged WHERE num = (SELECT count(*) FROM updates))
