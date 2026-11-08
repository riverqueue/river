INSERT INTO /* TEMPLATE: schema */river_notification (payload, topic)
SELECT batch.payload, config.topic
FROM JSON_TABLE(?, '$[*]' COLUMNS (
    ordinal FOR ORDINALITY,
    payload LONGTEXT CHARACTER SET utf8mb4 PATH '$' ERROR ON ERROR
)) AS batch
CROSS JOIN (SELECT ? AS topic) AS config
ORDER BY batch.ordinal;
