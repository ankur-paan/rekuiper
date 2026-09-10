SET 'execution.runtime-mode' = 'BATCH';

CREATE TABLE source_stream (
    id STRING,
    temp DOUBLE
) WITH (
    'connector' = 'filesystem',
    'path' = '/data/telemetry_500k.jsonl',
    'format' = 'json'
);

CREATE TABLE sink_stream (
    id STRING,
    temp_f DOUBLE
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO sink_stream
SELECT id, temp * 1.8 + 32.0 AS temp_f
FROM source_stream
WHERE temp > 20.0;
