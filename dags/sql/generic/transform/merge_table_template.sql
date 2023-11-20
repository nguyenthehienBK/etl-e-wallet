MERGE INTO '{{params.target_dataset}}.{{params.target_table}}' T
USING (
    SELECT
        *
    FROM '{{params.target_dataset}}.{{params.source_table}}'
) AS S
ON T.key = S.key
WHEN MATCHED THEN UPDATE SET
T.column = S.column
WHEN NOT MATCHED THEN INSERT
(T.column) VALUES (S.column);