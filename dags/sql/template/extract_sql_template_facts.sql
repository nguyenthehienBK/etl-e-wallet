SELECT
  {{params.columns}}
FROM {{params.table_name}} WITH (NOLOCK)
{{params.join}}
WHERE {{params.timestamp}} BETWEEN  {{params.extract_from}} AND {{params.extract_to}}
ORDER BY {{params.order_by}}