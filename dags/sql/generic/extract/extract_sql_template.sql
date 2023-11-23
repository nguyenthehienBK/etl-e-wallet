SELECT
  {{params.columns}}
FROM {{params.table_name}} WITH (NOLOCK)
{{params.join}}
{{params.where_condition}}
ORDER BY {{params.order_by}}