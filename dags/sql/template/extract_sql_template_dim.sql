SELECT
  {{params.columns}}
FROM {{params.table_name}}
{{params.join}}
{{params.where_condition}}
ORDER BY {{params.order_by}}