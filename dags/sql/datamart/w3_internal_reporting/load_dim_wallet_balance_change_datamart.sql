SELECT 
    , balance_change_id
    , wallet_id
    , transaction_id
    , request_log_id
    , trans_accounting_id
    , trans_type_id
    , amount
    , currency_code
    , date_created
    , content
    , `status`
    , trans_type
FROM w3_system_accounting_datawarehouse.wallet_balance_change;