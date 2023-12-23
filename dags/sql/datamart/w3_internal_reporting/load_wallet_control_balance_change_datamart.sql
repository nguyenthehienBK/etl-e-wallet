SELECT 
    wallet_control_balance_change_id
    , wallet_control_id
    , transaction_id
    , trans_type_id
    , amount
    , currency_code
    , direction
    , before_balance
    , after_balance
    , date_created
FROM w3_system_accounting_datawarehouse.wallet_control_balance_change
