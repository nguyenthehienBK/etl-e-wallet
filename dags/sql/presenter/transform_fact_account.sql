SELECT
    wallet.wallet_id AS account_id,
    wallet.customer_id AS user_id,
    users.phone_number AS msisdn,
    wallet.wallet_state_id AS account_state_id,
    wallet.wallet_type_id AS account_type_id,
    wallet_balance.balance AS account_balance,
    wallet_balance.holding_balance AS account_holding_balance,
    wallet_balance.available_balance AS account_available_balance,
    wallet.currency_code AS currency_id,
    wallet.created_date,
    wallet.modified_date,
    wallet_balance.date_modified AS last_change_balance,
    MAX(wallet.balance_change.date_created) AS last_trans_time,
    wallet.active_time,
    NULL AS is_master_account,
    NULL AS account_tier,
    NULL AS inviter_account,
    NULL AS wh_etl_session_key,
    CURRENT_TIMESTAMP() AS wh_load_ts_unix,
    NULL AS wh_source_data
FROM w3_system_accounting.wallet wallet
JOIN w3_system_accounting.wallet_balance wallet_balance ON wallet.wallet_id = wallet_balance.wallet_id
JOIN w3_system_accounting.wallet_balance_change wallet_balance_change on wallet.wallet_id = wallet_balance_change.wallet_id
JOIN w3_core_uaa.users users ON wallet.customer_id = users.id