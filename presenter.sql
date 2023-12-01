// FACT_ACCOUNT
CREATE TABLE IF NOT EXISTS `fact_account`
(
    `account_id` bigint NOT NULL,
    `user_id` bigint,
    `msisdn` varchar(255),
    `account_state_id` int,
    `account_type_id` int,
    `account_balance` float,
    `account_holding_balance` float,
    `account_available_balance` float,
    `currency_id` int,
    `created_date` datetime,
    `modified_date` datetime,
    `last_change_balance` datetime,
    `last_trans_time` datetime,
    `active_time` datetime,
    `is_master_account` int,
    `account_tier` int,
    `inviter_account` bigint,
    `wh_etl_session_key` int,
    `wh_load_ts_unix` bigint,
    `wh_source_data` varchar(255)
)

// FACT_TRANSACTION
CREATE TABLE IF NOT EXISTS `fact_transaction`
(
    `transaction_id` bigint NOT NULL,
    `from_account` bigint,
    `from_phone` varchar(255),
    `from_name` varchar(255),
    `from_party_role` bigint,
    `to_account` bigint,
    `to_phone` varchar(255),
    `to_name` varchar(255),
    `to_party_role` bigint,
    `carried_account` bigint,
    `carried_name` varchar(255),
    `carried_phone` varchar(255),
    `carried_code` varchar(255),
    `reason_id` int,
    `app_channel_id` int,
    `transaction_state_id` int,
    `transaction_type_id` int,
    `currency_id` int,
    `amount` float,
    `discount` float,
    `fee` float,
    `commission` float,
    `total_amount` float,
    `revenue_shared` float,
    `partner_code` varchar(255),
    `service_code` varchar(255),
    `date_created` datetime,
    `date_created_key` int,
    `date_modified` datetime,
    `date_expired` datetime,
    `date_deadline` datetime,
    `date_finished` datetime,
    `start_request_time` datetime,
    `end_response_time` datetime,
    `avg_process_time` int,
    `number_steps` int,
    `sum_step_process_time` float,
    `avg_step_process_time` float,
    `wh_etl_session_key` int,
    `wh_load_ts_unix` bigint,
    `wh_source_data` varchar(255)
)


--------------------------------------------------

CREATE TABLE IF NOT EXISTS `dim_kpi_criteria_code`
(
`KPI_CRITERIA_KEY` varchar(255) NOT NULL,
`ESCRIPTION` varchar(255),
`PARENT_CODE` varchar(255),
`LEVEL` bigint(20),
`NAME` varchar(255),
`NAME_LOCAL` varchar(255),
`UNIT` varchar(255),
`wh_etl_session_key` bigint(20) NOT NULL,
`wh_load_ts_unix` bigint(20) NOT NULL,
`wh_source_data` varchar(255) NOT NULL,
PRIMARY KEY (`KPI_CRITERIA_KEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


REPLACE INTO `dim_kpi_criteria_code` (
	`KPI_CRITERIA_KEY` ,
	`wh_etl_session_key` ,
	`wh_load_ts_unix` ,
	`wh_source_data`
) VALUES
('111111111', 156467215, 23215413234, "source_1"),
('222222222', 123124354, 12312312333, "source_1"),
('333333333', 678643564, 14535345345, "source_3"),
('444444444', 123154436, 12323547898, "source_2"),
('555555555', 435565212, 12425436566, "source_3"),
('666666666', 242342342, 12544657978, "source_2")



--------------------------------------------------

CREATE TABLE IF NOT EXISTS `dim_org_type`
(
`ORG_TYPE_KEY` bigint(20) NOT NULL,
`NAME` varchar(255),
`wh_etl_session_key` bigint(20) NOT NULL,
`wh_load_ts_unix` bigint(20) NOT NULL,
`wh_source_data` varchar(255) NOT NULL,
PRIMARY KEY (`ORG_TYPE_KEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


REPLACE INTO `dim_org_type` (
	`ORG_TYPE_KEY` ,
	`wh_etl_session_key` ,
	`wh_load_ts_unix` ,
	`wh_source_data`
) VALUES
(111, 156467215, 23215413234, "source_1"),
(222, 123124354, 12312312333, "source_1"),
(333, 678643564, 14535345345, "source_3");


---------------------------------------------------

CREATE TABLE IF NOT EXISTS `dim_reason`
(
`REASON_KEY` bigint(20) NOT NULL,
`REASON` varchar(255),
`REASON_GROUP_ID` bigint(20),
`wh_etl_session_key` bigint(20) NOT NULL,
`wh_load_ts_unix` bigint(20) NOT NULL,
`wh_source_data` varchar(255) NOT NULL,
PRIMARY KEY (`REASON_KEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


REPLACE INTO `dim_reason` (
	`REASON_KEY` ,
	`wh_etl_session_key` ,
	`wh_load_ts_unix` ,
	`wh_source_data`
) VALUES
(123, 156467215, 23215413234, "source_1"),
(456, 123124354, 12312312333, "source_1"),
(789, 678643564, 14535345345, "source_3")



---------------------------------------------------

CREATE TABLE IF NOT EXISTS `dim_reason_group`
(
`REASON_GROUP_KEY` bigint(20) NOT NULL,
`REASON_GROUP` varchar(255) NOT NULL,
`wh_etl_session_key` bigint(20) NOT NULL,
`wh_load_ts_unix` bigint(20) NOT NULL,
`wh_source_data` varchar(255) NOT NULL,
PRIMARY KEY (`REASON_GROUP_KEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


REPLACE INTO `dim_reason_group` (
	`REASON_GROUP_KEY` ,
	`REASON_GROUP` ,
	`wh_etl_session_key` ,
	`wh_load_ts_unix` ,
	`wh_source_data`
) VALUES
(1, 'group_1' , 156467215, 23215413234, "source_1"),
(2, 'group_2' , 123124354, 12312312333, "source_2"),
(3, 'group_3' , 678643564, 14535345345, "source_3")


-----------------------------------------------------

CREATE TABLE IF NOT EXISTS `dim_strange_behaviour`
(
`BEHAVIOUR_KEY` bigint(20) NOT NULL,
`DESCRIPTION` varchar(255) NOT NULL,
`EVALUATION_FREQUENCY` varchar(255),
`wh_etl_session_key` bigint(20) NOT NULL,
`wh_load_ts_unix` bigint(20) NOT NULL,
`wh_source_data` varchar(255) NOT NULL,
PRIMARY KEY (`BEHAVIOUR_KEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;



REPLACE INTO `dim_strange_behaviour` (
	`BEHAVIOUR_KEY` ,
	`DESCRIPTION` ,
	`wh_etl_session_key` ,
	`wh_load_ts_unix` ,
	`wh_source_data`
) VALUES
(1111, 'des_1' , 156467215, 23215413234, "source_1"),
(2222, 'des_2' , 123124354, 12312312333, "source_2"),
(3333, 'des_3' , 678643564, 14535345345, "source_3")



------------------------------------------------------


CREATE TABLE IF NOT EXISTS `dim_subscriber_type`
(
`SUBSCRIBER_KEY` bigint(20) NOT NULL,
`SUBSCRIBER` varchar(255) NOT NULL,
`wh_etl_session_key` bigint(20) NOT NULL,
`wh_load_ts_unix` bigint(20) NOT NULL,
`wh_source_data` varchar(255) NOT NULL,
PRIMARY KEY (`SUBSCRIBER_KEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;



REPLACE INTO `dim_subscriber_type` (
	`SUBSCRIBER_KEY` ,
	`SUBSCRIBER` ,
	`wh_etl_session_key` ,
	`wh_load_ts_unix` ,
	`wh_source_data`
) VALUES
(1111, 'sub_1' , 156467215, 23215413234, "source_1"),
(2222, 'sub_2' , 123124354, 12312312333, "source_2"),
(3333, 'sub_3' , 678643564, 14535345345, "source_3");



-------------------------------------------------------

CREATE TABLE IF NOT EXISTS `dim_telecom_com`
(
`COMPANY_KEY` bigint(20) NOT NULL,
`NAME` varchar(255) NOT NULL,
`wh_etl_session_key` bigint(20) NOT NULL,
`wh_load_ts_unix` bigint(20) NOT NULL,
`wh_source_data` varchar(255) NOT NULL,
PRIMARY KEY (`COMPANY_KEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


REPLACE INTO `dim_telecom_com` (
	`COMPANY_KEY` ,
	`NAME` ,
	`wh_etl_session_key` ,
	`wh_load_ts_unix` ,
	`wh_source_data`
) VALUES
(1, 'company_a' , 156467215, 23215413234, "source_1"),
(2, 'company_b' , 123124354, 12312312333, "source_2"),
(3, 'company_c' , 678643564, 14535345345, "source_3");


-------------------------------------------------------


CREATE TABLE IF NOT EXISTS `dim_telecom_service_type`
(
`SERVICE_TYPE_KEY` bigint(20) NOT NULL,
`SERVICE_TYPE` varchar(255),
`wh_etl_session_key` bigint(20) NOT NULL,
`wh_load_ts_unix` bigint(20) NOT NULL,
`wh_source_data` varchar(255) NOT NULL,
PRIMARY KEY (`SERVICE_TYPE_KEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


REPLACE INTO `dim_telecom_service_type` (
	`SERVICE_TYPE_KEY` ,
	`wh_etl_session_key` ,
	`wh_load_ts_unix` ,
	`wh_source_data`
) VALUES
(1111, 156467215, 23215413234, "source_1"),
(2222, 123124354, 12312312333, "source_2"),
(3333, 678643564, 14535345345, "source_3");


-------------------------------------------------------


CREATE TABLE IF NOT EXISTS `dim_transaction_state`
(
`TRANSACTION_STATE_ID` bigint(20) NOT NULL,
`TRANSACTION_STATE` varchar(255),
`wh_etl_session_key` bigint(20) NOT NULL,
`wh_load_ts_unix` bigint(20) NOT NULL,
`wh_source_data` varchar(255) NOT NULL,
PRIMARY KEY (`TRANSACTION_STATE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


REPLACE INTO `dim_transaction_state` (
	`TRANSACTION_STATE_ID` ,
	`wh_etl_session_key` ,
	`wh_load_ts_unix` ,
	`wh_source_data`
) VALUES
(11111, 156467215, 23215413234, "source_1"),
(22222, 123124354, 12312312333, "source_2"),
(33333, 678643564, 14535345345, "source_3");


-------------------------------------------------------


CREATE TABLE IF NOT EXISTS `dim_transaction_type`
(
`TRANSACTION_TYPE_KEY` bigint(20) NOT NULL,
`NAME` varchar(255) NOT NULL,
`wh_etl_session_key` bigint(20) NOT NULL,
`wh_load_ts_unix` bigint(20) NOT NULL,
`wh_source_data` varchar(255) NOT NULL,
PRIMARY KEY (`TRANSACTION_TYPE_KEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


REPLACE INTO `dim_transaction_type` (
	`TRANSACTION_TYPE_KEY` ,
	`NAME` ,
	`wh_etl_session_key` ,
	`wh_load_ts_unix` ,
	`wh_source_data`
) VALUES
(11111, 'transaction_a' , 156467215, 23215413234, "source_1"),
(22222, 'transaction_b' , 123124354, 12312312333, "source_2"),
(33333, 'transaction_c' , 678643564, 14535345345, "source_3");


-------------------------------------------------------


CREATE TABLE IF NOT EXISTS `dim_user`
(
`SURROGATE_KEY` bigint(20) NOT NULL,
`USER_ID` bigint(20) NOT NULL,
`MSISDN` varchar(255),
`TEL_COM_ID` bigint(20),
`CREATED_DATE` date,
`MODIFIED_DATE` date,
`PARENT_ID` bigint(20),
`COMMISSION_FORWARD` bigint(20),
`SUBSCRIBER_TYPE` bigint(20),
`TELECOM_SERVICE_TYPE` bigint(20),
`IDENTITY_NUMBER` varchar(20),
`IDENTITY_DOCUMENT_TYPE` bigint(20),
`IDENTITY_DOCUMENT_ISSUED_DATE` date,
`IDENTITY_DOCUMENT_EXPIRED_DATE` date,
`AREA_CODE` bigint(20),
`CELL_ID` varchar(20),
`BIRTHDAY` date,
`NATIONALITY` varchar(20),
`GENDER` bigint(20),
`ADDRESS` varchar(255),
`IS_ORGANIZATION` bigint(20),
`TRADING_NAME` varchar(255),
`wh_etl_session_key` bigint(20) NOT NULL,
`wh_load_ts_unix` bigint(20) NOT NULL,
`wh_source_data` varchar(255) NOT NULL,
PRIMARY KEY (`SURROGATE_KEY`, `USER_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


REPLACE INTO `dim_user` (
	`SURROGATE_KEY` ,
	`USER_ID` ,
	`TEL_COM_ID`,
	`CREATED_DATE` ,
	`MODIFIED_DATE` ,
	`AREA_CODE`,
	`CELL_ID` ,
	`BIRTHDAY` ,
	`GENDER` ,
	`wh_etl_session_key` ,
	`wh_load_ts_unix` ,
	`wh_source_data`
) VALUES
(11111, 1 , 12312 , '2023-11-01' , '2023-11-01' , 29, '123' , '2000-01-01' , 1, 156467215, 23215413234, "source_1"),
(22222, 2 , 21312 , '2023-11-01' , '2023-11-01' , 39, '111' , '1999-01-01' , 0, 123124354, 12312312333, "source_2"),
(33333, 3 , 12312 , '2023-11-01' , '2023-11-01' , 21, '232' , '1998-01-01' , 1, 618643564, 14535345345, "source_3");


-------------------------------------------------------


CREATE TABLE IF NOT EXISTS `dim_user_role`
(
`USER_ROLE_ID` bigint(20) NOT NULL,
`ROLE_NAME` varchar(255) ,
`USER_ROLE_GROUP_ID` bigint(20) NOT NULL,
`wh_etl_session_key` bigint(20) NOT NULL,
`wh_load_ts_unix` bigint(20) NOT NULL,
`wh_source_data` varchar(255) NOT NULL,
PRIMARY KEY (`USER_ROLE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


REPLACE INTO `dim_user_role` (
	`USER_ROLE_ID` ,
	`USER_ROLE_GROUP_ID` ,
	`wh_etl_session_key` ,
	`wh_load_ts_unix` ,
	`wh_source_data`
) VALUES
(11111, 123 , 156467215, 23215413234, "source_1"),
(22222, 232 , 123124354, 12312312333, "source_2"),
(33333, 231 , 678643564, 14535345345, "source_3");


-------------------------------------------------------


CREATE TABLE IF NOT EXISTS `dim_user_role_group`
(
`USER_ROLE_GROUP_KEY` bigint(20) NOT NULL,
`GROUP_NAME` varchar(255) ,
`wh_etl_session_key` bigint(20) NOT NULL,
`wh_load_ts_unix` bigint(20) NOT NULL,
`wh_source_data` varchar(255) NOT NULL,
PRIMARY KEY (`USER_ROLE_GROUP_KEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


REPLACE INTO `dim_user_role_group` (
	`USER_ROLE_GROUP_KEY` ,
	`GROUP_NAME` ,
	`wh_etl_session_key` ,
	`wh_load_ts_unix` ,
	`wh_source_data`
) VALUES
(11111, 'group 1' , 156467215, 23215413234, "source_1"),
(22222, null , 123124354, 12312312333, "source_2"),
(33333, 'group_2' , 678643564, 14535345345, "source_3");
