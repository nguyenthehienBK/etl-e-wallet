SELECT id,
    description,
    role_type_id,
    status,
    tier_code,
    tier_name,
    cast(created_at as string),
    created_by,
    cast(updated_at as string),
    updated_by,
    cast(deleted_at as string),
    deleted_by
FROM w3_core_mdm_staging.tiers
