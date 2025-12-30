{{
    config(
        materialized='incremental',
        unique_key = 'snapshot_date',
        on_schema_change = 'sync_all_columns',
        pre_hook='{% if is_incremental() %} DELETE FROM {{ this }} WHERE snapshot_date = CURRENT_DATE {% endif %}'
    )
}}

select *, current_date as snapshot_date from intapp_prod_data_model.dim_employee
