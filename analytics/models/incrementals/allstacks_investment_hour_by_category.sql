{{
    config(
        schema='intapp_prod_bidw',
        materialized='incremental',
        unique_key='project_id || x_axis_time',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

select
    distinct
   *,
   current_timestamp as dw_md_sys_modified_date,
   false as dw_delete_flag
from intapp_prod_stage.stg_allstacks_investment_hour_by_category
