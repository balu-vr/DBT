{% snapshot jira_hosted_histories.sql %} 

{{ 
   config( 
      target_schema='intapp_prod_bidw', 
      unique_key='histories_id || jira_field',
      strategy='timestamp', 
      updated_at='whenchanged',
      post_hook='update intapp_prod_bidw.jira_hosted_histories set dw_delete_flag=true where dbt_valid_to is not null;' 
)
}} 

select *, current_timestamp as dw_md_sys_modified_date, false as dw_delete_flag  
from intapp_prod_stage.stg_jira_hosted_histories

{% endsnapshot %}
