{% snapshot concur_expense_allocations.sql %} 

{{ 
   config( 
      target_schema='intapp_prod_bidw', 
      unique_key='id',
      strategy='check', 
      check_cols=['accountcode1', 'accountcode2', 'custom10_value', 'custom11_value'], 
      post_hook='update intapp_prod_bidw.concur_expense_allocations set dw_delete_flag=true where dbt_valid_to is not null;' 
) 
}} 

select *, current_timestamp as dw_md_sys_modified_date, false as dw_delete_flag  
from intapp_prod_stage.stg_concur_expense_allocations 

{% endsnapshot %}
