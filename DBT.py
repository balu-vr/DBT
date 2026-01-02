import os
from collections import deque


class dbt_model():

    def __init__(self):
        self.dbt_path = r"/home/ec2-user/DataEngineeringDBT/dbt_projects"

    def create_snapshot_model(self, target_table, unique_key, strategy, colName):
        print("calling function..")
        if not os.path.isfile(f"{self.dbt_path}/snapshots/{target_table}.sql"):
            target_schema = 'bidw'
            stage_schema = 'stage'
            if strategy =="check":
                with open(f"{self.dbt_path}/snapshots/{target_table}.sql", "w") as f:
                    f.write("{% snapshot "+target_table+".sql %} \n\n")
                    f.write("{{ \n")
                    f.write("   config( \n")
                    f.write(f"      target_schema='{target_schema}', \n")
                    f.write(f"      unique_key='{unique_key}', \n")
                    f.write(f"      strategy='check', \n")
                    f.write(f"      check_cols={colName}, \n")
                    f.write(f"      post_hook='update {target_schema}.{target_table} set dw_delete_flag=true where dbt_valid_to is not null;' \n")
                    f.write(") \n")
                    f.write("}} \n\n")
                    f.write(f"select *, current_timestamp as dw_md_sys_modified_date, false as dw_delete_flag  \n")
                    f.write(f"from {stage_schema}.stg_{target_table} \n\n")
                    f.write("{% endsnapshot %}")


            else:
                with open(f"{self.dbt_path}/snapshots/{target_table}.sql", "w") as f:
                    f.write("{% snapshot "+target_table+".sql %} \n\n")
                    f.write("{{ \n")
                    f.write("   config( \n")
                    f.write(f"      target_schema='{target_schema}', \n")
                    f.write(f"      unique_key='{unique_key}', \n")
                    f.write(f"      strategy='timestamp', \n")
                    f.write(f"      updated_at='{colName}', \n")
                    f.write(f"      post_hook='update {target_schema}.{target_table} set dw_delete_flag=true where dbt_valid_to is not null;' \n")
                    f.write(") \n")
                    f.write("}} \n\n")
                    f.write(f"select *, current_timestamp as dw_md_sys_modified_date, false as dw_delete_flag  \n")
                    f.write(f"from {stage_schema}.stg_{target_table} \n\n")
                    f.write("{% endsnapshot %}")


    def create_incr_model(self, target_table, unique_key):
        if not os.path.isfile(f"{self.dbt_path}/models/Incrementals/{target_table}.sql"):
            target_schema = 'bidw'
            stage_schema = 'stage'
            with open(f"{self.dbt_path}/models/Incrementals/{target_table}.sql", "w") as f:
                    f.write("{{ \n")
                    f.write("   config( \n")
                    f.write(f"      materialized='incremental', \n")
                    f.write(f"      schema='{target_schema}', \n")
                    f.write(f"      unique_key='{unique_key}', \n")
                    f.write(f"      on_schema_change = 'sync_all_columns'  \n")
                    f.write(") \n")
                    f.write("}} \n\n")
                    f.write(f"select *, current_timestamp as dw_md_sys_modified_date, false as dw_delete_flag  \n")
                    f.write(f"from {stage_schema}.stg_{target_table} \n\n")
    
    def overwrite_snapshot_model(self, stage_schema, stage_table, target_schema, target_table, unique_key, strategy, colName):
        
        #if not os.path.isfile(f"{self.dbt_path}/snapshots/{target_table}.sql"):

            if strategy =="check":
                with open(f"{self.dbt_path}/snapshots/{target_table}.sql", "w") as f:
                    f.write("{% snapshot "+target_table+".sql %} \n\n")
                    f.write("{{ \n")
                    f.write("   config( \n")
                    f.write(f"      target_schema='{target_schema}', \n")
                    f.write(f"      unique_key='{unique_key}', \n")
                    f.write(f"      strategy='check', \n")
                    f.write(f"      check_cols={colName}, \n")
                    f.write(f"      post_hook='update {target_schema}.{target_table} set dw_delete_flag=true where dbt_valid_to is not null;' \n")
                    f.write(") \n")
                    f.write("}} \n\n")
                    f.write(f"select *, current_timestamp as dw_md_sys_modified_date, false as dw_delete_flag  \n")
                    f.write(f"from {stage_schema}.{stage_table} \n\n")
                    f.write("{% endsnapshot %}")


            else:
                with open(f"{self.dbt_path}/snapshots/{target_table}.sql", "w") as f:
                    f.write("{% snapshot "+target_table+".sql %} \n\n")
                    f.write("{{ \n")
                    f.write("   config( \n")
                    f.write(f"      target_schema='{target_schema}', \n")
                    f.write(f"      unique_key='{unique_key}', \n")
                    f.write(f"      strategy='timestamp', \n")
                    f.write(f"      updated_at='{colName}', \n")
                    f.write(f"      post_hook='update {target_schema}.{target_table} set dw_delete_flag=true where dbt_valid_to is not null;' \n")
                    f.write(") \n")
                    f.write("}} \n\n")
                    f.write(f"select *, current_timestamp as dw_md_sys_modified_date, false as dw_delete_flag  \n")
                    f.write(f"from {stage_schema}.{stage_table} \n\n")
                    f.write("{% endsnapshot %}")

    def overwrite_incr_model(self, stage_schema, target_schema, target_table, unique_key):
        #if not os.path.isfile(f"{self.dbt_path}/models/Incrementals/{target_table}.sql"):
            with open(f"{self.dbt_path}/models/Incrementals/{target_table}.sql", "w") as f:
                    f.write("{{ \n")
                    f.write("   config( \n")
                    f.write(f"      materialized='incremental', \n")
                    f.write(f"      schema='{target_schema}', \n")
                    f.write(f"      unique_key='{unique_key}', \n")
                    f.write(f"      on_schema_change = 'sync_all_columns'  \n")
                    f.write(") \n")
                    f.write("}} \n\n")
                    f.write(f"select *, current_timestamp as dw_md_sys_modified_date, false as dw_delete_flag  \n")
                    f.write(f"from {stage_schema}.stg_{target_table} \n\n")
    
    def execute_model(self, dbt_operation, target_table):
        os.system(f'cd {self.dbt_path} ;' \
                    f'dbt {dbt_operation} --select {target_table} --log-format-file json --log-path logs/{target_table};' )
        
    def execute_full_model(self, dbt_operation, target_table):
        os.system(f'cd {self.dbt_path} ;' \
                    f'dbt {dbt_operation} --full-refresh --select {target_table} --log-format-file json --log-path logs/{target_table} ;' )
        
    def scan_log_for_error(self,target_table):
        log_file_path = f"{self.dbt_path}/logs/{target_table}/dbt.log"
        
        print("checking the logs in ", log_file_path)
        try:
            # Read the last 10 lines of the log file
            last_lines = []
            with open(log_file_path, 'r') as file:
                last_lines = list(deque(file, 10))
            
            #print("last lines:", last_lines)
            # Check each line for the keyword 'failed'
            for line in last_lines:
                if 'failed' in line.lower():
                    raise Exception("Process failed: 'failed' keyword found in log.")
            
            print("No failure message found in log. Process completed successfully.")
        
        except Exception as e:
            print(f"Error: {e}")
            raise
        
    
    
