# Setting up DBT with Redshift:


- Data Build Tool (DBT) is an open-source command-line tool developed in Python that assists analysts and data engineers in transforming data within data warehouses.
- This tool expedites the data transformation and integration processes within data warehouses. 
- DBT facilitates the deployment of models to data warehouses by composing SQL queries in the background.
- By utilizing DBT, we leverage materializations to transform data after extraction, thereby transitioning from the Extract, Transform, Load (ETL) approach to the Extract, Load, Transform (ELT) approach.   

   <img width="800" height="300" alt="image" src="https://github.com/user-attachments/assets/0b970937-313b-45b6-beda-b064054518a5" />

 
**DBT Materialization**: DBT uses different materialization strategies to transform the data in an automated way.The following materialization options can be configured: 

- View - Creates a view in datawrehouse for the defined model in DBT

- Table - Creates a table in datawrehouse for the defined model in DBT

- Incremental - This will automatically creates an SCD type1 transformation between Stage and Target tables

- Ephemeral - Builds a temporary table which will be created  only for refrence by other models 

 

**DBT Snapshots**: 

- To define SCD type 2 transformation, DBT uses different strategy called DBT snapshots.

- The snapshot models are defined in DBT under snapshot folder. 

- To apply SCD Type2 on Target table, the table either should not exist so DBT creates while running the model or we add the folloing to the existing table.
       - dbt_scd_id: MD5 value 
       - dbt_updated_at : TImestamp field used transformation strategy 
       - dbt_valid_from : Represents the row is active since. 
       - dbt_valid_to : End timestamp of the record.  

**DBT architecture**:

- To setup DBT, we install DBT in a Python environment and install necessary adapters to connect to the database.
- Install dbt core  - https://docs.getdbt.com/docs/core/installation-overview 
- Once the DBT is installed on  the server, the following files and folders are created by DBT.
   - .dbt folder is created at the root 
   - profiles.yml file is created in the .dbt file where the database connections exist 
   - Create a dbt project folder using ‘dbt init {project_name}’
   - Project folder contains:
   - dbt_project.yml file that contains the configurations for the specific project 
   - models folder where the actual dbt models are defined in a .sql file format 
   - macros folder where additional jinja macros are created based on the dbt project need
   - snapshots folder that contain dbt models for scd type2 
   - targets folder that contains compiled code of the models defined 

   <img width="530" height="360" alt="image" src="https://github.com/user-attachments/assets/2720a40d-c72a-41c5-bef8-c64327c6147a" />


**Git repository setup**:
- Setup GIT repository that will sync the models from repository to the server.

   <img width="243" height="157" alt="image" src="https://github.com/user-attachments/assets/e33eae62-fcb6-4d5f-b549-2a1ce2039de9" />
 
**DBT Model creaation automation**:
- Create a python script that can help with creating your standarized DBT models and snapshots.
- Refer to DBT.py defined in the repo for the automated py script.
- Steps to create model using DBT.py:
    - Open a python shell on the server 
    - Navigate to the folder where the script is saved.
         - from DBT import dbt_model
         - dbt = dbt_model()
         - Create Snapshot executing the following command with parameters:
              - The snapshot model will create the target table with 4 key columns to track history.Refer to DBT snapshot section for more details [DBT model snapshot](#**DBT model template for snapshots**)
              - dbt.create_snapshot_model(target_table, unique_key, strategy,colName)
              - target_table: Target table that we are loading by the process
              - unique_key: Primary key of the table ( Concat fields in case of composite key 'field_1 || field_2 || field_3')
              - Strategy: The strategy can be ‘check’ to check the list of columns for changes or ‘timestamp’ to consider timestamp field to identify changes from the stage table
              - colName: the timestamp filed column name or the list of columns to track for changes

         - Create Incremental model as follows
              - The incremental process will maintain only active records in the table by the unique key using upsert operations. 
              - dbt.create_incr_model(target_table, unique_key)
              - target_table:Target table that we are loading by the process
              - unique_key: Primary key of the table ( Concat fields in case of composite key 'field_1 || field_2 || field_3')

**Note**: To create models that can override the schema of the source and target tables, we can refer to the overwrite functions in DBT.py

**DBT Model execution in Python**:

 - In each of the data pipelines, we leverage the DBT class to execute the dbt models created above as follows:
 - Import the DBT script as library in the Python code
    - from DBT import dbt_model
 - Create the dbt instance using dbt_model class 
    - dbt = dbt_model()
 - To execute the snapshot model in the python process:
   - dbt.execute_model('snapshot', bidw_table )
 - To execute the incremental model in the python process:
   - dbt.execute_model('run', bidw_table )
 - To execute full refresh of incremental model that recreates and reloads the target table:
   - dbt.execute_full_model('run', bidw_table )
 - Scan the logs for errors and raise exception on failure of DBT model
   - dbt.scan_log_for_error(bidw_table)

**DBT model template for snapshots**:

 - The template below serves as a DBT model to create daily or weekly templates of views or tables in Redshift.
 - This is an ‘incremental’ model that creates snapshottable as configured.
 - The prehook with incremental check executes the prehook query only if the table exists. 

 <img width="800" height="400" alt="image" src="https://github.com/user-attachments/assets/6dd2a414-bdcb-4117-810d-8ea8c25e12ba" />

    
