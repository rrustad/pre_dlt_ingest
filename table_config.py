table_configs = [
  {
    'workflow': 1,
    'target_db': 'pre_dlt_ingest_bz',
    'table_name': f'pre_dlt_ingest_bz.encounters',
    'lifecycle':'bronze',
    'input_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/landed/encounters',
    'schema_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/schemas/bronze/encounters',
    'checkpoint_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/checkpoints/bronze/encounters',
    'output_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/output/bronze/encounters',
    # make empty list if no need for z ordering
    'zorderby':[]
  },
    {
    'workflow':2,
    'target_db': 'pre_dlt_ingest_sv',
    #TODO db_name
    'table_name': f'pre_dlt_ingest_sv.encounters',
    'lifecycle':'silver',
    'input_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/output/bronze/encounters',
    'schema_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/schemas/silver/encounters',
    'checkpoint_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/checkpoints/silver/encounters',
    'output_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/output/silver/encounters',

    # make empty list if no need for z ordering
    'zorderby':['id'],
    'merge_key':['id']
  },
  {
    'workflow': 1,
    'target_db': 'pre_dlt_ingest_bz',
    'table_name': f'pre_dlt_ingest_bz.patients',
    'lifecycle':'bronze',
    'input_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/landed/patients',
    'schema_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/schemas/bronze/patients',
    'checkpoint_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/checkpoints/bronze/patients',
    'output_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/output/bronze/patients',
    # make empty list if no need for z ordering
    'zorderby':[]
  },
    {
    'workflow':2,
    'target_db': 'pre_dlt_ingest_sv',
    #TODO db_name
    'table_name': f'pre_dlt_ingest_sv.patients',
    'lifecycle':'silver',
    'input_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/output/bronze/patients',
    'schema_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/schemas/silver/patients',
    'checkpoint_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/checkpoints/silver/patients',
    'output_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/output/silver/patients',

    # make empty list if no need for z ordering
    'zorderby':['id'],
    'merge_key':['id']
  },
  {
    'workflow': 1,
    'target_db': 'pre_dlt_ingest_bz',
    'table_name': f'pre_dlt_ingest_bz.procedures',
    'lifecycle':'bronze',
    'input_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/landed/procedures',
    'schema_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/schemas/bronze/procedures',
    'checkpoint_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/checkpoints/bronze/procedures',
    'output_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/output/bronze/procedures',
    # make empty list if no need for z ordering
    'zorderby':[]
  },
    {
    'workflow':2,
    'target_db': 'pre_dlt_ingest_sv',
    #TODO db_name
    'table_name': f'pre_dlt_ingest_sv.procedures',
    'lifecycle':'silver',
    'input_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/output/bronze/procedures',
    'schema_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/schemas/silver/procedures',
    'checkpoint_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/checkpoints/silver/procedures',
    'output_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/output/silver/procedures',

    # make empty list if no need for z ordering
    'zorderby':[],
    'merge_key':['encounter', 'code']
  },
  {
    'workflow': 1,
    'target_db': 'pre_dlt_ingest_bz',
    'table_name': f'pre_dlt_ingest_bz.conditions',
    'lifecycle':'bronze',
    'input_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/landed/conditions',
    'schema_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/schemas/bronze/conditions',
    'checkpoint_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/checkpoints/bronze/conditions',
    'output_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/output/bronze/conditions',
    # make empty list if no need for z ordering
    'zorderby':[]
  },
    {
    'workflow':2,
    'target_db': 'pre_dlt_ingest_sv',
    #TODO db_name
    'table_name': f'pre_dlt_ingest_sv.conditions',
    'lifecycle':'silver',
    'input_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/output/bronze/conditions',
    'schema_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/schemas/silver/conditions',
    'checkpoint_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/checkpoints/silver/conditions',
    'output_path': 'dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/output/silver/conditions',

    # make empty list if no need for z ordering
    'zorderby':[],
    'merge_key':['encounter', 'code']
  },
]