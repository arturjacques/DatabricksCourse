# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Funções

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##SETUP

# COMMAND ----------

from functions.data_creation import create_compras_de_usuario

df = create_compras_de_usuario(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataframe In Dataframe Out
# MAGIC
# MAGIC <p>Esse tipo de função recebe um DataFrame e retorna um DataFrame possibilitando ser possível compartimentalizar ela e testar de forma adequada.</p>

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql import DataFrame
from uuid import uuid4

def add_metadata_table(df: DataFrame, data_source)-> DataFrame:
    return (df
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("batch_id", lit(str(uuid4())))
        .withColumn("input_file_name", input_file_name())
        .withColumn("data_source", lit(data_source))
    )

# COMMAND ----------

add_metadata_table(df, "dados_criados_para_teste").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Void Functions
# MAGIC
# MAGIC <p>Funções sem retorno. São utilizadas para salvar tabelas, realizar configurações, orquestrar e etc.</p>

# COMMAND ----------

def overwrite_table(df, table_name):
    df.write.mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

def add_metadata_and_save_table(df, data_source, table_name):
    df_with_metadata = add_metadata_table(df, data_source)
    overwrite_table(df_with_metadata, table_name)

# COMMAND ----------

add_metadata_and_save_table(df, "dados_criados_para_teste", "bronze.functions_table")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Funções auxiliares

# COMMAND ----------

def get_table_columns_information_list(table_name):
    table_describe_collected = spark.sql(f"DESCRIBE {table_name}").collect()
    return [row.asDict() for row in table_describe_collected]

# COMMAND ----------

get_table_columns_information_list("bronze.functions_table")
