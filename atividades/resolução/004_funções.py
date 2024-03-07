# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Exercício 1
# MAGIC
# MAGIC <p>
# MAGIC   Crie uma função retorna a localização física da tabela (location ou path).
# MAGIC </p>

# COMMAND ----------

def get_table_location(table_name):
    return (spark
            .sql(f"DESCRIBE EXTENDED {table_name}")
            .where("col_name='Location'").collect()[0]
            .data_type)
    
get_table_location("samples.tpch.nation")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Exercício 2
# MAGIC
# MAGIC <p>
# MAGIC   crie uma função que retorna um dataframe com somente o último registro do dataframe de cadastro para cada cliente.
# MAGIC </p>

# COMMAND ----------

cadastro_data = [
    {"id_cliente": 1, "update_at": "2021-01-01", "endereco": "Rua A"},
    {"id_cliente": 2, "update_at": "2021-01-02", "endereco": "Rua B"},
    {"id_cliente": 3, "update_at": "2021-01-03", "endereco": "Rua C"},
    {"id_cliente": 1, "update_at": "2021-01-04", "endereco": "Rua D"},
    {"id_cliente": 2, "update_at": "2021-01-05", "endereco": "Rua E"},
    {"id_cliente": 4, "update_at": "2021-01-06", "endereco": "Rua F"},
    {"id_cliente": 3, "update_at": "2021-01-07", "endereco": "Rua G"},
    {"id_cliente": 1, "update_at": "2021-01-08", "endereco": "Rua H"},
    {"id_cliente": 4, "update_at": "2021-01-09", "endereco": "Rua I"},
    {"id_cliente": 4, "update_at": "2021-01-09", "endereco": "Rua I"}
]

df_cadastro = spark.createDataFrame(cadastro_data)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import max, col, row_number

def get_last_register(df, partition_columns, time_column):
    window_spec = Window.partitionBy(partition_columns).orderBy(col(time_column).desc())
    df_row_number = df.withColumn("row_number", row_number().over(window_spec))
    
    return df_row_number.where("row_number=1").drop("row_number")

get_last_register(df_cadastro, ['id_cliente'], "update_at").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Exercício 3
# MAGIC
# MAGIC <p>
# MAGIC   Faça uma função que passado o nome de um widget ele gere na forma de texto e já retorne o valor do widget.
# MAGIC </p>

# COMMAND ----------

def get_param(*args):
    dbutils.widgets.text(*args)
    return dbutils.widgets.get(args[0])

get_param('start_date', '2022-01-10')
