# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Trabalhando com JSON

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##SETUP

# COMMAND ----------

from functions.data_creation import create_compras_de_usuario_list

diretorio_json = "/tmp/compras_json"

dbutils.fs.rm(diretorio_json, True)
dbutils.fs.mkdirs(diretorio_json)

# COMMAND ----------

import json
from uuid import uuid4

for i in range(3):
    compras_list = create_compras_de_usuario_list()

    with open(f"/dbfs/{diretorio_json}/{uuid4()}.json", "w", encoding='utf8') as f:
        json.dump(compras_list, f, ensure_ascii=False)

# COMMAND ----------

display(dbutils.fs.ls(diretorio_json))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Leitura

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>Utilizar o a opção de ler Json do pyspark facilita bastante para entender a estrutura. Porem isso força o spark a inferir o schema e em caso da estrutura do json mudar pode causar falhas na leitura e no salvamento.</p>

# COMMAND ----------

df_json = spark.read.json("/tmp/compras_json/*.json")

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>Utilizando a opção de ler texto para ler json a estrutura não fica tão boa visualmente mas garante que a ingestão sempre será feita corretamente.</p>

# COMMAND ----------

df_text = spark.read.text("/tmp/compras_json/*.json")
df_text.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Salvar na bronze
# MAGIC
# MAGIC <p>Importante salvar na bronze com metadados do jeito que eles foram lidos antes de tratar o json.</p>

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

df_with_metadata = add_metadata_table(df_text, "json_compras_files")

df_with_metadata.display()

# COMMAND ----------

df_with_metadata.write.mode('append').saveAsTable("bronze.json_compras")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Limpar dados e salvar na silver
# MAGIC
# MAGIC <p>Agora podemos tratar o json para salvar sem duplicados na silver. Primeira etapa é dar uma estrutura para a coluna do json.</p>

# COMMAND ----------

df_bronze = spark.table("bronze.json_compras").dropDuplicates(subset=['input_file_name'])

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType, BooleanType, FloatType, DecimalType
from pyspark.sql.functions import from_json, col, explode, posexplode

schema = ArrayType(
            StructType([
                StructField("compra_id", StringType()),
                StructField("id_pessoa", StringType()),
                StructField("nome", StringType()),
                StructField("endereço", StringType()),
                StructField("compras", ArrayType(
                    StructType(
                        [
                            StructField("id_product", StringType()),
                            StructField("nome_do_produto", StringType()),
                            StructField("preço", DecimalType(16, 2)),
                            StructField("peso", DecimalType(16, 2))
                        ]
                    )
                ))
            ])
        )

df_json_treated = df_bronze.withColumn("json_field", from_json(col('value'), schema)).drop('value')
df_json_treated.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>Com o json tratado é possível realizar transformações em colunas. Primeiramente cada compra deve estar em uma linha individual</p>

# COMMAND ----------

df_compras_separeted = df_json_treated.select("*", explode("json_field").alias("compra_json")).drop('json_field')

df_compras_separeted.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>Com as compras separadas é possível criar uma coluna nova para cada chave do json.</p>

# COMMAND ----------

df_compras_columns = df_compras_separeted.select("*", "compra_json.*").drop('compra_json')
df_compras_columns.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>Agora é possível ter uma linha para cada produto de uma compra</p>

# COMMAND ----------

df_items_de_compra = df_compras_columns.select("*", posexplode("compras").alias("item_position", "item_de_compra_json")).drop("compras")
df_items_de_compra.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>Assim como o json de compras também é possível transformar as chaves do json em colunas.</p>

# COMMAND ----------

df_todos_os_items = df_items_de_compra.select("*", "item_de_compra_json.*").drop("item_de_compra_json")
df_todos_os_items.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Salvando a tabela na silver

# COMMAND ----------

from delta.tables import DeltaTable

table_name = 'silver.compras_json'
save_table_merge = False
merge_condition = "(source.compra_id = target.compra_id) AND (source.item_position = target.item_position)"

try:
    delta_table_compras = DeltaTable.forName(spark, table_name)
    save_table_merge = True
except Exception as e:
    if "is not a Delta table" in str(e):
        df_todos_os_items.write.saveAsTable(table_name)
    else:
        raise e

if save_table_merge:
    (delta_table_compras.alias("target")
        .merge(df_todos_os_items.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
