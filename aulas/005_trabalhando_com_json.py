# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Trabalhando com JSON
# MAGIC
# MAGIC <p>
# MAGIC   Muitas fontes de dados vem em json ou estruturas de documentos, fontes de API, banco de dados MongoDB, fontes web e etc. Essa fonte de dados está sujeita a muitas mudanças com o tempo e pode causar muitos erros nas pipelines, principalmente devido a mudanças na estrutura do json e nos tipos dos dados.
# MAGIC </p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##SETUP

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Criação de um schema para o Aluno para criar isolamento entre as atividades. Dessa forma cada aluno salva em seu próprio schema chamando a variável "SCHEMA".
# MAGIC </p>

# COMMAND ----------

ALUNO = spark.sql("SELECT current_user() as user").collect()[0].user.split("@")[0]

if ALUNO == "":
    raise Exception("É necessário preencher o nome do aluno")
elif " " in ALUNO:
    raise Exception("O nome do aluno não pode conter espaço")

SCHEMA = ALUNO + "_schema"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

print(30*'=')
print(f"CREATE USER SCHEMA {SCHEMA}")
print(30*'=')

# COMMAND ----------

from functions.data_creation import create_compras_de_usuario_list

diretorio_json = f"/tmp/{ALUNO}/compras_json"

dbutils.fs.rm(diretorio_json, True)
dbutils.fs.mkdirs(diretorio_json)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   criando arquivos json para serem consumidos.
# MAGIC </p>

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
# MAGIC <p>Utilizar o a opção de ler Json do pyspark facilita bastante para entender a estrutura. Porem isso força o spark a inferir o schema e em caso da estrutura do json mudar pode causar falhas na leitura e no salvamento. Mas é possível obter o schema do json que foi inferido para realizar a transformação depois.</p>

# COMMAND ----------

df_json = spark.read.json(f"{diretorio_json}/*.json")

# COMMAND ----------

df_json.limit(5).display()

# COMMAND ----------

df_json.printSchema()

# COMMAND ----------

df_json.schema

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Utilizando a opção de ler texto para ler json a estrutura não fica tão boa visualmente mas garante que a ingestão sempre será feita corretamente.
# MAGIC </p>

# COMMAND ----------

df_text = spark.read.text(f"{diretorio_json}/*.json")
df_text.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Salvar na bronze
# MAGIC
# MAGIC <p>
# MAGIC   Importante salvar na bronze com metadados do jeito que eles foram lidos antes de tratar o json. Geralmente as camadas bronze, silver e gold ficam em schemas diferentes, porem para maior simplicidade vamos salvar no schema do aluno todas as etapas do processo. Também é ideal que cada etapa do processo tenha seu próprio notebook para aumentar o isolamento dos códigos facilitando manutenção (quanta uma etapa do processo falha não é necessário rodar todas as etapas para resolver em uma pipeline).
# MAGIC </p>

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

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Geralmente as tabelas bronze são salvas utilizando append para evitar erros durante o processo e porque deseja-se manter todos os dados da leitura. Mas não é lido todos os dados da origem sempre na hora de salvamente e em aulas posteriores veremos formas de realizar leituras somente dos dados novos.
# MAGIC </p>

# COMMAND ----------

df_with_metadata.write.mode('append').saveAsTable(f"{SCHEMA}.bronze_json_compras")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Limpar dados e salvar na silver
# MAGIC
# MAGIC <p>Agora podemos tratar o json para salvar sem duplicados na silver. Primeira etapa é dar uma estrutura para a coluna do json.</p>

# COMMAND ----------

df_bronze = spark.table(f"{SCHEMA}.bronze_json_compras").dropDuplicates(subset=['input_file_name'])

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
