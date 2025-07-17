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

SCHEMA = "hive_metastore." + ALUNO + "_schema"

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

from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql import DataFrame
from uuid import uuid4

def add_metadata_table(df: DataFrame, data_source)-> DataFrame:
    return (df
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("batch_id", lit(str(uuid4())))
        .withColumn("input_file_path", col("_metadata.file_path"))
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
# MAGIC <p>Agora podemos tratar o json para salvar sem duplicados na silver, também está sendo removido arquivos iguais porque na estrutura de salvamento da bronze pode acontecer do mesmo arquivo ser salvo mais de uma vez. Para limpar os dados precisamos tratar a column value para ter ter uma tabela com schema marcado.</p>

# COMMAND ----------

df_bronze = spark.table(f"{SCHEMA}.bronze_json_compras").dropDuplicates(subset=['input_file_path'])

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
# MAGIC <p>Com o json tratado é possível realizar transformações em colunas. Primeiramente cada compra deve estar em uma linha individual e como é um array podemos usar o comando explode que converte o array para uma nova linha para cada elemento do array.</p>

# COMMAND ----------

df_compras_separeted = df_json_treated.select("*", explode("json_field").alias("compra_json")).drop('json_field')

df_compras_separeted.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>Com as compras separadas é possível criar uma coluna nova para cada chave do json. Usando o wild card "*" você diz que dentro de uma coluna cada elemento ira se tornar uma coluna própria e após isso será necessário remover a coluna antiga que continha todas as colunas em uma estrutura mais complexa.</p>

# COMMAND ----------

df_compras_columns = df_compras_separeted.select("*", "compra_json.*").drop('compra_json')
df_compras_columns.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>Agora é possível ter uma linha para cada produto de uma compra usando o comando explode novamente para ter uma linha para cada item da compra. Mas utilizando o comando "explode" ira perder a informação sobre a posição do item e pode ser que houver 2 items iguais na mesma compra a linha fique exatamente igual por isso podemos utilizar a função posexplode que explode cada item em uma linha nova mas também cria uma coluna nova com a posição do item.</p>

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

table_name = f'{SCHEMA}.silver_compras_json'
save_table_merge = False
merge_condition = "(source.compra_id = target.compra_id) AND (source.item_position = target.item_position)"

if spark.catalog.tableExists(f'{SCHEMA}.silver_compras_json'):
    delta_table_compras = DeltaTable.forName(spark, table_name)

    (delta_table_compras.alias("target")
        .merge(df_todos_os_items.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df_todos_os_items.write.saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Limpar dados e salvar na silver com Variant
# MAGIC
# MAGIC <p>Uma nova feature do Databricks é o tipo <a href="https://docs.databricks.com/aws/en/sql/language-manual/data-types/variant-type">Variant</a>. Essa feature está em public preview e disponível a partir do runtime 15.3 do Databricks. Podemos realizar a mesma série de transformações com esse tipo com algumas diferenças</p>

# COMMAND ----------

df_bronze = spark.table(f"{SCHEMA}.bronze_json_compras").dropDuplicates(subset=['input_file_path'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Convertendo a coluna que é json para variant

# COMMAND ----------

from pyspark.sql.functions import parse_json

df_bronze_variant = df_bronze.withColumn("value", parse_json('value'))
df_bronze_variant.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Explodingo o Array. O processo para explodir o variant no momento não é tão fácil de realizar em pyspark, mas podemos converter em um array de variant e explodir como se fosse um array.

# COMMAND ----------

df_variant_compras_separeted = df_bronze_variant.selectExpr("*", "explode(cast(value as array<variant>)) as compra_json").drop("value")
df_variant_compras_separeted.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Como o tipo é variant não há um schema fixo, logo não podemos usar um wild card para transformar os elementos do variant em colunas, então o processo deve ser feito individualmente para cara coluna.

# COMMAND ----------

df_compras_variant_columns = df_variant_compras_separeted.selectExpr(
    "*", 
    "compra_json:compra_id::string",
    "compra_json:compras::array<variant>",
    "compra_json:`endereço`::string",
    "compra_json:id_pessoa::string",
    "compra_json:nome::string"
).drop("compra_json")

df_compras_variant_columns.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Agora podemos fazer a explosão com a posição novamente das compras.

# COMMAND ----------

df_variant_items_de_compra = df_compras_variant_columns.select("*", posexplode("compras").alias("item_position", "item_de_compra_json")).drop("compras")
df_variant_items_de_compra.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC E agora ficamos com uma coluna com um documento do tipo variant. Podemos transformar o documento em colunas.

# COMMAND ----------

df_compras_variant_columns = df_variant_items_de_compra.selectExpr(
    "*", 
    "item_de_compra_json:id_product::string",
    "item_de_compra_json:nome_do_produto::string",
    "item_de_compra_json:peso::Decimal(18,3)",
    "item_de_compra_json:`preço`::Decimal(18,2)",
).drop("item_de_compra_json")

df_compras_variant_columns.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## LIMPEZA DO AMBIENTE DO USUARIO

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
dbutils.fs.rm(diretorio_json, True)
