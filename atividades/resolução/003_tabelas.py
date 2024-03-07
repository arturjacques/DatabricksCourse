# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##Configuração do ambiente

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

# MAGIC %sql
# MAGIC
# MAGIC GRANT SELECT ON ANY FILE TO `users`

# COMMAND ----------

from functions.data_creation import CreateComprasDataCsv

CreateComprasDataCsv(spark, dbutils).create_and_save_data()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercício 1
# MAGIC ####Trazer os dados do que estão dentro da pasta "/tmp/compras"
# MAGIC
# MAGIC <p>Adicionar metadados (arquivo de origem, data de carga, nome da source)</p>
# MAGIC <p>salvar no formato delta em uma tabela gerenciada no schema bronze.</p>

# COMMAND ----------

from pyspark.sql.functions import input_file_name, current_date, lit, col

df = spark.read.option("header", True).csv("/tmp/compras")

df_with_metadata = (df
            .withColumn("input_file_name", input_file_name())
            .withColumn("load_at", current_date())
            .withColumn("source", lit("super loja"))
)

df_with_metadata.write.mode("overwrite").saveAsTable(f"{SCHEMA}.bronze_compras")

# COMMAND ----------

spark.table(f"{SCHEMA}.bronze_compras").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercício 2
# MAGIC #### Ler da bronze e salvar na silver como tabela gerenciada corrigindo o tipo das colunas.

# COMMAND ----------

df_bronze = spark.table(f"{SCHEMA}.bronze_compras")

df_bronze_treated = (df_bronze
    .withColumn("product_qt", col('product_qt').cast("integer"))
    .withColumn("value", col('value').cast("decimal(18,2)"))               
    )

df_bronze_treated.write.mode("overwrite").saveAsTable(f"{SCHEMA}.silver_compras")

# COMMAND ----------

spark.table(f"{SCHEMA}.silver_compras").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercício 3
# MAGIC #### Mostrar históricos das tabelas

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {SCHEMA}.bronze_compras").display()

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {SCHEMA}.silver_compras").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercício 4
# MAGIC #### Comentar as colunas e a tabela da silver com o que é cada coluna representa na tabela.

# COMMAND ----------

spark.table(f"{SCHEMA}.silver_compras").printSchema()

# COMMAND ----------

spark.sql(f"ALTER TABLE {SCHEMA}.silver_compras CHANGE id id string COMMENT 'id de compras'")
spark.sql(f"ALTER TABLE {SCHEMA}.silver_compras CHANGE user_id user_id string COMMENT 'id_do_usuário'")
spark.sql(f"ALTER TABLE {SCHEMA}.silver_compras CHANGE product_id product_id string COMMENT 'id do produto'")
spark.sql(f"ALTER TABLE {SCHEMA}.silver_compras CHANGE product_qt product_qt integer COMMENT 'quantidade do produto'")
spark.sql(f"ALTER TABLE {SCHEMA}.silver_compras CHANGE value value decimal(18,2) COMMENT 'valor em moeda local'")
spark.sql(f"ALTER TABLE {SCHEMA}.silver_compras CHANGE country country string COMMENT 'nome do país'")
spark.sql(f"ALTER TABLE {SCHEMA}.silver_compras CHANGE input_file_name input_file_name string COMMENT 'nome do arquivo de origem'")
spark.sql(f"ALTER TABLE {SCHEMA}.silver_compras CHANGE load_at load_at date COMMENT 'data de extração'")
spark.sql(f"ALTER TABLE {SCHEMA}.silver_compras CHANGE source source string COMMENT 'nome da source'")

# COMMAND ----------

spark.sql(f"DESCRIBE TABLE {SCHEMA}.silver_compras").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## LIMPEZA DO AMBIENTE DO USUARIO

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
