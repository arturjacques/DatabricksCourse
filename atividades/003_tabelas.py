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



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercício 2
# MAGIC #### Ler da bronze e salvar na silver como tabela gerenciada corrigindo o tipo das colunas.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercício 3
# MAGIC #### Mostrar históricos das tabelas

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercício 4
# MAGIC #### Comentar as colunas e a tabela da silver com o que é cada coluna e a tabela

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## LIMPEZA DO AMBIENTE DO USUARIO

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
