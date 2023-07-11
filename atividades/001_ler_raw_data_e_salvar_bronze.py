# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <h2>Configuração do ambiente<\h2>

# COMMAND ----------

storage_account_name = "dlstraining01"
container_name = "raw"
sas_token = dbutils.secrets.get('kv-training-03', 'dlstraining01readtoken')

spark.conf.set(
    f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net",
    sas_token
)

# COMMAND ----------

data_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"

dbutils.fs.ls(data_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2>Trazer os dados do que estão dentro da pasta "compras"</h2>
# MAGIC
# MAGIC <p>Adicionar metadados (arquivo de origem, data de carga, nome da source)</p>
# MAGIC <p>salvar no formato delta em uma tabela no schema bronze.</p>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2>Ler da bronze e salvar na silver (corrigindo alguns erros)</h2>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h1>Mostrar históricos das tabelas</h1>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h1>Comentar as colunas e a tabela da silver com o que é cada coluna e a tabela</h1>

# COMMAND ----------


