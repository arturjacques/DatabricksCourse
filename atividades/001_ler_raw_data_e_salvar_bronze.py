# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <h2>Configuração do ambiente<\h2>

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
# MAGIC <h2>Trazer os dados do que estão dentro da pasta "/tmp/compras"</h2>
# MAGIC
# MAGIC <p>Adicionar metadados (arquivo de origem, data de carga, nome da source)</p>
# MAGIC <p>salvar no formato delta em uma tabela no schema bronze.</p>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2>Ler da bronze e salvar na silver corrigindo o tipo das colunas.</h2>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2>Mostrar históricos das tabelas</h2>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2>Comentar as colunas e a tabela da silver com o que é cada coluna e a tabela</h2>

# COMMAND ----------


