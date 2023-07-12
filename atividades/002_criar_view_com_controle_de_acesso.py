# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <h1>Criar view com controle de acesso a nível de linha</h1>
# MAGIC
# MAGIC <p>membros do grupo 'br_users' devem ver somente dados do brasil da tabela silver.compras​</p>
# MAGIC <p>membros do 'usa_users' devem ver somente dados dos EUA​</p>
# MAGIC <p>Pessoas no grupo admin devem ver toda a tabela.​</p>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h1>Criar uma view de compras que mostre um Hashed do user_id de compras para usuários não admins</h1>
# MAGIC
# MAGIC <p>Concatenar uma "salt key" com o user_id antes de hash a column</p>

# COMMAND ----------


