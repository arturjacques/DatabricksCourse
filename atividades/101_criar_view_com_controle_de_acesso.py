# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <h1>Criar view com controle de acesso a nível de linha</h1>
# MAGIC
# MAGIC <p>membros do grupo 'br_users' devem ver somente dados do brasil da tabela samples.tpch.customer dos samples do Databricks​</p>
# MAGIC <p>membros do 'usa_users' devem ver somente dados dos EUA​</p>
# MAGIC <p>Pessoas no grupo admin devem ver toda a tabela. A tabela samples.tpch.nation pode ser utilizada para saber os paises.​</p>

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM samples.tpch.customer
# MAGIC WHERE c_nationkey = '2'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM samples.tpch.nation

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h1>Criar uma view de compras que mostre um Hashed do user_id de compras para usuários não admins</h1>
# MAGIC
# MAGIC <p>Concatenar uma "salt key" com o user_id antes de hash a column</p>

# COMMAND ----------


