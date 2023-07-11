# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <h1>Criar view com controle de acesso a nível de linha</h1>
# MAGIC
# MAGIC <p>membros do grupo 'br_users' devem ver somente dados do brasil da tabela silver.compras​</p>
# MAGIC <p>membros do 'usa_users' devem ver somente dados dos EUA​</p>
# MAGIC <p>Pessoas no grupo admin devem ver toda a tabela.​</p>

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW silver.view_compras
# MAGIC AS (
# MAGIC SELECT *
# MAGIC FROM silver.compras
# MAGIC WHERE (COUNTRY = 'BR' AND is_member('br_users'))
# MAGIC OR (COUNTRY = 'US' AND is_member('usa_users'))
# MAGIC OR is_member('admins')
# MAGIC )
