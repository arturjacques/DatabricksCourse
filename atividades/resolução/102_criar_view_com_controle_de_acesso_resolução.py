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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h1>Criar uma view de compras que mostre um Hashed do user_id de compras para usuários não admins</h1>
# MAGIC
# MAGIC <p>Concatenar uma "salt key" com o user_id antes de hash a column</p>

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW silver.view_compras_hashed
# MAGIC AS (
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   CASE
# MAGIC     WHEN is_member('admins') THEN user_id
# MAGIC     ELSE sha2(concat(user_id,'ahskdaljshdhasldjkaksjdlakjsd'), 256)
# MAGIC   END
# MAGIC   user_id,
# MAGIC   product_id,
# MAGIC   product_qt,
# MAGIC   value,
# MAGIC   country
# MAGIC FROM silver.compras
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM silver.view_compras_hashed

# COMMAND ----------

salt_key = dbutils.secrets.get("kv-training-03", "columnhashsaltkey")

spark.sql(
    f"""
CREATE OR REPLACE VIEW silver.view_compras_hashed_2
AS (
SELECT 
  id,
  CASE
    WHEN is_member('br_users') THEN user_id
    ELSE sha2(concat(user_id, '{salt_key}'), 256)
  END
  user_id,
  product_id,
  product_qt,
  value,
  country
FROM silver.compras
)
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h3>Nas duas formas a cima a salt key irá aparecer nos metadados da tabela na definição da view o que não é ideal.</h3>

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE FUNCTION gold.hash_column_salt_key(x STRING) RETURNS STRING RETURN sha2(concat(x, '{salt_key}'), 256);")

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW silver.view_compras_hashed_3
AS (
SELECT 
  id,
  CASE
    WHEN is_member('br_users') THEN user_id
    ELSE hash_column_salt_key(user_id)
  END
  user_id,
  product_id,
  product_qt,
  value,
  country
FROM silver.compras
)
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h3>Criando uma função e aplicando na view a salt key some dos metadados da view porém fica na da função. Logo a função precisa estar em um schema separado sem permissão de vizualação do metadado</h3>

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE FUNCTION EXTENDED hash_column_salt_key

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW USER FUNCTIONS IN gold
