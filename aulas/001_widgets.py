# Databricks notebook source
# MAGIC %md #Widgets

# COMMAND ----------

# MAGIC %md ##Widgets de texto
# MAGIC
# MAGIC <p>
# MAGIC   Textos são livres e podem receber qualquer valor.
# MAGIC </p>

# COMMAND ----------

dbutils.widgets.text("start_date", "2022-01-01", "1_start_date")
dbutils.widgets.text("end_date", "2022-04-01", "2_end_date")

# COMMAND ----------

start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

print(f"start_date é {start_date}\nend_date {end_date}")

# COMMAND ----------

# MAGIC %md ##Widgets de dropdown
# MAGIC <p>
# MAGIC   Possuem valores predeterminados que podem selecionados
# MAGIC </p>

# COMMAND ----------

dbutils.widgets.dropdown("debug", "True", ["True", "False"], "3_debug")

# COMMAND ----------

debug = dbutils.widgets.get("debug")

print(f"debug mode é {debug} e é do tipo {type(debug)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets de combobox
# MAGIC <p>
# MAGIC   Muito parecido com dropbox possui valores predeterminados que pode selecionar.
# MAGIC </p>

# COMMAND ----------

dbutils.widgets.combobox("country", "BR", ["BR","CA", "IL", "MI", "NY", "OR", "VA"], "4_country")

# COMMAND ----------

country = dbutils.widgets.get("country")

print(f"country é {country}")

# COMMAND ----------

# MAGIC %md ## Widgets de multiselect
# MAGIC <p>
# MAGIC   Pode selecionar um ou mais elementos.
# MAGIC </p>

# COMMAND ----------

dbutils.widgets.multiselect("columns_select", "id", ["id", "country", "name"], "5_columns_select")

# COMMAND ----------

columns_select = dbutils.widgets.get("columns_select")

print(f"columns_select é '{columns_select}'")
