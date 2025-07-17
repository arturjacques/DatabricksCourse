# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Exercicio 1
# MAGIC #### Filtrar orders date
# MAGIC
# MAGIC <p>
# MAGIC   Filtrar dados da tabela samples.tpch.orders de janeiro de 1998 a fevereiro de 1998 utilizando um widget de start_date e um de end_date.
# MAGIC </p>

# COMMAND ----------

dbutils.widgets.text("start_date", "1998-01-01")
dbutils.widgets.text("end_date", "1998-02-01")

start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

print(f"o_orderdate>='{start_date}' AND o_orderdate<'{end_date}'")

spark.table("samples.tpch.orders").where(f"o_orderdate>='{start_date}' AND o_orderdate<'{end_date}'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercicio 2
# MAGIC #### Valor Default
# MAGIC
# MAGIC <p>
# MAGIC   Fazer um widget que deve ter o nome que a tabela deve ser salva. Caso não seja informado o valor default deve ser "sandbox.table_name".
# MAGIC </p>

# COMMAND ----------

dbutils.widgets.text("save_name", "sandbox.table_name")

save_name = dbutils.widgets.get("save_name")

print(save_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercicio 3
# MAGIC #### Debug Print
# MAGIC
# MAGIC <p>
# MAGIC   A função a baixo só printa se o valor do widget "debug" for igual a True. Crie o widget debug e faça a função printar o valor ao executar.
# MAGIC </p>

# COMMAND ----------

def print_debug(text):
    try:
        debug = dbutils.widgets.get("debug")
    except Exception as e:
        if "No input widget named debug is defined" in e.__str__():
            debug="False"
        else:
            raise e

    if debug=="True":
        print(text)

print_debug("debugando o código")

dbutils.widgets.text("debug", "True")
