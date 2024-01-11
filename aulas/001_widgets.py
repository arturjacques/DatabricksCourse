# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # SETUP

# COMMAND ----------

lista_compras = [
    {"id_compra": 1, "id_usuario": 101, "valor": 50.0, "imposto": 5.0, "data": "2022-01-01", "pais": "BR"},
    {"id_compra": 2, "id_usuario": 102, "valor": 30.0, "imposto": 3.0, "data": "2022-01-02", "pais": "BR"},
    {"id_compra": 3, "id_usuario": 103, "valor": 100.0, "imposto": 10.0, "data": "2022-01-03", "pais": "CO"},
    {"id_compra": 4, "id_usuario": 104, "valor": 80.0, "imposto": 8.0, "data": "2022-01-04", "pais": "PE"},
    {"id_compra": 5, "id_usuario": 105, "valor": 120.0, "imposto": 12.0, "data": "2022-01-05", "pais": "PE"}
]

df = spark.createDataFrame(lista_compras)
df.createOrReplaceTempView('compras')

# COMMAND ----------

# MAGIC %md #Widgets
# MAGIC
# MAGIC <p>Um widget no Databricks é uma ferramenta interativa que permite aos usuários personalizar e controlar a execução de um notebook. Ele fornece uma maneira de interagir com o código e visualizar os resultados em tempo real.</p>
# MAGIC
# MAGIC <p>Os widgets podem ser implementados em várias formas, como caixas de seleção, botões, campos de texto e barras de rolagem. Eles são incorporados em um notebook e podem ser usados para fornecer entrada de dados, alterar parâmetros de execução ou filtrar visualizações de dados.</p>
# MAGIC
# MAGIC <p>Além de possibilitar uma execução iterativa do notebook também é utilizado para parametrizar execuções de orquestradores.</p>
# MAGIC
# MAGIC <p>Importante notar que quando um widget é alterado na parte superior do notebook automáticamente a célula que atribuí aquele widget a uma variável é executada. É possível alterar isso nas configurações do widget.</p>
# MAGIC
# MAGIC <p>Leitura adicional: <a href="https://docs.databricks.com/pt/notebooks/widgets.html">https://docs.databricks.com/pt/notebooks/widgets.html</a></p>

# COMMAND ----------

#documentação oficial da classe

help(dbutils.widgets)

# COMMAND ----------

# MAGIC %md ##Widgets de texto
# MAGIC
# MAGIC <p>
# MAGIC   Textos são livres um máximo de 2048 bytes. Essa limitação acontece com valores iterativos mas não com valores enviados por APIs de orquestração tendo valores.
# MAGIC </p>
# MAGIC <p>É possível inicializar o widget de texto utilizando o método <b>dbutils.widgets.text</b>, o primeiro valor consiste no nome do widget e o valor que será considerado pela API, o segundo valor é opcional e é o valor default dele, o terceiro também é opcional e vai ser o nome que ficará em destaque no widget e esse nome que define a ordem (segue em ordem alfabética) todos os widgets ficam na parte superior do notebook.</p>

# COMMAND ----------

dbutils.widgets.text("start_date", "2022-01-01", "01_start_date")
dbutils.widgets.text("end_date", "2022-01-02", "02_end_date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![Widget Location](https://github.com/arturjacques/DatabricksCourse/blob/main/images/widgets%20location.png?raw=true)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>Para ter o retorno do valor do widget para uma variável é possível utilizar o método <b>dbutils.widggets.get</b> passando o nome do widget.</p>

# COMMAND ----------

start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

df.where(f"data>='{start_date}' AND data<='{end_date}'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>É possível utilizar ${} com o nome do widget dentro das chaves para obter o valor do widget na query SQL.</p>

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT '${start_date}' as date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM compras
# MAGIC WHERE data>='${start_date}' AND data<='${end_date}'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>Como é possível inserir qualquer tipo de texto algumas forma mais complexas também podem ser enviadas como por exemplo um JSON, YML ou outras formas de texto.</p>

# COMMAND ----------

dbutils.widgets.text("json_text", """{"start_date": "2022-01-01", "end_date": "2022-01-04"}""", "03_json_text")

# COMMAND ----------

import json

json_text = dbutils.widgets.get("json_text")

json_dict = json.loads(json_text)
json_dict['start_date']

# COMMAND ----------

# MAGIC %md ##Widgets de dropdown
# MAGIC <p>
# MAGIC   Possuem valores predeterminados que podem selecionados. Não é muito utilizado para fazer parametrização de trabalhos para workflow mas é útil em ocasiões de trabalho iterativo. Uma feature interessante é deixar esse widget com uma variável de "debug" que pode printar algumas coisas na tela durante e a execução do notebook para melhor entender o processo.
# MAGIC </p>

# COMMAND ----------

dbutils.widgets.dropdown("debug", "True", ["True", "False"], "04_debug")

# COMMAND ----------

debug = dbutils.widgets.get("debug")

print(f"debug mode é {debug} e é do tipo {type(debug)}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>Também é possível fazer uma caixa de dropdown para selecionar o pais que será processado ou filtrado no notebook.</p>

# COMMAND ----------

dbutils.widgets.dropdown("country_filtered", "BR", ["BR", "CO", "PE"], "05_country_filtered")
country_filtered = dbutils.widgets.get("country_filtered")

df.where(f"pais='{country_filtered}'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets de combobox
# MAGIC <p>
# MAGIC   Muito parecido com dropbox possui valores predeterminados que pode selecionar. Possui somente uma estética diferente no método de seleção dos paises. Pode ser utilizada da mesma forma que o "dropdown"
# MAGIC </p>

# COMMAND ----------

dbutils.widgets.combobox("country", "BR", ["BR","CA", "IL", "MI", "NY", "OR", "VA"], "06_country")

# COMMAND ----------

country = dbutils.widgets.get("country")

print(f"country é {country}")

# COMMAND ----------

# MAGIC %md ## Widgets de multiselect
# MAGIC <p>
# MAGIC   Pode selecionar um ou mais elementos. É muito útil para filtros em que se deseja filtrar uma parcela dos dados. Podem ser filtrados paises, distribuidores, clientes, colunas e etc.
# MAGIC </p>

# COMMAND ----------

dbutils.widgets.multiselect("columns_select", "data", ['data', 'id_compra', 'id_usuario', 'imposto', 'pais', 'valor'], "07_columns_select")

# COMMAND ----------

columns_select = dbutils.widgets.get("columns_select")

print(f"columns_select é '{columns_select}'")

df.select(columns_select.split(',')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Também é possível combinar widgets para gerar tabelas ou gráficos filtrando colunas e linhas.
# MAGIC </p>

# COMMAND ----------

dbutils.widgets.multiselect("countries_selected", "BR", ['BR', 'CO', 'PE'], "08_countries_selected")
country_select = dbutils.widgets.get("countries_selected")

if len(country_select.split(',')) == 1:
    query_country = "('" + country_select + "')"
else:
    query_country = tuple(country_select.split(','))

df.where(f"pais in {query_country}").select(columns_select.split(',')).display()
