# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## SETUP

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Criação de um schema para o Aluno para criar isolamento entre as atividades. Dessa forma cada aluno salva em seu próprio schema chamando a variável "SCHEMA".
# MAGIC </p>

# COMMAND ----------

ALUNO = spark.sql("SELECT current_user() as user").collect()[0].user.split("@")[0]

if ALUNO == "":
    raise Exception("É necessário preencher o nome do aluno")
elif " " in ALUNO:
    raise Exception("O nome do aluno não pode conter espaço")

SCHEMA = "hive_metastore." + ALUNO + "_schema"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

print(30*'=')
print(f"CREATE USER SCHEMA {SCHEMA}")
print(30*'=')

# COMMAND ----------

pessoas = [
    {"id_pessoa": 1, "nome": "Ana", "idade": 25, "sexo": "F"},
    {"id_pessoa": 2, "nome": "Bruno", "idade": 30, "sexo": "M"},
    {"id_pessoa": 3, "nome": "Carla", "idade": 28, "sexo": "F"}
]

compras =  [
        {"id_compra": 101, "valor": 150.00, "produto": "Vestido", "id_pessoa": 1},
        {"id_compra": 102, "valor": 50.00, "produto": "Sapato", "id_pessoa": 1},
        {"id_compra": 103, "valor": 200.00, "produto": "Relógio", "id_pessoa": 2},
        {"id_compra": 104, "valor": 100.00, "produto": "Camisa", "id_pessoa": 2},
        {"id_compra": 105, "valor": 300.00, "produto": "Notebook", "id_pessoa": 3},
        {"id_compra": 106, "valor": 80.00, "produto": "Fone de ouvido", "id_pessoa": 3}
    ]

videogames_favoritos = [ 
    {"id_pessoa": 1, "console": "PlayStation 5", "videogame": "God of War: Ragnarok"},
    {"id_pessoa": 2, "console": "Xbox Series X", "videogame": "Halo Infinite"},
    {"id_pessoa": 3, "console": "Nintendo Switch", "videogame": "Animal Crossing: New Horizons"} 
]

df_pessoas = spark.createDataFrame(pessoas)
df_compras = spark.createDataFrame(compras)
df_videogames_favoritos = spark.createDataFrame(videogames_favoritos)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Formato de Dados
# MAGIC
# MAGIC <p>
# MAGIC     Existem muitos formatos de dados json, csv, parquet, avro, orc, Delta e etc. Importante notar que quando estamos lidando com uma tabela no databricks a tabela nada mais é que um apontamento para uma serie de arquivos que pode estar visível para o usuário ou não. Cada formato tem suas vantagens e desvantagens mas um formato muito utilizado na estrutura spark é o Delta. Delta é um formato de dados que tem por os aquivos salvos em parquet porém é colocado uma cama lógica em cima atravez do _delta_log que permite operações ACID com essa tabela. Logo torna-se possível realizar merge, updates, deletes, inserts na tabela sem corromper os dados e garantindo que ou o processo funciona ou falha por completo. Por padrão quando é salva uma tabela no Databricks é utilizado Delta.
# MAGIC </p>
# MAGIC
# MAGIC <p>Leitura adicional: <a href="https://www.linkedin.com/pulse/formatos-e-tipos-de-arquivos-na-aws-raphael-pizzo/?originalSubdomain=pt">https://www.linkedin.com/pulse/formatos-e-tipos-de-arquivos-na-aws-raphael-pizzo/?originalSubdomain=pt</a></p>
# MAGIC
# MAGIC <p>Leitura adicional: <a href="https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html">https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html</a></p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Features do Formato Delta

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Historico
# MAGIC <p>
# MAGIC   Essa função permite ver todas as alterações realizadas em uma tabela e por quem foi realizado, assim podendo ter um controle maior além de conseguir debugar com muito mais facilidade.
# MAGIC </p>

# COMMAND ----------

# limpeza 
spark.sql(f"DROP TABLE IF EXISTS {SCHEMA}.video_game")

# primeira versão
df_videogames_favoritos.write.saveAsTable(f"{SCHEMA}.video_game")

#segunda versão
videogames_favoritos_append = [ 
    {"id_pessoa": 4, "console": "Nintendo Switch", "videogame": "Zelda: Tears of the kingdom"} 
]
videogames_favoritos_append_df = spark.createDataFrame(videogames_favoritos_append)
videogames_favoritos_append_df.write.mode("append").saveAsTable(f"{SCHEMA}.video_game")

#terceira versão
spark.sql(f"DELETE FROM {SCHEMA}.video_game WHERE id_pessoa=3")

#historico das versões 
spark.sql(f"DESCRIBE HISTORY {SCHEMA}.video_game").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Time Travel
# MAGIC <p>
# MAGIC   Time Travel consiste na habilidade de ver versões anteriores da tabela, podendo consultar diferentes versões da tabela. Utilizando o histórico da tabela e possível verificar qual versão ler, utilizando "@" e o número da versão após o nome da tabegla é possível voltar para um estado anterior da tabela. Não é possível voltar para versões da tabela onde o vacuum já foi realizado.
# MAGIC </p>
# MAGIC
# MAGIC <p>Leitura adicional: <a href="https://www.databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html">https://www.databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html</a></p>

# COMMAND ----------

#versão atual da tabela

spark.table(f"{SCHEMA}.video_game").display()

# COMMAND ----------

#primeira versão da tabela

spark.table(f"{SCHEMA}.video_game@v0").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Vacuum
# MAGIC
# MAGIC <p>
# MAGIC   Vacuum é uma importante operação em tabelas Delta. Como elas mantém histórico de várias versões da tabela é importante com o tempo excluir versões antigas. O comando vacuum tem o valor minimo de 168 horas por padrão, o que signifca que versões da tabela que tem mais de 168 horas serão excluídas. Importante sempre que iniciar um novo projeto ter em mente que sera necessário rodar o vacuum de todas as tabelas deltas de tempos em tempos (o tempo de retenção dos dados ira variar de acordo com o a politica da empresa).
# MAGIC   É possível alterar o check do tempo de retenção da tabela desabilitando o check no spark e assim podendo realizar vacuum anterior a 168 horas (1 semana).
# MAGIC </p>
# MAGIC
# MAGIC <p>Leitura adicional: <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-vacuum">https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-vacuum</a></p>

# COMMAND ----------

#excluindo todas as versões antigas da tabela com retenção de 0 horas

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(f"VACUUM {SCHEMA}.video_game RETAIN 0 HOURS").display()

# COMMAND ----------

# A tabela continua existindo com a última versão

spark.table(f"{SCHEMA}.video_game").display()

# COMMAND ----------

# agora não é mais possível fazer query na primeira versão da tabela

spark.table(f"{SCHEMA}.video_game@v0").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tipos de Tabelas

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Tabela Externa
# MAGIC <p>
# MAGIC   Na hora de salvar um dataframe é possível salvar em um caminho específico no sistema, esse caminho pode estar em diferentes fornecedores de serviço de cloud. Quando salvamos o dataframe dessa forma no formato "Delta" toda estrutura da tabela estara salva nos arquivos físicos e você pode realizar insert, update, delete no path de salvamento sem problemas, tambem é possível salvar dados em outros formatos mas alguns formatos não permite update.
# MAGIC </p>
# MAGIC <p>
# MAGIC   Quando temos os arquivos em um path específico é possível criar uma tabela no hive metastore ou no unity catalog apontando essa path. Essa tabela criada será externa, também é possível criar uma tabela externa definindo a localização da tabela na hora da criação. Tabelas externas quando executado o comando "DROP" é removido o apontamento da tabela para os arquivos, porem os arquivos continuam existindo e é necessário excluir no path.
# MAGIC </p>

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Salvando um dado no path

# COMMAND ----------

save_path = f"/tmp/{ALUNO}"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   para salvar os dados podemos utilizar o método write do dataframe. A opção format é opcional para delta (por padrão os dataframes são salvos como delta, outros formatos devem ser especificados). O mode pode ser "append" quando desejamos incluir todos os dados no destino, "overwrite" quando desejamos substituir os dados de destino, "error" ou "errorifexists" levanta uma excessão se o dado já existir, "ignore" quando não faz nada se já existir dados no destino. E no comando save irá o destino onde será salvo o dataframe.
# MAGIC </p>

# COMMAND ----------

compras_path = save_path+"/compras"

df_compras.write.format("delta").mode('overwrite').save(compras_path)

# COMMAND ----------

spark.read.format("delta").load(compras_path).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   para criar uma tabela com base em um arquivo de um local pode se utilizar o comando "CREATE TABLE {nome da tabela} LOCATION {path}". Quando criado do formato delta ou parquet ele vai utilizar os formatos do metadados.
# MAGIC </p>

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.compras_table LOCATION '{compras_path}'")

spark.table(f"{SCHEMA}.compras_table").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Mesmo dropando a tabela os dados continuam existindo no diretório.
# MAGIC </p>

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {SCHEMA}.compras_table")

display(dbutils.fs.ls(compras_path))

# COMMAND ----------

# MAGIC %md ## Tabelas Gerenciada
# MAGIC
# MAGIC <p>
# MAGIC   Tabelas gerenciadas, como o nome já diz, são gerenciadas pelo hive metastore ou unity catalog. Essas tabelas quando salvam ficam na location padrão do schema que pode ser configurada durante a criação do schema (para verificar o local que as tabelas ficam salvas utilizase o comando "DESRIBE SCHEMA {nome do schema}"). Diferente da tabela externa quando é utilizado o comando "DROP" em uma tabela gerenciada o dado é excluído junto no hive_metastore, no unity catalog a tabela é marcada para delete e 7 dias depois é excluída, logo é possível reverter o drop de tabelas no unity.
# MAGIC </p>
# MAGIC
# MAGIC <p>
# MAGIC   Para salvar o dataframe como tabela pode utilizar o comando saveAsTable. Tabelas externas também é possível utilizar esse comando após a criação dela para savar os dados dentro da tabela. O Databricks recomenda que as tabelas sejam gerenciadas, porem muitas vezes é uma necessidade de negocio salvar dados em storages específicos, logo devemos definir a location do schema dentro da storage para evitar precisar salvar como external.
# MAGIC </p>
# MAGIC
# MAGIC <p>Leitura adicional: <a href="https://learn.microsoft.com/pt-br/azure/databricks/data-governance/unity-catalog/create-tables">https://learn.microsoft.com/pt-br/azure/databricks/data-governance/unity-catalog/create-tables</a></p>

# COMMAND ----------

df_compras.write.mode("overwrite").saveAsTable(f"{SCHEMA}.compras_table_managed")

spark.table(f"{SCHEMA}.compras_table_managed").display()

# COMMAND ----------

spark.sql(f"DESCRIBE SCHEMA {SCHEMA}").display()

# COMMAND ----------

database_path = (spark
                    .sql(f"DESCRIBE SCHEMA {SCHEMA}")
                    .where("database_description_item='Location'")
                    .collect()[0]
                    .database_description_value)

display(dbutils.fs.ls(database_path))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Quando dropamos a tabela no hive metastore os dados somem como mostrado a baixo.
# MAGIC </p>

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {SCHEMA}.compras_table_managed")

dbutils.fs.ls(database_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Modos de Salvamento de Tabelas

# COMMAND ----------

# MAGIC %md
# MAGIC ###Append
# MAGIC
# MAGIC <p>
# MAGIC   Append é o modo mais simples de salvamento. O dado é adicionado na tabela existente, esse modo é ideal para a camada bronze, por ser um modo muito simples dificilmente levanta erros.
# MAGIC </p>

# COMMAND ----------

# criando tabela

df_pessoas.write.mode("overwrite").saveAsTable(f"{SCHEMA}.pessoas_append")

spark.table(f"{SCHEMA}.pessoas_append").display()

# COMMAND ----------

#append na tabela

pessoas_append = [
    {"id_pessoa": 3, "nome": "Carla", "idade": 28, "sexo": "F"},
    {"id_pessoa": 4, "nome": "João", "idade": 65, "sexo": "M"}
]

df_append = spark.createDataFrame(pessoas_append)
df_append.write.mode("append").saveAsTable(f"{SCHEMA}.pessoas_append")

spark.table(f"{SCHEMA}.pessoas_append").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overwrite
# MAGIC
# MAGIC <p>
# MAGIC   Overwrite é o modo que reescreve a tabela inteira mas mantem o schema da tabela. Se for feito o overwrite da tabela com o schema diferente a operação ira falhar se não permitir alteração de schema. Esse modo é muito útil para tabelas gold, algumas tabelas de KPIs sumarizados que são para todos os dados de uma vez.
# MAGIC </p>

# COMMAND ----------

# criando tabela
spark.sql(f"DROP TABLE IF EXISTS {SCHEMA}.pessoas_overwrite")
df_pessoas.write.mode("overwrite").saveAsTable(f"{SCHEMA}.pessoas_overwrite")

spark.table(f"{SCHEMA}.pessoas_overwrite").display()

# COMMAND ----------

#overwrite na tabela

pessoas_overwrite = [
    {"id_pessoa": 3, "nome": "Carla", "idade": 28, "sexo": "F"},
    {"id_pessoa": 4, "nome": "João", "idade": 65, "sexo": "M"}
]

df_overwrite = spark.createDataFrame(pessoas_overwrite)
df_overwrite.write.mode("overwrite").saveAsTable(f"{SCHEMA}.pessoas_overwrite")

spark.table(f"{SCHEMA}.pessoas_overwrite").display()

# COMMAND ----------

#overwrite na tabela e schema (id_pessoa se tornou id)

pessoas_overwrite = [
    {"id": 3, "nome": "Carla", "idade": 28, "sexo": "F"},
    {"id": 4, "nome": "João", "idade": 65, "sexo": "M"}
]

df_overwrite = spark.createDataFrame(pessoas_overwrite)
df_overwrite.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable(f"{SCHEMA}.pessoas_overwrite")

spark.table(f"{SCHEMA}.pessoas_overwrite").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Replace Where
# MAGIC
# MAGIC <p>
# MAGIC     O Replace Where permite que você trunque uma parte dos dados e insira os dados novos. Por exemplo, imagine um caso que você retira dados dos últmos 7 dias diariamente de um banco de dados. Esses dados podem sofrer alterações como reajuste de preço, remoção de compra e etc. Cada nova carga pode substituir os dados antigos para garantir estar igual os dados do banco de dados. Para isso é possível utilizar esse tipo de salvamento.
# MAGIC </p>
# MAGIC <p>
# MAGIC     A condição de Replace é onde a tabela será truncada, se o dado que for inserido na tabela não estiver dentro da condição o processo levantará um erro.
# MAGIC </p>

# COMMAND ----------

#carga inicial

data = [
    {"id_compra": 1, "id_pessoa": 1, "id_produto": 1, "valor": 100.0, "data_de_compra": "2022-01-01"},
    {"id_compra": 2, "id_pessoa": 2, "id_produto": 2, "valor": 200.0, "data_de_compra": "2022-01-02"},
    {"id_compra": 3, "id_pessoa": 3, "id_produto": 1, "valor": 150.0, "data_de_compra": "2022-01-03"},
    {"id_compra": 4, "id_pessoa": 2, "id_produto": 3, "valor": 75.0, "data_de_compra": "2022-01-04"},
    {"id_compra": 5, "id_pessoa": 1, "id_produto": 2, "valor": 50.0, "data_de_compra": "2022-01-05"},
    {"id_compra": 6, "id_pessoa": 3, "id_produto": 2, "valor": 125.0, "data_de_compra": "2022-01-06"},
    {"id_compra": 7, "id_pessoa": 1, "id_produto": 3, "valor": 80.0, "data_de_compra": "2022-01-07"},
    {"id_compra": 8, "id_pessoa": 2, "id_produto": 1, "valor": 175.0, "data_de_compra": "2022-01-08"}
]
df = spark.createDataFrame(data)
df.write.mode('overwrite').saveAsTable(f"{SCHEMA}.compras_replace_where")

# COMMAND ----------

data = [
    {"id_compra": 6, "id_pessoa": 3, "id_produto": 2, "valor": 125.0, "data_de_compra": "2022-01-06"},
    {"id_compra": 8, "id_pessoa": 2, "id_produto": 1, "valor": 55.0, "data_de_compra": "2022-01-09"},
    {"id_compra": 9, "id_pessoa": 2, "id_produto": 1, "valor": 175.0, "data_de_compra": "2022-01-10"},
    {"id_compra": 10, "id_pessoa": 2, "id_produto": 1, "valor": 175.0, "data_de_compra": "2022-01-11"}
]

df_replace = spark.createDataFrame(data)

condition = "data_de_compra>='2022-01-06' AND data_de_compra<='2022-01-12'"

(df_replace.write.mode("overwrite") 
            .option("replaceWhere", condition)  
            .saveAsTable(f"{SCHEMA}.compras_replace_where"))

spark.table(f"{SCHEMA}.compras_replace_where").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Upsert
# MAGIC <p>
# MAGIC   Upsert é o caso de update e insert. Se caracteriza pelo insert de dados que não existem na tabela e update de dados novos. É muito útil para tabelas de cadastro que precisam ser atualizadas, por exemplo uma tabela de clientes que possui colunas id_cliente, nome, sobrenome, telefone, update_date podemos filtrar a data de update da tabela origem e atualizar a tabela destino somente com o último registro do cliente.
# MAGIC </p>

# COMMAND ----------

#carga inicial

data = [
    {"id_cliente": 1, "nome": "John", "sobrenome": "Doe", "telefone": "555-1234", "update_date": "2022-01-01"},
    {"id_cliente": 2, "nome": "Jane", "sobrenome": "Smith", "telefone": "555-5678", "update_date": "2022-01-02"},
    {"id_cliente": 3, "nome": "Mike", "sobrenome": "Johnson", "telefone": "555-9876", "update_date": "2022-01-03"}
]

df = spark.createDataFrame(data)
df.write.mode('overwrite').saveAsTable(f"{SCHEMA}.cliente_upsert")
spark.table(f"{SCHEMA}.cliente_upsert").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Surgiu 3 novos clientes na tabela e o cliente de id 1 mudou o número e deseja inserir essa nova informação na tabela. A condition é onde a tabela vai verificar valores iguais, importante notar que no dataframe que vai ser inserido na tabela esse condition não pode ser duplicada (como ter dois ids iguais). Existem muitas opções de update e insert que podem ser lidos a baixo, whenMatchedUpdateAll quando tem um id igual atualiza todas as colunas e whenNotMatchInsertAll insere todas as colunas em caso de não achar o id. Tem também a opção whenNotMatchedBySourceDelete que é muito útil para full loads que quando o dado não existe mais na source ele é deletado na tabela destino.
# MAGIC </p>
# MAGIC
# MAGIC <p>Leitura adicional: <a href="https://docs.delta.io/latest/delta-update.html">https://docs.delta.io/latest/delta-update.html</a></p>

# COMMAND ----------

upsert_data = [
    {"id_cliente": 4, "nome": "Sarah", "sobrenome": "Jones", "telefone": "555-4321", "update_date": "2022-01-04"},
    {"id_cliente": 5, "nome": "Adam", "sobrenome": "Brown", "telefone": "555-8765", "update_date": "2022-01-05"},
    {"id_cliente": 1, "nome": "John", "sobrenome": "Doe", "telefone": "555-2345", "update_date": "2022-01-06"},
    {"id_cliente": 6, "nome": "Michael", "sobrenome": "Williams", "telefone": "555-6543", "update_date": "2022-01-07"}
]

df_upsert = spark.createDataFrame(upsert_data)

# COMMAND ----------

import pyspark.sql.functions as F
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, f"{SCHEMA}.cliente_upsert")

condition = "table.id_cliente = upsert.id_cliente"

(delta_table
    .alias("table")
    .merge(df_upsert.alias("upsert"), condition)
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

spark.table(f"{SCHEMA}.cliente_upsert").display()

# COMMAND ----------

# MAGIC %md #Alterando Tabela
# MAGIC

# COMMAND ----------

# MAGIC %md ## Comentários
# MAGIC
# MAGIC <p>
# MAGIC   Comentários em tabelas e colunas são importantes para documentar adequadamente os dados em ambientes de análise de dados como o Databricks. Eles ajudam a garantir a compreensão, consistência e colaboração entre os membros da equipe. É fundamental que os usuários do Databricks documentem adequadamente seus dados por meio de comentários em tabelas e colunas.
# MAGIC </p>
# MAGIC
# MAGIC <p>
# MAGIC   Gerado pelo Chat GPT
# MAGIC </p>

# COMMAND ----------

df_pessoas.write.mode("overwrite").saveAsTable(f"{SCHEMA}.pessoas")

spark.sql(f"DESCRIBE TABLE {SCHEMA}.pessoas").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Comentando tabela e colunas

# COMMAND ----------

spark.sql(f"COMMENT ON TABLE {SCHEMA}.pessoas IS 'tabela contendo dados sobre os clientes da lojas'")
spark.sql(f"ALTER TABLE {SCHEMA}.pessoas CHANGE id_pessoa id_pessoa bigint COMMENT 'identificado do cliente'")
spark.sql(f"ALTER TABLE {SCHEMA}.pessoas CHANGE idade idade bigint COMMENT 'idade do cliente'")
spark.sql(f"ALTER TABLE {SCHEMA}.pessoas CHANGE nome nome string COMMENT 'nome do cliente (PII)'")
spark.sql(f"ALTER TABLE {SCHEMA}.pessoas CHANGE sexo sexo string COMMENT 'sexo do cliente'")

# COMMAND ----------

spark.sql(f"DESCRIBE TABLE EXTENDED {SCHEMA}.pessoas").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## LIMPEZA DO AMBIENTE DO USUARIO

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
dbutils.fs.rm(save_path, True)
