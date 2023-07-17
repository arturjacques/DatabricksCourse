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
# MAGIC <h2>Trazer os dados do que está dentro da pasta "compras"</h2>
# MAGIC
# MAGIC <p>Adicionar metadados (arquivo de origem, data de carga, nome da source)</p>
# MAGIC <p>salvar no formato delta em uma tabela no schema bronze.</p>

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit, when, col
from pyspark.sql import DataFrame

# COMMAND ----------

checkpoint_location_bronze = '/tmp/compras_bronze_checkpoint'
schema_location = '/tmp/compras_schema'
bronze_table = 'bronze.compras'

checkpoint_location_silver = '/tmp/compras_silver_checkpoint'
silver_table = 'silver.compras'

# COMMAND ----------

dbutils.fs.rm(checkpoint_location_bronze, True)
dbutils.fs.rm(schema_location, True)
spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")

dbutils.fs.rm(checkpoint_location_silver, True)
spark.sql(f"DROP TABLE IF EXISTS {silver_table}")

# COMMAND ----------

checkpoint_location_bronze = '/tmp/compras_bronze_checkpoint'
schema_location = '/tmp/compras_schema'

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv") 
    .option("cloudFiles.schemaLocation",schema_location) 
    .load(data_path+'/compras')
    .withColumn('origin_file_name', input_file_name())
    .withColumn('extraction_date', current_timestamp())
    .withColumn('data_source', lit('compras'))
    .writeStream
    .outputMode("append")
    .trigger(once = True)
    .option("checkpointLocation", checkpoint_location_bronze)
    .option("mergeSchema", "True")
    .toTable(bronze_table)
)

stream.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM bronze.compras

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2>Ler da bronze e salvar na silver (corrigindo alguns erros)</h2>

# COMMAND ----------

def trocar_valor_para_null(df: DataFrame, valor: str, colunas: list)->DataFrame:
    for coluna in colunas:
         df = (df
                .withColumn(coluna, when(col(coluna)==valor, None)           
                .otherwise(col(coluna))
            ))
    return df

def transformar_bronze_compras(df: DataFrame)->DataFrame:
    trocar_none_por_null_columns = ['id', 'user_id', 'value', 'product_id', 'product_qt', 'country']

    df_null = trocar_valor_para_null(df, 'None', trocar_none_por_null_columns)

    return (df_null
        .drop('_rescued_data')
        .withColumn('id', col('id').cast('bigInt'))
        .withColumn('user_id', col('user_id').cast('string'))
        .withColumn('value', col('value').cast('decimal(16,2)'))
        .withColumn('product_id', col('product_id').cast('string'))
        .withColumn('product_qt', col('product_qt').cast('bigInt'))
    )

def transformar_e_salvar_tabela(df: DataFrame, batch_id: int):
    df_transformed = transformar_bronze_compras(df)

    df_transformed.write.mode('append').saveAsTable(silver_table)

# COMMAND ----------

checkpoint_location_silver = '/tmp/compras_silver_checkpoint'

stream = (spark
            .readStream
            .format('delta')
            .table(bronze_table)
            .writeStream
            .foreachBatch(transformar_e_salvar_tabela)
            .option("checkpointLocation", checkpoint_location_silver)
            .trigger(once=True)
            .start())

stream.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h1>Mostrar históricos da tabela</h1>

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY bronze.compras

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY silver.compras

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h1>Comentar as colunas e a tabela da silver com o que é cada coluna e a tabela</h1>

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COMMENT ON TABLE silver.compras IS 'Tabela de compras em um e-commerce';
# MAGIC ALTER TABLE silver.compras CHANGE id id bigint COMMENT "id único de compra";
# MAGIC ALTER TABLE silver.compras CHANGE user_id user_id string COMMENT "id do usuário";
# MAGIC ALTER TABLE silver.compras CHANGE value value decimal(16,2) COMMENT "valor total da compra";
# MAGIC ALTER TABLE silver.compras CHANGE product_id product_id string COMMENT "id do produto comprado";
# MAGIC ALTER TABLE silver.compras CHANGE product_qt product_qt bigint COMMENT "quantidade do produto comprado";
# MAGIC ALTER TABLE silver.compras CHANGE country country string COMMENT "país de compra";
# MAGIC ALTER TABLE silver.compras CHANGE origin_file_name origin_file_name string COMMENT "nome do arquivo da origem do dado";
# MAGIC ALTER TABLE silver.compras CHANGE extraction_date extraction_date timestamp COMMENT "data que o arquivo de origem foi lido e adicionado na tabela bronze";
# MAGIC ALTER TABLE silver.compras CHANGE data_source data_source string COMMENT "a o nome da source da informação";

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED silver.compras

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h1>BONUS otimizar as tabelas</h1>

# COMMAND ----------

spark.sql("ALTER TABLE bronze.compras SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = 'true', delta.autoOptimize.autoCompact = 'true')")
spark.sql("ALTER TABLE silver.compras SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = 'true', delta.autoOptimize.autoCompact = 'true')")
