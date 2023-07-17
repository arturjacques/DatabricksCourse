# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <h2>Testar função que agrupa valor comprado por pessoa em cada país</h2>
# MAGIC
# MAGIC <p>A chave do usuário é unica somente dentro do país</p>
# MAGIC <p>Usuário e país formam uma chave primária</p>
# MAGIC <p>A função deve receber um DataFrame e retornar um DataFrame</p>
# MAGIC <p>A coluna de soma deve se chamar "total_value"</p>
# MAGIC

# COMMAND ----------

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from unittest import TestCase
from unittest.mock import patch, Mock

# COMMAND ----------

def valor_comprador_por_usuario(df: DataFrame) -> DataFrame:
    return df.groupBy(['user_id', 'country']).agg(F.sum('value').alias('total_value'))

# COMMAND ----------

df = spark.table('silver.compras')

valor_comprador_por_usuario(df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2>Exemplo de test</h2>

# COMMAND ----------

def test_value_bought_by_user():
    #prepare
    df_data = [
        {
            "user_id": 1,
            "country": "BR",
            "value": 2.00
        },
        {
            "user_id": 1,
            "country": "BR",
            "value": 4.00
        },
        {
            "user_id": 1,
            "country": "MX",
            "value": 2.50
        },
        {
            "user_id": 2,
            "country": "MX",
            "value": 4.50
        }
    ]

    df = spark.createDataFrame(df_data)

    expected_data = [
        {
            "user_id": 1,
            "country": "BR",
            "total_value": 6.00,
        },
        {
            "user_id": 1,
            "country": "MX",
            "total_value": 2.50,
        },
        {
            "user_id": 2,
            "country": "MX",
            "total_value": 4.50,
        }
    ]

    #call
    df_returned = valor_comprador_por_usuario(df)

    #assert
    df_returned_collect = [row.asDict() for row in df_returned.collect()]
    assert df_returned_collect==expected_data

# COMMAND ----------

test_value_bought_by_user()

# COMMAND ----------

class TestFunctions(TestCase):
    def test_value_bought_by_user(self):
        #prepare
        df_data = [
            {
                "user_id": 1,
                "country": "BR",
                "value": 2.00
            },
            {
                "user_id": 1,
                "country": "BR",
                "value": 4.00
            },
            {
                "user_id": 1,
                "country": "MX",
                "value": 2.50
            },
            {
                "user_id": 2,
                "country": "MX",
                "value": 4.50
            }
        ]

        df = spark.createDataFrame(df_data)

        expected_data = [
            {
                "user_id": 1,
                "country": "BR",
                "total_value": 6.00,
            },
            {
                "user_id": 1,
                "country": "MX",
                "total_value": 2.50,
            },
            {
                "user_id": 2,
                "country": "MX",
                "total_value": 4.50,
            }
        ]

        #call
        df_returned = valor_comprador_por_usuario(df)

        #assert
        df_returned_collect = [row.asDict() for row in df_returned.collect()]
        self.assertEqual(df_returned_collect, expected_data)

# COMMAND ----------

TestFunctions().test_value_bought_by_user()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2>Testar funções a baixo</h2>

# COMMAND ----------

def limpar_nome_das_colunas(df:DataFrame)->DataFrame:
    """remove os caracteres " ", ",", ";", "{", "}", "(", ")", "\n", "\t", "="
    do nome da coluna para poder ser salvo.

    Args:
        df (DataFrame): DataFrame que será salvo.

    Returns:
        DataFrame: DataFrame renomeado as colunas
    """
    caracters_especiais  = [' ', ',', ';', '{', '}', '(', ')', '\n', '\t', '=']
    df_renomeado = df

    for coluna in df.columns:
        novo_nome_da_coluna = coluna

        for caracters in caracters_especiais:
            novo_nome_da_coluna = novo_nome_da_coluna.replace(caracters, '')

        if novo_nome_da_coluna != coluna:
            df_renomeado = df_renomeado.withColumnRenamed(coluna, novo_nome_da_coluna)
    
    return df_renomeado

# COMMAND ----------

def renomear_columnas_com_um_dicionario(df: DataFrame, dicionario_renomear: dict) -> DataFrame:
    """renomeia um DataFrame com um dicionário onde a chave é o valor antigo
    e o valor o nome novo.

    Args:
        df (DataFrame): DataFrame que será salvo.
        dicionario_renomear (DataFrame): dicionário com chave de que é o nome da coluna
        e valor que é o novo nome da coluna.

    Returns:
        DataFrame: DataFrame renomeado as colunas
    """
    for key, value in dicionario_renomear.items():
        df = df.withColumnRenamed(key, value)
     
    return df

# COMMAND ----------

def trocar_valor_para_null(df: DataFrame, valor: str, colunas: list)->DataFrame:
    """Recebe 3 parametros o DataFrame com valores como string que são nulos mas não estão
    como nulos. O Valor são essas strings que são o valor nulo e colunas é uma lista de 
    colunas para alterar o valor para nulo.

    Args:
        df (DataFrame): DataFrame que possui strings nula.

    Returns:
        DataFrame: DataFrame com as linhas com os valores substituídos
        por nulo
    """
    for coluna in colunas:
         df = (df
                .withColumn(coluna, F.when(F.col(coluna)==valor, None)
                .otherwise(F.col(coluna))
            ))
    return df

def get_widget(nome_do_widget: str, valor_default: str='', nome_aparente_do_widget: str='')-> str:
    """Tem como função criar o widget com um valor padrão e um nome aparente

    Args:
        nome_do_widget (str): Nome do widget para ser pego.
        valor_default (str, opcional): valor default do widget.
        nome_aparente_do_widget (str, opcional): nome do widget na barra de widgets.

    Returns:
        str: retorna valor do widget
    """
    if nome_aparente_do_widget == '':
        nome_aparente_do_widget = nome_do_widget
    
    dbutils.widgets.text(nome_do_widget, valor_default, nome_aparente_do_widget)
    return dbutils.widgets.get(nome_do_widget)

# COMMAND ----------

def trocar_valor_para_null(df: DataFrame, valor: str, colunas: list)->DataFrame:
    """Recebe 3 parametros o DataFrame com valores como string que são nulos mas não estão
    como nulos. O Valor são essas strings que são o valor nulo e colunas é uma lista de 
    colunas para alterar o valor para nulo.

    Args:
        df (DataFrame): DataFrame que possui strings nula.

    Returns:
        DataFrame: DataFrame com as linhas com os valores substituídos
        por nulo
    """
    for coluna in colunas:
        df = (df
                .withColumn(coluna, F.when(F.col(coluna)==valor, None)
                .otherwise(F.col(coluna))
            ))
    return df

def get_widget(nome_do_widget: str, valor_default: str='', nome_aparente_do_widget: str='')-> str:
    """Tem como função criar o widget com um valor padrão e um nome aparente

    Args:
        nome_do_widget (str): Nome do widget para ser pego.
        valor_default (str, opcional): valor default do widget.
        nome_aparente_do_widget (str, opcional): nome do widget na barra de widgets.

    Returns:
        str: retorna valor do widget
    """
    if nome_aparente_do_widget == '':
        nome_aparente_do_widget = nome_do_widget
    
    dbutils.widgets.text(nome_do_widget, valor_default, nome_aparente_do_widget)
    return dbutils.widgets.get(nome_do_widget)





# COMMAND ----------

class TestFunctions2(TestCase):
    def test_limpar_nome_das_colunas(self):
    #prepare
        dataframe_data = [
            {
                "column 1": "1",
                "column 2": "2",
                """column
                3""": "3",
                "column,;{}()4": "4",
            }
        ]

        df = spark.createDataFrame(dataframe_data)

        expected_data = [
            {
                "column1": "1",
                "column2": "2",
                "column3": "3",
                "column4": "4",
            }
        ]

        #call
        df_returned = limpar_nome_das_colunas(df)

        #assert
        dict_df = [row.asDict() for row in df_returned.collect()]
        self.assertEqual(dict_df, expected_data)

    def test_renomear_columnas_com_um_dicionario(self):
        #prepare
        dataframe_data = [
            {"id": 1,"name": "João","country": "MX", "update_at": "2022-01-01"},
            {"id": 6,"name": "Ana","country": "MX", "update_at": "2023-02-05"},
            {"id": 3,"name": "Barbora","country": "CO", "update_at": "2022-08-01"},
            {"id": 12,"name": "James","country": "BO", "update_at": "2023-04-01"}
        ]
        df = spark.createDataFrame(dataframe_data)

        rename_columns = {
            'name': 'user_name',
            'country': 'country_cd'
        }

        expected_result = [
            {"id": 1,"user_name": "João","country_cd": "MX", "update_at": "2022-01-01"},
            {"id": 6,"user_name": "Ana","country_cd": "MX", "update_at": "2023-02-05"},
            {"id": 3,"user_name": "Barbora","country_cd": "CO", "update_at": "2022-08-01"},
            {"id": 12,"user_name": "James","country_cd": "BO", "update_at": "2023-04-01"}
        ]

        #call
        df_returned = renomear_columnas_com_um_dicionario(df, rename_columns)

        #assert
        dict_df = [row.asDict() for row in df_returned.collect()]
        self.assertEqual(dict_df, expected_result)

    def test_trocar_valor_para_null(self):
        #prepare
        dataframe_data = [
            {"id": 1,"name": "None","country": "MX", "update_at": "2022-01-01"},
            {"id": 6,"name": "Ana","country": "None", "update_at": "2023-02-05"},
            {"id": 3,"name": "None","country": "CO", "update_at": "None"},
            {"id": 12,"name": "James","country": "None", "update_at": "2023-04-01"}
        ]
        
        df = spark.createDataFrame(dataframe_data)

        trocar_para_null_columns = ["name", "update_at"]
        valor_nulo = "None"

        expected_data = [
            {"id": 1,"name": None, "country": "MX", "update_at": "2022-01-01"},
            {"id": 6,"name": "Ana","country": "None", "update_at": "2023-02-05"},
            {"id": 3,"name": None, "country": "CO", "update_at": None},
            {"id": 12,"name": "James","country": "None", "update_at": "2023-04-01"}
        ]

        #call
        df_returned = trocar_valor_para_null(df, valor_nulo, trocar_para_null_columns)

        #assert
        dict_df = [row.asDict() for row in df_returned.collect()]
        self.assertEqual(dict_df, expected_data)

    @patch('__main__.dbutils')
    def test_get_widget(self, mock_dbutils):
        #prepare
        nome = "teste_widget"
        default = "valor_padrão"
        nome_aparente = '1_teste_widget'

        #call
        returned_value = get_widget(nome, default, nome_aparente)

        #assert
        mock_dbutils.widgets.text.assert_called_with(nome, default, nome_aparente)
        mock_dbutils.widgets.get.assert_called_with(nome)
        self.assertEqual(mock_dbutils.widgets.get(), returned_value)

    @patch('__main__.dbutils')
    def test_get_widget_somente_nome(self, mock_dbutils):
        #prepare
        nome = "teste_widget"

        #call
        returned_value = get_widget(nome)

        #assert
        mock_dbutils.widgets.text.assert_called_with(nome, "", nome)
        mock_dbutils.widgets.get.assert_called_with(nome)
        self.assertEqual(mock_dbutils.widgets.get(), returned_value)

# COMMAND ----------

obj_test = TestFunctions2()
obj_test.test_limpar_nome_das_colunas()
obj_test.test_renomear_columnas_com_um_dicionario()
obj_test.test_trocar_valor_para_null()
obj_test.test_get_widget()
