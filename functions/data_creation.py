from random import (
                randint,
                choice
)

import uuid

produtos = [{"id_product": 1, "nome do produto": "camiseta", "preço": 25.00, "peso": 0.2},
            {"id_product": 2, "nome do produto": "calça", "preço": 50.00, "peso": 0.4},
            {"id_product": 3, "nome do produto": "tênis", "preço": 100.00, "peso": 0.8},
            {"id_product": 4, "nome do produto": "mochila", "preço": 80.00, "peso": 1.0},
            {"id_product": 5, "nome do produto": "óculos", "preço": 150.00, "peso": 0.05},
            {"id_product": 6, "nome do produto": "relógio", "preço": 200.00, "peso": 0.1},
            {"id_product": 7, "nome do produto": "fones de ouvido", "preço": 120.00, "peso": 0.15},
            {"id_product": 8, "nome do produto": "carregador", "preço": 30.00, "peso": 0.1}]

pessoas = [
    {"id_pessoa": 1, "nome": "Ana", "idade": 25, "sexo": "F"},
    {"id_pessoa": 2, "nome": "Bruno", "idade": 30, "sexo": "M"},
    {"id_pessoa": 3, "nome": "Carla", "idade": 28, "sexo": "F"}
]


def create_compras_de_usuario(spark, rows=20):
    compras_list = []


    for _ in range(rows):
        user = choice(pessoas)
        compra_id = str(uuid.uuid4())

        for quantidade in range(randint(0, 10)):
            produto = choice(produtos)
            compras_list.append(
                {
                    "compra_id": compra_id,
                    "id_pessoa": user["id_pessoa"],
                    "id_product": produto["id_product"],
                    "valor": produto["preço"]
                }
            )

    return spark.createDataFrame(compras_list)