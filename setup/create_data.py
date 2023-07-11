# Databricks notebook source
class ConfigureDatabases:
    def __init__(self):
        self.schema_list = ["raw", "bronze", "silver", "gold"]

    def start_course(self):
        self._create_databases()

    def _create_databases(self):
        for schema in self.schema_list:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema} LOCATION '/tmp/{schema}.db'")

    def finish_course(self):
        for schema in self.schema_list:
            spark.sql(f"DROP SCHEMA {schema}")

# COMMAND ----------

configure = ConfigureDatabases()
configure.start_course()

# COMMAND ----------

configure.finish_course()
