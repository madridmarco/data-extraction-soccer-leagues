import requests
import pandas as pd
from pandas import json_normalize
import http.client
import ssl
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder 
    .appName("countries") 
    .getOrCreate()
)

ssl_context = ssl._create_unverified_context()
http.client.HTTPSConnection("v3.football.api-sports.io", context=ssl_context)

credencials = {
    "x-rapidapi-host": "v3.football.api-sports.io",
    "x-rapidapi-key": "a7651e81f75b7516319f70fd91663320"
}

url_paises = "https://v3.football.api-sports.io/countries"
response = requests.request("GET", url_paises, headers=credencials)
data_paises = json_normalize(response.json()['response'])
data_paises = spark.createDataFrame(data_paises)

hudiOptions = {
        "className": "org.apache.hudi",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.database": "football_bronce",
        "hoodie.datasource.hive_sync.table": "countries",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.partition_fields": "code",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.table.name": "",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
        "hoodie.datasource.write.recordkey.field": "code",
        "hoodie.datasource.write.partitionpath.field": "code",
        "hoodie.datasource.write.table.name": "countries",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.metadata.enable": False,
        "hoodie.parquet.compression.codec": "snappy",
        "hoodie.datasource.write.hive_style_partitioning": True,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.precombine.field": "code",
        "hoodie.upsert.shuffle.parallelism": 120,
        "hoodie.insert.shuffle.parallelism": 120,
        "hoodie.bulkinsert.shuffle.parallelism": 120,
        "hoodie.write.concurrency.mode": "single_writer",
        "hoodie.cleaner.policy.failed.writes": "EAGER"
    }
    
data_paises.write.format("hudi").mode("append").options(**hudiOptions).save("s3://football-hist-bronce/countries/")