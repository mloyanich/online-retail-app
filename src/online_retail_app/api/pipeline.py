from fastapi import APIRouter
import pandas as pd
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.linalg import SparseVector, DenseVector

router = APIRouter()


@router.get("/")
def read_pipeline(skip: int = 0, limit: int = 100):
    return {"status": "success!!"}


@router.post("/")
def start_pipeline():
    filePath = 'https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx'
    logging.info(f'Loading dataset from {filePath}')
    with SparkSession.builder.appName('online-retail').getOrCreate() as spark:
        df = pd.read_excel(filePath).fillna({'CustomerID': 0})
        file_location = 'online_retail.csv'
        df.to_csv(file_location, index_label='product_id')
        logging.info(f'CSV file saved at {file_location}')

        logging.info('Processing the data')
        sdf = spark.read.csv(file_location, header='true', inferSchema='true')\
            .dropna(subset=['Description']).withColumn('Description', f.trim(f.col('Description')))
        sdf.write.json("products_json", mode="overwrite")

        tokenizer = Tokenizer(inputCol="Description", outputCol="words")
        wordsData = tokenizer.transform(sdf)

        # TODO calculate number of features based on the full vocab there is in products
        num_features = 100
        hashingTF = HashingTF(
            inputCol="words", outputCol="rawFeatures", numFeatures=num_features)
        featurizedData = hashingTF.transform(wordsData)

        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)

        @f.udf("map<string, double>")
        def vector_array_as_map(w, v):
            if isinstance(v, SparseVector):
                return dict(zip(w, v.values.tolist()))
            elif isinstance(v, DenseVector):
                return dict(zip(w, v.values.tolist()))
        # columns = sdf.columns
        rescaledData = rescaledData.select(
            "product_id", f.explode(vector_array_as_map("words", "features")).alias("word", "word_rank"))
        rescaledData.write.json("ranking_json", mode="overwrite")

    return {"status": "pipeline run success!!"}
