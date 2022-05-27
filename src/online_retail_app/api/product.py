from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.linalg import SparseVector, DenseVector, VectorUDT
from pyspark.sql.types import StructType, DoubleType, StructField, StringType
import tempfile
import os
import pandas as pd

def tokenize(input_df):
    tokenizer = Tokenizer(inputCol="Description", outputCol="words")
    wordsData = tokenizer.transform(input_df)

    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=100)
    featurizedData = hashingTF.transform(wordsData)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    output_df = idfModel.transform(featurizedData)
    return output_df



class ProductDataFrame:
    def __init__(self):
        self.spark = SparkSession.builder.appName('online-retail').getOrCreate()
        self.load_csf_spark(f'{os.getcwd()}/data/online_retail.csv')
        self.df = self.spark.createDataFrame(data = [], schema = StructType([StructField('Description',
                                  StringType(), True)]))
        # self.transform()

    def load_csf_spark(self, file_location):
        self.file_location = file_location
        print('loading csv file with spark')
        self.df = self.spark.read.csv(self.file_location, header='true', inferSchema='true')\
            .dropna(subset=['Description']).withColumn('Description', f.trim(f.col('Description')))

    def set_df(self, df):
        self.df = df

    def transform(self):
        self.transformed_df = tokenize(self.df)
        return self.transformed_df

    def relative_products(self, product_description):
        dataDictionary = [{'product_id': 10, 'Description':product_description}]

        test_df = self.spark.createDataFrame(data=dataDictionary)

        vector = tokenize(test_df).head()['rawFeatures']
        def dot_fun(x, y):
            if x is not None and y is not None:
                return float(x.dot(y))

        dot = f.udf(dot_fun, DoubleType())
        fillVector = f.udf(lambda z: vector,VectorUDT())
        if not self.transformed_df:
            return {"error": "pipeline was not loaded"}
        result = self.transformed_df.dropDuplicates(['StockCode']).withColumn('todo',fillVector('features')).withColumn("product", dot("features","todo")).sort(f.col("product").desc())
        return [row.asDict() for row in result.head(10)]

    def load_df(self):
        filePath = 'https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx'
        file_location = '/tmp/online_retail.csv'
        # self.file_location = tempfile.TemporaryFile()
        # print(self.file_location)
        print('Downloading file to csv using pandas dataframe')
        df = pd.read_excel(filePath).fillna({'CustomerID': 0})
        df.to_csv(file_location, index=False)
        
        self.load_csf_spark(file_location)
        return self.df