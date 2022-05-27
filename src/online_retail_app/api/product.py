from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as f
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.linalg import SparseVector, DenseVector, VectorUDT
from pyspark.sql.types import StructType, DoubleType, StructField, StringType

import tempfile
import os
import pandas as pd

def tokenize(input_df, num_words):
    tokenizer = Tokenizer(inputCol="d_clean", outputCol="words")
    wordsData = tokenizer.transform(input_df)
    
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=num_words)
    featurizedData = hashingTF.transform(wordsData)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    output_df = idfModel.transform(featurizedData)
    return output_df



class ProductDataFrame:
    def __init__(self):
        self.spark = SparkSession.builder.appName('online-retail').getOrCreate()
        self.load_csf_spark(f'{os.getcwd()}/data/online_retail.csv')
        self.transform()

    def load_csf_spark(self, file_location):
        self.file_location = file_location
        print('loading csv file with spark')
        self.df = self.spark.read.csv(self.file_location, header='true', inferSchema='true')\
            .dropna(subset=['Description']).withColumn('Description', f.trim(f.col('Description')))
        self.columns = self.df.columns
        self.df = self.df.withColumn('d_clean', f.regexp_replace('Description', r'["\'-]', ''))\
                    .withColumn('d_clean', f.regexp_replace('d_clean', r'[@_!#$%^+&*()<>?/\|}{~:]', ' '))\
                    .withColumn('d_clean', f.regexp_replace('d_clean', r'\s+', ' '))
        words = self.df.select('d_clean').collect()
        self.num_words = len(set([x for item in words for x in item['d_clean'].split()]))*2

    def set_df(self, df):
        self.df = df
        self.transform()

    def transform(self):
        self.transformed_df = tokenize(self.df, self.num_words)
        return self.transformed_df

    def relative_products(self, product_description):
        dataDictionary = [{'product_id': 10, 'd_clean' : product_description}]

        test_df = self.spark.createDataFrame(data=dataDictionary)

        vector = tokenize(test_df,self.num_words).head()['rawFeatures']
        def dot_fun(x, y):
            if x is not None and y is not None:
                return float(x.dot(y))
        dot = f.udf(dot_fun, DoubleType())
        fillVector = f.udf(lambda z: vector,VectorUDT())
        if not self.transformed_df:
            return {"error": "pipeline was not loaded"}
        result = self.transformed_df.dropDuplicates(['StockCode'])\
            .withColumn('todo',fillVector('features'))\
                .withColumn("product", dot("features","todo"))\
                        .where("product>0")\
                            .select(*self.columns)\
                                .sort(f.col("product").desc())
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