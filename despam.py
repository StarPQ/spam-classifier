from pyspark.ml import Pipeline
from pyspark.ml.feature import Word2Vec
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import IndexToString
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark import SparkConf
import os

VECTOR_SIZE = 100
print "hello"
