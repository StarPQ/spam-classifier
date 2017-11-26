from pyspark.ml import Pipeline
from pyspark.ml.feature import Word2Vec
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import IndexToString
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SQLContext
from pyspark.sql import DataFrameReader
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import rdd
import os

VECTOR_SIZE = 100
print "hello"
def deal(x):
    a, b = x.split('\t')
    c = []
    c = b.split(' ')
    return [a, c]
conf = SparkConf().setAppName("spark_streaming_kafka").setMaster("local[4]")
sc = SparkContext(conf=conf)
data = sc.textFile('/home/hadoop/Bigdata/project/trainsort')
data = data.map(lambda x: deal(x))
# data = data.map(lambda x: x[1].split(' '))
sqlCtx = SQLContext(sc)
print "---------------------------------------"
print data.first()
print "---------------------------------------"
msgDF = sqlCtx.createDataFrame(data, ["label","message"])
#print msgDF.collect()
# dfr = DataFrameReader()
# msgDF = dfr.json('/home/hadoop/Bigdata/project/trainsort')
# print msgDF.dtypes
# df = sqlCtx.read.format('json').load('/home/hadoop/Bigdata/project/trainsort')
# print "---------------------------------------"
# print df.dtypes
# print "---------------------------------------"

indexer = StringIndexer(inputCol="label", outputCol="indexed").fit(msgDF)
# indexed = indexer.transform(msgDF)
# print("Transformed string column '%s' to indexed column '%s'"
#       % (indexer.getInputCol(), indexer.getOutputCol()))
# indexed.show()

word2Vec = Word2Vec(vectorSize=VECTOR_SIZE, minCount=1, inputCol="message", outputCol="features")
# model = word2Vec.fit(msgDF)

# result = model.transform(msgDF)
# for row in result.collect():
#     text, vector = row
#     print("Text: [%s] => \nVector: %s\n" % (", ".join(text), str(vector)))

layers = [VECTOR_SIZE, 6, 5, 2]
trainer = MultilayerPerceptronClassifier(maxIter=128, layers=layers, blockSize=512, seed=1234L, featuresCol="features", labelCol="indexed", predictionCol="prediction")

labelConverter = IndexToString(inputCol="prediction", outputCol="predictedlabel", labels=indexer.labels)

trainingData, testData = msgDF.randomSplit([8.0, 2.0], 24)

pipeline = Pipeline(stages=[indexer, word2Vec, trainer, labelConverter])
model = pipeline.fit(trainingData)
ResultDF = model.transform(testData)
ResultDF.printSchema 
ResultDF.select("message","label","predictedlabel").show(30)
 
evaluator = MulticlassClassificationEvaluator(labelCol="indexed", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(ResultDF)
print accuracy