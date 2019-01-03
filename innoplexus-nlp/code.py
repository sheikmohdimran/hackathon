# pyspark --driver-memory 6G
# Read HTML data csv file into spark
data = spark.read.format("csv").option("quote", "\"").option("escape", "\"").option("header", "true").option("multiline","true").load("/u01/workspace/02-hackathon/html_data.csv")

#Import BS4 for clearing HTML tags
from bs4 import BeautifulSoup
def formathtml(doc):
 soup = BeautifulSoup(str(doc), "lxml")
 return soup.text

#Register objects to be used in Spark SQK
sqlContext.registerFunction("formathtml", formathtml)
data.registerTempTable("data")

#Clear HTML tags from input file
clean_data=sqlContext.sql("SELECT Webpage_id,formathtml(Html) as Html from data")
clean_data.registerTempTable("clean_data")

#Load train dataset
train=spark.read.format("csv").option("header", "true").load("/u01/workspace/02-hackathon/train.csv").select("Webpage_id","Tag")
train.registerTempTable("train")

#Join train with clean_data on webpage_id
final_train=sqlContext.sql("SELECT train.Webpage_id,train.Tag,clean_data.Html from clean_data,train where clean_data.Webpage_id=train.Webpage_id")
final_train.registerTempTable("final_train")

#Import libraries for ML
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#Tokenizer
regexTokenizer = RegexTokenizer(inputCol="Html", outputCol="words", pattern="\\W")
# stop words
add_stopwords = ["Twitter","LinkedIn","Search","Menu","and","navigation","the","by","in","a","that","are","you","i","of","to","as"] 
stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(add_stopwords)
# bag of words count
countVectors = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=10000, minDF=5)

#StringIndexer
label_stringIdx = StringIndexer(inputCol = "Tag", outputCol = "label")
pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, countVectors, label_stringIdx])
# Fit the pipeline to training documents.
pipelineFit = pipeline.fit(final_train)
pipelineFit.save("/u01/workspace/02-hackathon/pipeline")
dataset = pipelineFit.transform(final_train)

# set seed for reproducibility
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)

# Create LR model
lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
lrModel = lr.fit(trainingData)
lrModel.save("/u01/workspace/02-hackathon/model")

#Load Test data
test=spark.read.format("csv").option("header", "true").load("/u01/workspace/02-hackathon/test.csv").select("Webpage_id")
test.registerTempTable("test")

#Join test dataset with HTML content
final_test=sqlContext.sql("SELECT test.Webpage_id,clean_data.Html from clean_data,test where clean_data.Webpage_id=test.Webpage_id")
final_test.registerTempTable("final_test")

#Fit the cleaned test data set on to pipeline
test_dataset = pipelineFit.transform(final_test)

#Generate predictions
predictions = lrModel.transform(test_dataset)
predictions.registerTempTable("predictions")

#String Indexer to map predictions with tags
indexer = StringIndexer(inputCol="Tag", outputCol="TagIndex")
indexed = indexer.fit(train).transform(train)
indexed.registerTempTable("indexed")
index_out=sqlContext.sql("SELECT distinct(CONCAT(Tag, '-',  TagIndex)) as val1 FROM indexed")

index_out.registerTempTable("index_out")
reference=sqlContext.sql("SELECT split(val1,'-')[0] as Tag,split(val1,'-')[1] as prediction from index_out")
reference.registerTempTable("reference")

# Save the final CSV output
joined_output=sqlContext.sql("SELECT predictions.Webpage_id,reference.Tag from predictions,reference where predictions.prediction=reference.prediction")
joined_output.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/tmp/submn")