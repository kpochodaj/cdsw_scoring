# import dependencies

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer

# launch Spark session
spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .master("local[*]") \
    .getOrCreate()   

# Prepare training documents from a list of (id, text, label) tuples.
training = spark.createDataFrame([
    (0, "a b c d e spark", 1.0),
    (1, "b d", 0.0),
    (2, "spark f g h", 1.0),
    (3, "hadoop mapreduce", 0.0)
], ["id", "text", "label"])

# Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
lr = LogisticRegression(maxIter=10, regParam=0.001)
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

# Fit the pipeline to training documents.
model = pipeline.fit(training)

# save in local file system
model.save("file:/home/cdsw/models/spark_serialized")
# save in HDFS
model.save("/tmp/spark_serialized")

# .....
# Prepare new unlabeled (id, text) tuples.
new = spark.createDataFrame([
    (4, "spark i j k"),
    (5, "l m n"),
    (6, "spark hadoop spark"),
    (7, "apache hadoop")
], ["id", "text"])

# load model
loaded_model = model.load('file:/home/cdsw/models/spark_serialized')

# predict using loading model
prediction = loaded_model.transform(new)

# Display columns of interest.
selected = prediction.select("id", "text", "probability", "prediction")
for row in selected.collect():
    rid, text, prob, prediction = row
    print("(%d, %s) --> prob=%s, prediction=%f" % (rid, text, str(prob), prediction))

# save to local storage
selected.select("id", "prediction").write.csv("file:/home/cdsw/output/text_prediction.csv")

# save to Impala
selected \
  .write.format("parquet")\
  .mode("overwrite")\
  .saveAsTable('default.text_prediction')

# display result
spark.sql("select * from default.text_prediction").show()
  
# save to HDFS
selected.select("id", "prediction").write.save("/tmp/text_prediction.csv")
!hadoop fs -ls /tmp
!hadoop fs -chmod a+rwx /tmp/text_prediction.csv
