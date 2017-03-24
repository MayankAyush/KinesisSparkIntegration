from future import print_function
import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
sc = SparkContext()
ssc = StreamingContext(sc, 1)
streamName = 'StreamName'
appName = 'AppName'
endpointUrl = 'https://kinesis.us-east-1.amazonaws.com'
regionName = 'us-east-1'
dstream = KinesisUtils.createStream(ssc, appName, streamName, endpointUrl, regionName, InitialPositionInStream.TRIM_HORIZON, 5)
py_rdd = dstream.map(lambda x: json.loads(x))
py_rdd.saveAsTextFile("s3n://dev_temp/kinesis_spark_test/output.txt")
ssc.start()
ssc.awaitTermination()
ssc.stop()

