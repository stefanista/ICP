import sys
import os

os.environ["SPARK_HOME"] = "D:\\spark-2.3.1-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]="D:\\Drive\\winutils"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

"""
This is use for create streaming of text from txt files that creating dynamically 
from files.py code. This spark streaming will execute in each 3 seconds and It'll
show number of words count from each files dynamically
"""



    sc = SparkContext(appName="PysparkStreaming")
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)
ssc = StreamingContext(sc, 3)
lines = ssc.socketTextStream("localhost", 9999)
counts = lines.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
counts.pprint()
ssc.start()
ssc.awaitTermination()


if __name__ == "__main__":
    main()
