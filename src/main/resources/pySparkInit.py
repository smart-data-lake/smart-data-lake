from pyspark.java_gateway import launch_gateway
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame

# Initialize python spark session from java spark context.
# The java spark context is set as entrypoint for the py4j gateway.
gateway = launch_gateway()
entryPoint = gateway.entry_point
javaSparkContext = entryPoint.getJavaSparkContext()
sparkConf = SparkConf(_jvm=gateway.jvm, _jconf=javaSparkContext.getConf())
sc = SparkContext(conf=sparkConf, gateway=gateway, jsc=javaSparkContext)
session = SparkSession(sc, entryPoint.session())
sqlContext = SQLContext(sc, session, entryPoint.getSQLContext())
print("python spark session initialized (sc, session, sqlContext)")
