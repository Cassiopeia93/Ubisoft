
#Generate a column which gives the total amount spent per customer in the last 10 minutes (without including the current transaction).

!pip install pyspark
from pyspark import SparkContext, SQLContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from pyspark.sql import Window
from pyspark.sql import functions as f
sqlContext.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

#transactionDataFrame.withColumn('sumres', sum(col("address"))).over(w).rowsBetween(-3, -1)
transactionDataFrame=transactionDataFrame.withColumn("current_date",psf.unix_timestamp(psf.current_date()))
transactionDataFrame=transactionDataFrame.withColumn('datetime', psf.unix_timestamp(psf.col('order_datetime'))).withColumn('10min_delta', (psf.col('current_date') - psf.lag('datetime').over(w)) / 60 <= 10) 

result=transactionDataFrame.withColumn('amount_spent_last_ten_minutes', f.sum('amount_eur').over(Window.partitionBy('customerid').orderBy('order_datetime').rowsBetween(-3, -1)))
result=result.where(f.col("10min_delta").isin({True}))
result.orderBy('customerid').show()
