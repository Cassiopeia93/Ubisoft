
#Generate a column which gives the total amount spent per customer in the last 10 minutes (without including the current transaction).

from pyspark.sql import Window
import pyspark.sql.functions as F 

df = schemaTransaction.withColumn('timestamp',schemaTransaction.order_datetime.astype('Timestamp').cast("long"))


exclude_current = F.udf(lambda lst: sum(lst[:-1]) if lst[:-1] else 0)


windowSpec = Window.partitionBy("customerid").orderBy("timestamp").rangeBetween(-600, 0)
df1 =(
    df.withColumn("last_10_Min", exclude_current(F.collect_list("amount_eur").over(windowSpec)))
)

df1.show()

