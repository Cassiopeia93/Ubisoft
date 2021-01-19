# Generate a column which gives the number of distinct characters for an address (e.g hello has 4 distinct characters).
# Generate a column which flags “True” if the address is located in “rue de Paris”.


from pyspark import SparkContext, SQLContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import col,udf
@udf(returnType='int')
def cnt(word):
  L=[]                           #create an empty list
  for letter in word:
      if letter not in L and letter != ' ':
          L.append(letter)       #append unique chars to list

  return len(L)                   #count the chars in list

@udf(returnType='boolean')
def check(word):
  if  "rue de Paris" in word:
    return True
  else:
    return False


transactionDataFrameTemp=schemaTransaction.withColumn('nb_distinct_char_in_address', cnt(col("address"))).withColumn('ls_located_in_rue_de_paris', check(col("address")))
transactionDataFrameTemp.collect()
