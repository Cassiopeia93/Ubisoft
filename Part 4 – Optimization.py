# You will find a script below which creates some features. How would you optimize it? Please describe a few potential solutions.

# first you need to persist store the transaction table 
# and then the user table doesn't affect the result so don't need to do the join

def nb_purchases(date):
 return len(date)
nb_purchases_udf = udf(nb_purchases, IntegerType())

schemaTransaction\
.groupby("customerid").agg(
 F.min("amount_eur").alias("min_amount_eur"),
 F.max("amount_eur").alias("max_amount_eur"),
 nb_purchases_udf(F.collect_list("order_date")).alias("nb_purchases")
).show()
