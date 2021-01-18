# You will find a script below which creates some features. How would you optimize it? Please describe a few potential solutions.

from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
u = [(16130,'John Snow',46),
(76280,'Madame Bovary',72),
(14469,'Albus Dumbledore',118),
(55433,'Anne of Green Gables',12),
(67310,'Enola Holmes', 34),
(80959,'Don Quixote',61),
(38510,'Phileas Fogg',55),
(77224,'Lady Macbeth',30),
(88075,'Peer Gynt',38),
(84461,'Hercule Poirot',69),
(10959,'Ebenezer Scrooge',95),
(63656,'Elizabeth Bennet',23),
(74587,'Katniss Everdeen',18),
(73621,'Atticus Finch',47),
(93224,'Saleem Sinai',36),
(72946,'Holden Caulfield',21),
(34752,'Rodion Raskolnikov',34),
(33948,'Lisbeth Salander',24),
]
rdd = sc.parallelize(u)
users = rdd.map(lambda x: Row(customerid=x[0], name=x[1], age=x[2]))
dim_user = sqlContext.createDataFrame(users)
#def nb_purchases(date):
# return len(date)
#nb_purchases_udf = udf(nb_purchases, IntegerType())
schemaTransaction\
.join(F.broadcast(dim_user), on="customerid", how="inner")\
.groupby("customerid").agg(
 F.min("amount_eur").alias("min_amount_eur"),
 F.max("amount_eur").alias("max_amount_eur"),
 F.length(F.collect_list("order_date")).alias("nb_purchases")
).show()
