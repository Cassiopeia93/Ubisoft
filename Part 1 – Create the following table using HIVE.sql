-- Generate a column which gives the number of distinct IP address LTD (live to date) per customerId.
-- Generate a column which gives the amount spent UTD (up to date) per customerId.


SELECT TRANSACTION.*, 
       result.nb_address_ip_ltd, 
       Sum(TRANSACTION.amount_eur) 
         OVER( 
           partition BY TRANSACTION.customerid 
           ORDER BY TRANSACTION.order_datetime) amount_eur_utd 
FROM   (SELECT t.customerid, 
               Count(DISTINCT t.ip_address) AS nb_address_ip_ltd 
        -- Sum(t.amount_eur)            AS amount_eur_utd  
        FROM   schematransaction t  
        GROUP  BY t.customerid) result 
       JOIN schematransaction TRANSACTION 
         ON result.customerid = TRANSACTION.customerid 
