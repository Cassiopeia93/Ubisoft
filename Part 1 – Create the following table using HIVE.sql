-- Generate a column which gives the number of distinct IP address LTD (live to date) per customerId.
-- Generate a column which gives the amount spent UTD (up to date) per customerId.


SELECT TRANSACTION.*, 
       result.nb_address_ip_ltd, 
       result.amount_eur_utd 
FROM   (SELECT t.customerid, 
               Count(DISTINCT t.ip_address) AS nb_address_ip_ltd, 
               Sum(t.amount_eur)            AS amount_eur_utd 
        FROM   schematransactionzhen t --schematransaction name has already been taken so I used 'schematransactionzhen'
        GROUP  BY t.customerid) result 
       JOIN schematransactionzhen TRANSACTION 
         ON result.customerid = TRANSACTION.customerid 
