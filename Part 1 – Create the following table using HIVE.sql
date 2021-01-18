SELECT TRANSACTION.*, 
       result.nb_address_ip_ltd, 
       result.amount_eur_utd 
FROM   (SELECT t.customerid, 
               Count(DISTINCT t.ip_address) AS nb_address_ip_ltd, 
               Sum(t.amount_eur)            AS amount_eur_utd 
        FROM   schematransactionzhen t 
        GROUP  BY t.customerid) result 
       JOIN schematransactionzhen TRANSACTION 
         ON result.customerid = TRANSACTION.customerid 
