import datetime
import random
import pandas as pd
import duckdb
import numpy as np

#db connection
con=duckdb.connect(r"C:\Users\mislam\Desktop\duckdb\test_data.db")

#Create simulated data
df=pd.DataFrame({
"customer":[random.randint(1, 50000) for i in range(5000000)],
"products":[np.random.choice(["A","B","C","D","E"]) for i in range(5000000)],
"sale_date":[(datetime.date(2020,1,1)+datetime.timedelta(days=random.randint(0, 1000))).strftime("%d-%m-%Y")\
            for i in range(5000000)],
"amount":[1000 * random.random() for i in range(5000000)]})

#Insert data
con.sql("""create table data as select customer,products,STRPTIME(sale_date, '%d-%m-%Y')::date as sale_date , amount  from df""")

#prepare data
con.sql(""" 
select customer,
products,
last_day(sale_date) as end_of_month,
amount
,dense_rank () over (partition by customer,products, year(sale_date) order by last_day(sale_date)) as dr
from data""")

#Close db connection
con.close()
