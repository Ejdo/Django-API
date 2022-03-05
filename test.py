import json
import psycopg2
   
try:
    conn = psycopg2.connect("dbname='dota2' user='xzjavka' host='147.175.150.216' password='#Chevalier64'")
except:
    print("I am unable to connect to the database")

cur = conn.cursor()
output = list()

cur.execute("SELECT  version()")
cur.execute("SELECT pg_database_size('dota2')/1024/1024 as dota2_db_size;")

output = {
    "version": cur.fetchone(),
    "dota2_db_size": cur.fetchone()
}
output = json.dumps(output)

