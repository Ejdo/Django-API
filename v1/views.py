from django.http import HttpResponse
import psycopg2
import json
import os

def now(request):
    try:
        conn = psycopg2.connect(dbname="dota2", user=os.environ.get('dbname'), host='147.175.150.216',password=os.environ.get('dbpassword'))
    except:
        print("I am unable to connect to the database")

    cur = conn.cursor()
    output = list()

    cur.execute("SELECT  version()")
    version = cur.fetchone()
    cur.execute("SELECT pg_database_size('dota2')/1024/1024 as dota2_db_size;")
    size = cur.fetchone()

    output = {
        "pgsql":{
            "version": version,
            "dota2_db_size": size
        }
    }
    output = json.dumps(output)
    
    return HttpResponse(output,content_type='text/plain')