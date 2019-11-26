import psycopg2


conn=psycopg2.connect(database=db_name,user=user,password=pwd,host=host,port=port)
cur=conn.cursor()