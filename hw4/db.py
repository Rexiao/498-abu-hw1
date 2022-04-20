from pyhive import hive  # or import hive or import trino
cursor = hive.Connection(host='10.128.0.9',port=10000,username='liang20182000').cursor()
cursor.execute('SELECT * FROM webSearch LIMIT 10')
print(cursor.fetchone())