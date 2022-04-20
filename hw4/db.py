from pyhive import hive  # or import hive or import trino
connection = hive.Connection(host='10.128.0.9',port=10000,username='liang20182000')

def exec_query(query):
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchall()