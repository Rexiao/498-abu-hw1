import db
import json

from flask import Flask, request
from math import ceil

app = Flask(__name__)


@app.route("/results", methods=['POST'])
def get_results():
    try:
        data = request.get_json()
        name = data['term']
        query = f"""SELECT termval,clicks from webSearch LATERAL view explode(term) term_table AS termkey, termval WHERE termval = '{name}'"""
        print("query", query)
        db_response = db.exec_query(query)
        print(db_response[0][1])
        result_dict = {}
        result_dict['results'] = json.loads(db_response[0][1])
        return result_dict
    except Exception as e:
        print(e)
        return "<p>Error!</p>"

@app.route("/trends", methods=['POST'])
def get_trends():
    try:
        data = request.get_json()
        name = data['term']
        query = f"""SELECT SUM(clickcount) from webSearch
LATERAL view explode(term) term_table AS termkey, termval
LATERAL view explode(clicks) click_table AS clickurl, clickcount
WHERE termval = '{name}'
GROUP BY termval"""
        print("query", query)
        db_response = db.exec_query(query)
        print(db_response[0][0])
        result_dict = {}
        result_dict['clicks'] = db_response[0][0]
        return result_dict
    except Exception as e:
        print(e)
        return "<p>Error!</p>"

@app.route("/popularity", methods=['POST'])
def get_popularity():
    try:
        data = request.get_json()
        name = data['url']
        query = f"""SELECT SUM(clickcount) from webSearch
LATERAL view explode(term) term_table AS termkey, termval
LATERAL view explode(clicks) click_table AS clickurl, clickcount
WHERE clickurl = '{name}'
GROUP BY clickurl"""
        print("query", query)
        db_response = db.exec_query(query)
        print(db_response[0][0])
        result_dict = {}
        result_dict['clicks'] = db_response[0][0]
        return result_dict
    except Exception as e:
        print(e)
        return "<p>Error!</p>"

@app.route("/getBestTerms", methods=['POST'])
def get_best_terms():
    try:
        data = request.get_json()
        name = data['website']
        query1 = f"""SELECT SUM(clickcount) from webSearch
LATERAL view explode(term) term_table AS termkey, termval
LATERAL view explode(clicks) click_table AS clickurl, clickcount
WHERE clickurl = '{name}'
GROUP BY clickurl"""
        print("query", query1)
        db_response = db.exec_query(query1)
        total_count = db_response[0][0]
        print(total_count)
        thresold = 0.05
        query2 = f"""SELECT termval from webSearch
LATERAL view explode(term) term_table AS termkey, termval
LATERAL view explode(clicks) click_table AS clickurl, clickcount
WHERE clickurl = '{name}' AND clickcount >= {ceil(total_count * thresold)}"""
        result_dict = {}
        print("query2", query2)
        db_response = db.exec_query(query2)
        result_dict['best_terms'] = db_response[0]
        return result_dict
    except Exception as e:
        print(e)
        return "<p>Error!</p>"
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
