# from .db import *

from flask import Flask, request
app = Flask(__name__)

@app.route("/results")
def hello_world():
    print(request.json)
    print('sad', request.args)
    return "<p>Hello, World!</p>"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
