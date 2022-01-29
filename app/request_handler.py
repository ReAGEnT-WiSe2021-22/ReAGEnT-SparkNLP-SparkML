from app import app

from flask import request
from pymongo import PyMongo
from bson.json_util import dumps

mongo = PyMongo(app)

@app.route("/reagent/<collection_str>", methods=["GET"])
def get_collection(collection_str):

    data = mongo.db[collection_str].find({})

    if data is None:
        return ("No data found.", 404)
    else:
        response = app.response_class(
            response= dumps(data),
            status= 200,
            mimetype='application/json'
        )

        return response