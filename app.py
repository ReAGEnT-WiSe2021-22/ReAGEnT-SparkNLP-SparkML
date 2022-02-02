from flask import Flask
#from pymongo import PyMongo
#from bson.json_util import dumps

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://hgaertner:nUH8shJg@reagent1.f4.htw-berlin.de:27017/examples?authSource=examples"
#mongo = PyMongo(app)

@app.route("/<collection_str>", methods=["GET"])
def get_collection(collection_str):

    # data = mongo.db[collection_str].find({})

    # if data is None:
    #     return ("No data found.", 404)
    # else:
    #     response = app.response_class(
    #         response= dumps(data),
    #         status= 200,
    #         mimetype='application/json'
    #     )

    #     return response
    return 'Flask app name'

@app.route("/")
def hello_world():
    return "<p>Welcome to reAgent-data endpoint! Use /'collection_name' to get data.</p>"

if __name__ == "__main__":
    app.run()