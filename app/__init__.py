from flask import Flask

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://hgaertner:nUH8shJg@reagent1.f4.htw-berlin.de:27017/examples?authSource=examples"