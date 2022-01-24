from http.server import BaseHTTPRequestHandler, HTTPServer
import os
from pymongo import MongoClient
from bson.json_util import dumps
klientString=os.getenv("")
klient=MongoClient(os.getenv("REAGENT_MONGO")+"?authSource=examples")
db=klient["examples"]
labels=db["ml_party_reputation_labels"]
predictions=db["ml_party_reputation_predictions"]


class myHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('content-type', 'application/json')
        self.end_headers()
        labelcursor=labels.find({})
        labelcount=labels.count_documents({})
        predictioncursor=predictions.find({})
        predictioncount=predictions.count_documents({})
        labelstring="["
        for i, label in enumerate(labelcursor, 1):
            labelstring+=dumps(label)
            if i != labelcount:
                labelstring+=","
        labelstring+="]"
        predictionstring="["
        for i, prediction in enumerate(predictioncursor, 1):
            predictionstring+=dumps(prediction)
            if i != predictioncount:
                predictionstring+=","
        predictionstring+="]"
        combinedString=("["+labelstring +","+predictionstring+"]")
        self.wfile.write(combinedString.encode("utf-8"))

def main():
    port=8123
    server=HTTPServer(('', port), myHandler)
    print("starting server")
    server.serve_forever()

main()