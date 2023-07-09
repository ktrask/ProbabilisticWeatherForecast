import threading
# Importing flask module in the project is mandatory
# An object of Flask class is our WSGI application.
from flask import Flask, request, jsonify
from controller import downloadData, openData, makeMeteogram, downloadAndLoad
# Flask constructor takes the name of
# current module (__name__) as argument.
app = Flask(__name__)

#app.debug = True

downloadAndLoad()


# The route() function of the Flask class is a decorator,
# which tells the application which URL should call
# the associated function.
@app.route('/')
# ‘/’ URL is bound with hello_world() function.
def hello_world():
    return 'Hello World'

@app.route('/getMeteogram/<latitude>/<longitude>/', methods=['GET'])
def getMeteogram(latitude, longitude):
    # latitude = round(float(latitude)/0.4)*0.4
    # longitude = round(float(longitude)/0.4)*0.4
    # return jsonify({'lat': latitude,'lon':longitude})
    return jsonify(makeMeteogram(float(latitude), float(longitude)))

@app.route('/updateData', methods=['GET'])
def updateData():
    x = threading.Thread(target=downloadAndLoad, args=())
    x.start()
    return jsonify({"result": True})

# main driver function
if __name__ == '__main__':
    # run() method of Flask class runs the application
    # on the local development server.
    app.run(host="0.0.0.0")

