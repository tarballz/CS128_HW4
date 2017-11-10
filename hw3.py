import os
from flask import Flask
from flask import request
from flask import jsonify
from json import dumps, decoder

import subprocess
import requests
from urllib3.exceptions import NewConnectionError

app = Flask(__name__)
KVStore = {}
MAINIP = None
PORT = '0.0.0.0'
IP = 8080


@app.route('/kv-store/<key>', methods=['PUT'])
def add_kv(key):
    response_data = {}
    status_code = 200
    print("*" * 80)
    print(request.values.get('val'))
    print("*" * 80)
    if MAINIP is None:
        if len(key) > 200 or len(key) < 1:
            status_code = 403
            response_data["result"] = 'Error'
            response_data["msg"] = 'Key not valid'
        else:
            if key in KVStore:
                response_data["replaced"] = 'True'
                response_data["msg"] = "Value of existing key replaced"
            else:
                response_data["replaced"] = 'False'
                response_data["msg"] = "New key created"
                status_code = 201
            # return dumps({'result': 'Error', 'msg': 'No value provided'}), 403, {'Content-Type': 'application/json'}
            KVStore[str(key)] = request.values.get('val')
        return dumps(response_data), status_code, {'Content-Type': 'application/json'}
    else:
        print ("hoasdla")
        print (key)
        print (request.args.get('val', 'null'))
        print ("http://" + MAINIP + '/kv-store/' + key)
        try:
            res = requests.put(url=(
                "http://" + MAINIP + '/kv-store/' + key), data={'val': request.args.get('val')})
        except NewConnectionError:
            return dumps({'result': 'Error', 'msg': 'Server unavailable'}), 501, {'Content-Type': 'application/json'}
        print ("asdasdasd")
        try:
            response_dump = dumps(res.json())
        except decoder.JSONDecodeError:
            response_dump = dumps({})
        return response_dump, res.status_code, {'Content-Type': 'application/json'}
        # return response_dump, res.status_code, {'Content-Type': 'application/json'}
        # we are a forwarder node
    return dumps({}), 501, {'Content-Type': 'application/json'}


@app.route('/kv-store/<key>', methods=['GET'])
def get_kv(key):
    if MAINIP is None:
        response_data = {}
        status_code = 200
        if key in KVStore:
            response_data["msg"] = "Success"
            response_data["value"] = KVStore[str(key)]
        else:
            response_data["msg"] = 'Key does not exist'
            response_data["result"] = 'Error'
            status_code = 404
        return dumps(response_data), status_code, {'Content-Type': 'application/json'}
    else:
        res = requests.get(url=('http://' + MAINIP + '/kv-store/' +
                                key), data={'val': request.args.get('val')})
        try:
            response_dump = dumps(res.json())
        # means it was bad json (specifically I think. empty)
        except decoder.JSONDecodeError:
            response_dump = dumps({})
    return dumps({'somehow': 'this gets returned'}), 501, {'Content-Type': 'application/json'}


@app.route('/kv-store/<key>', methods=['DELETE'])
def del_kv(key):
    if MAINIP is None:
        response_data = {}
        status_code = 200
        if key in KVStore:
            response_data["result"] = "Success"
            KVStore.pop(key, None)
        else:
            response_data["result"] = 'Error'
            response_data["msg"] = "Key does not exist"
            status_code = 404
        return dumps(response_data), status_code, {'Content-Type': 'application/json'}
    else:
        res = requests.delete(url=(
            'http://' + MAINIP + '/kv-store/' + key), data={"val": request.args.get('val')})
        return dumps(res.json()), res.status_code, {'Content-Type': 'application/json'}
    return dumps({}), 501, {'Content-Type': 'application/json'}


if __name__ == '__main__':
    MAINIP = os.getenv('MAINIP', None)
    IP = os.getenv('IP', '0.0.0.0')
    PORT = os.getenv('PORT', 8080)
    if MAINIP is not None:
        print("mip: " + MAINIP)
    app.run(host=IP, port=PORT)
