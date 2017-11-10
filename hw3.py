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
K = 0
VIEW = None
IPPORT = '0.0.0.0:8080'


@app.route('/kv-store/<key>', methods=['PUT'])
def add_kv(key):
    response_data = {}
    status_code = 200
    print("*" * 80)
    print(request.values.get('val'))
    print("*" * 80)
    # if MAINIP is None:
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
    """else:
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
        return response_dump, res.status_code, {'Content-Type': 'application/json'}"""
        # return response_dump, res.status_code, {'Content-Type': 'application/json'}
        # we are a forwarder node
    return dumps({}), 501, {'Content-Type': 'application/json'}


@app.route('/kv-store/<key>', methods=['GET'])
def get_kv(key):
    # if MAINIP is None:
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
    """else:
        res = requests.get(url=('http://' + MAINIP + '/kv-store/' +
                                key), data={'val': request.args.get('val')})
        try:
            response_dump = dumps(res.json())
        # means it was bad json (specifically I think. empty)
        except decoder.JSONDecodeError:
            response_dump = dumps({})"""
    return dumps({'somehow': 'this gets returned'}), 501, {'Content-Type': 'application/json'}


@app.route('/kv-store/<key>', methods=['DELETE'])
def del_kv(key):
    # if MAINIP is None:
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
    """else:
        res = requests.delete(url=(
            'http://' + MAINIP + '/kv-store/' + key), data={"val": request.args.get('val')})
        return dumps(res.json()), res.status_code, {'Content-Type': 'application/json'}"""
    return dumps({}), 501, {'Content-Type': 'application/json'}


if __name__ == '__main__':
    K      = os.getenv('K', 3)
    VIEW   = os.getenv('VIEW', "10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080")
    IPPORT = os.getenv('IPPORT', None)
    all_nodes     = []
    replica_nodes = []
    proxy_nodes   = []
    degraded_mode = False
    vc = {}

    if IPPORT is not None:
        IP = IPPORT.split(':')[0]
        PORT = IPPORT.split(':')[1]
    else:
        IP = '0.0.0.0'
        PORT = 8080

    if VIEW is not None and K is not None:
        all_nodes = VIEW.split(',')
        # Strips out PORT field, seems unnecessary as they're all 8080.
        for node in all_nodes:
            node = node.split(':')[0]
            # Init vc  dictionary
            vc[node] = 0
        print(vc)
        if len(VIEW) >= K:
            replica_nodes = VIEW[0:(K + 1)]
            proxy_nodes   = VIEW[(K + 1)::]
        else:
            degraded_mode = True
        

    


    app.run(host=IP, port=PORT)
