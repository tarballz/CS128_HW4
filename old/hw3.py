import os
import collections
import time
from flask import Flask
from flask import request
from flask import jsonify
from json import dumps, decoder

import subprocess
import requests
from urllib3.exceptions import NewConnectionError

app = Flask(__name__)
KVStore = {}
K = None
VIEW = None
IPPORT = '0.0.0.0:8080'
current_vc = collections.OrderedDict()


@app.route('/kv-store/<key>', methods=['PUT'])
def add_kv(key):
    response_data = {}
    status_code = 200
    print("*" * 80)
    print(request.values.get('val'))
    print("*" * 80)
    # if MAINIP is None:
    # if len(key) > 200 or len(key) < 1:
    #     status_code = 403
    #     response_data["result"] = 'Error'
    #     response_data["msg"] = 'Key not valid'
    # else:
    value = request.values.get('val')
    causal_payload = request.values.get('causal_payload')
    #if IP in replica_nodes:
    if key in KVStore:
        # Have to make sure new causal payload > old causal payload?
        compare_vc(current_vc, causal_payload)
        KVStore[key] = Element(key, value, causal_payload)
    else:
        KVStore[key] = Element(key, value, causal_payload)
    response_data["result"]         = "success"
    response_data["node_id"]        = KVStore[key].node_id
    response_data["causal_payload"] = KVStore[key].causal_payload
    response_data["timestamp"]      = KVStore[key].timestamp
    return dumps(response_data), status_code, {'Content-Type': 'application/json'}
        # return dumps({'result': 'Error', 'msg': 'No value provided'}), 403, {'Content-Type': 'application/json'}
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
from urllib3.exceptions import NewConnectionError

            response_dump = dumps({})
        return response_dump, res.status_code, {'Content-Type': 'application/json'}"""
    # return response_dump, res.status_code, {'Content-Type': 'application/json'}
    # we are a forwarder node
    return dumps({}), 501, {'Content-Type': 'application/json'}


@app.route('/kv-store/<key>', methods=['GET'])
def get_kv(key):
    response_data = {}
    status_code = 200
    causal_payload = request.values.get('causal_payload')
    if key in KVStore:
        response_data["result"]         = "success"
        response_data["node_id"]        = KVStore[key].node_id
        response_data["causal_payload"] = KVStore[key].causal_payload
        response_data["timestamp"]      = KVStore[key].timestamp
    else:
        response_data["msg"]    = 'Key does not exist'
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


@app.route('/kv-store/get_node_details', methods=['GET'])
def get_node_details():
    response_data = {}
    status_code = 200
    response_data["result"] = "success"
    if IPPORT in replica_nodes:
        response_data["replica"] = "Yes"
    elif IPPORT in proxy_nodes:
        response_data["replica"] = "No"
    else:
        response_data["replica"] = "ERROR!!"  # Hopefully never gets here.
    return jsonify(dumps(response_data)), status_code


@app.route('/kv-store/get_all_replicas', methods=['GET'])
def get_all_replicas():
    response_data = {}
    status_code = 200
    response_data["result"] = "success"
    response_data["replicas"] = replica_nodes
    return jsonify(dumps(response_data)), status_code


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

# Element should be created for every new Write issued.


class Element:
    def __init__(self, key, value, causal_payload):
        self.key = key
        self.value = value
<<<<<<< HEAD
        self.causal_payload = split_cp(causal_payload)  # vector clock
=======
        self.causal_payload = causal_payload  # vector clock
>>>>>>> aab2e85ed912e034300170e934079480964bfbe5
        self.node_id = list(current_vc.keys()).index(IP)
        self.timestamp = int(time.time()) # @TODO: do we need to worry about extra precision?
                                          # because this truncates the decimal portion...



<<<<<<< HEAD
def split_cp(payload):
    return [int(a) for a in payload.split('.')]

def compare_vc(vc, cp):
    vc = vc.values()
    print(vc)
    cp = split_cp(cp)
    compared_clocks = [((o <= vc[i]), (o < vc[i])) for i, o in enumerate(cp)]
=======
def compare_vc(vc_in, cp_in):
    vc_in = vc_in.values()
    normalized_cp = [int(a) for a in cp_in.split('.')]
    print(normalized_cp)
    compared_clocks = [((o <= vc_in[i]), (o < vc_in[i])) for i, o in enumerate(normalized_cp)]
>>>>>>> aab2e85ed912e034300170e934079480964bfbe5
    print(compared_clocks)
    # 1 -- cp > vc
    # 0 -- cp = vc
    # -1 -- cp < vc
    return compared_clocks



if __name__ == "__main__":
    K = os.getenv('K', 3)
    # VIEW = os.getenv(
    #     'VIEW', "10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080")
    VIEW = os.getenv(
        'VIEW', "0.0.0.0:8080")
    IPPORT = os.getenv('IPPORT', None)
    all_nodes     = []
    replica_nodes = []
    proxy_nodes   = []
    degraded_mode = False

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
<<<<<<< HEAD
            # Init vc dictionary
=======
            # Init current_vc dictionary
>>>>>>> aab2e85ed912e034300170e934079480964bfbe5
            current_vc[node] = 0
        print(current_vc)
        if len(VIEW) >= K:
            replica_nodes = VIEW[0:(K + 1)]
            proxy_nodes = VIEW[(K + 1)::]
        else:
            degraded_mode = True
            replica_nodes = VIEW

    app.run(host=IP, port=PORT)
