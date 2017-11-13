import sys
import os
import re
import collections
import time
from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework import status
from kvstore.models import Entry
import requests as req


# Environment variables.
K          = os.getenv('K', 0)
VIEW       = os.getenv('VIEW', "0.0.0.0:8080,10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080")
IPPORT     = os.getenv('IPPORT', "0.0.0.0:8080")
current_vc = collections.OrderedDict()

all_nodes     = []
replica_nodes = []
proxy_nodes   = []
degraded_mode = False

# if IPPORT != "0.0.0.0":
#     IP = IPPORT.split(':')[0]

if VIEW != None:
    # This is just for testing locally.
    if VIEW != "0.0.0.0:8080":
        all_nodes = VIEW.split(',')
    else:
        all_nodes = [VIEW]
    # Strip out PORT field.
    for node in all_nodes:
        #node = node.split(':')[0]
        current_vc[node] = 0
    print(current_vc)
    print(list(current_vc.values()))
    if len(VIEW) >= K:
        replica_nodes = all_nodes[0:(K + 1)]
        proxy_nodes   = all_nodes[(K + 1)::]
    else:
        degraded_mode = True
        replica_nodes = VIEW


def is_replica():
    #return not os.environ.has_key('MAINIP')
    return True

# FAILURE RESPONSE -- BAD KEY INPUT
@api_view(['GET', 'PUT'])
def failure(request, key):
    return Response({'result': 'Error', 'msg': 'Key not valid'}, status=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE)

@api_view(['GET'])
def get_node_details(request):
    if IPPORT in replica_nodes:
        return Response({"result": "success", "replica": "Yes"}, status=status.HTTP_200_OK)
    elif IPPORT in proxy_nodes:
        return Response({"result": "success", "replica": "no"}, status=status.HTTP_200_OK)
    else:
        return Response({"result": "error", "msg": "Node not found"}, status=status.HTTP_404_NOT_FOUND)

@api_view(['GET'])
def get_all_replicas(request):
    return Response({"result": "success", "replicas": replica_nodes}, status=status.HTTP_200_OK)

# CORRECT KEYS
@api_view(['GET', 'PUT'])
def kvs_response(request, key):
    method = request.method

    # MAIN RESPONSE
    if is_replica():
        # MAIN PUT
        if method == 'PUT':
            # ERROR HANDLING: INVALID KEY TYPE (NONE)
            if 'val' not in request.data:
                return Response({'result':'Error','msg':'No value provided'},status=status.HTTP_400_BAD_REQUEST)
            input_value = request.data['val']

            # ERROR HANDLING: EMPTY VALUE or TOO LONG VALUE
            if 'val' not in request.data or sys.getsizeof(input_value) > 1024 * 1024 * 256:
                return Response({'result':'Error','msg':'No value provided'},status=status.HTTP_403_BAD_REQUEST)

            # Maybe comment this out b/c causal payload can be '' in case if no reads have happened yet?
            if 'causal_payload' not in request.data:
                return Response({'result': 'Error', 'msg': 'No causal_payload provided'}, status=status.HTTP_400_BAD_REQUEST)

            causal_payload = str(request.data['causal_payload'])
            node_id        = list(current_vc.keys()).index(IPPORT)
            timestamp      = int(time.time())
            # len(causal_payload) == 0 if the user hasn't done ANY reads yet. 
            # TODO: MAKE SEPARATE CASE.
            # causal_payload > current_vc
            cp_list = causal_payload.split('.')
            if compare_vc(cp_list, list(current_vc.values())) == 1:
                print ("OLD VC:")
                print (current_vc)
                # Gross-ass way to update current_vc
                i = 0
                for k,v in current_vc.items():
                    current_vc[k] = cp_list[i]
                    i += 1
                print ("NEW VC:")
                print (current_vc)
                entry, created = Entry.objects.update_or_create(key=key, defaults={'value': input_value, 
                                                                                   'causal_payload': causal_payload,
                                                                                   'node_id': node_id,
                                                                                   'timestamp': timestamp})
                return Response({'result': 'success', "value": input_value, "node_id": node_id, "causal_payload": causal_payload, "timestamp": timestamp}, status=status.HTTP_200_OK)
            # Vector clocks are same value, have to compare timestamps.
            elif compare_vc(cp_list, list(current_vc.values())) == 0:
                incoming_timestamp = request.data['timestamp']
                if incoming_timestamp > timestamp:
                    entry, created = Entry.objects.update_or_create(key=key, defaults={'value': input_value,
                                                                                       'causal_payload': causal_payload,
                                                                                       'node_id': node_id,
                                                                                       'timestamp': timestamp})
                    return Response({'result': 'success', "value": input_value, "node_id": node_id, "causal_payload": causal_payload, "timestamp": timestamp}, status=status.HTTP_200_OK)
                # Can't go back in time, reject incoming PUT
                elif incoming_timestamp < timestamp:
                    return Response({'result': 'failure', 'msg': 'Can\'t go back in time.'}, status=status.HTTP_406_NOT_ACCEPTABLE)

            # causal payload < current_vc
            else:
                return Response({'result': 'failure', 'msg': 'Can\'t go back in time.'}, status=status.HTTP_406_NOT_ACCEPTABLE)
                
                # TODO: Figure out what to do in case incoming_timestamp == timestamp


        # MAIN GET
        elif method == 'GET':
            try:
                # KEY EXISTS
                existing_entry = Entry.objects.get(key=key)
                return Response({'result':'Success', 'msg': 'Success', 'value': existing_entry.value}, status=status.HTTP_200_OK)
            except:
                # ERROR HANDLING: KEY DOES NOT EXIST
                return Response({'result':'Error','msg':'Key does not exist'},status=status.HTTP_400_BAD_REQUEST)

        # MAIN DEL
        elif method == 'DELETE':
            try:
                Entry.objects.get(key=key).delete()
                return Response({'result': 'Success'}, status=status.HTTP_200_OK)
            except Entry.DoesNotExist:
                # ERROR HANDLING: KEY DOES NOT EXIST
                return Response({'result':'Error','msg':'Key does not exist'}, status=status.HTTP_404_NOT_FOUND)
    # PROXY RESPONSE
    else:

    # 	# GENERATE BASE URL STRING
        url_str = 'http://'+os.environ['MAINIP']+'/kv-store/'+key


    # 	# FORWARD GET REQUEST
    # 		# PACKAGE AND RETURN RESPONSE TO CLIENT
        if method == 'GET':
            res = req.get(url_str)
            response = Response(res.json())
            response.status_code = res.status_code
    # 	# MODIFY URL STRING WITH PUT INPUT AND FORWARD PUT REQUEST
    # 		# PACKAGE AND RETURN RESPONSE TO CLIENT
        elif method == 'PUT':
            try:
                res = req.put(url=url_str, data=request.data)
                response = Response(res.json())
                response.status_code = res.status_code
            except Exception:
                return Response({'result': 'Error', 'msg': 'Server unavailable'}, status=501)
    # 	# MODIFY URL STRING WITH DEL INPUT AND FORWARD DEL REQUEST
    # 		# PACKAGE AND RETURN RESPONSE TO CLIENT
        elif method == 'DELETE':
            # Key exists
            if req.get(url_str).status_code == 200:
                res = req.delete(url_str)
                response = Response({'result': 'Success'}, status=status.HTTP_200_OK)
            # Key doesn't exist
            elif req.get(url_str).status_code == 404:
                return Response({'msg': 'Error', 'error': 'Key does not exist'}, status=status.HTTP_404_NOT_FOUND)
            # 'Everything else', 500 errors etc.
            else:
                return Response(req.delete(url_str).json())
        return response

def compare_vc(a, b):
    """
    Compares two vector clocks, returns -1 if ``a < b``,
    1 if ``a > b`` else 0 for concurrent events
    or identical values.
    """
    gt = False
    lt = False
    for j, k in zip(a, b):
        gt |= int(j) > int(k)
        lt |= int(j) < int(k)
        if gt and lt:
            break
    return int(gt) - int(lt)
