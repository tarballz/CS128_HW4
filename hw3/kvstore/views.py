import sys
import os
import re
import collections
import time
from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework import status
from .models import Entry
import requests as req


# Environment variables.
K          = os.getenv('K', 3)
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
        replica_nodes = all_nodes[0:K]
        proxy_nodes   = all_nodes[K::]
    else:
        degraded_mode = True
        replica_nodes = VIEW


def is_replica():
    return (IPPORT in replica_nodes)

# FAILURE RESPONSE -- BAD KEY INPUT
@api_view(['GET', 'PUT'])
def failure(request, key):
    return Response({'result': 'Error', 'msg': 'Key not valid'}, status=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE)

@api_view(['GET'])
def get_node_details(request):
    if IPPORT in replica_nodes:
        return Response({"result": "success", "replica": "Yes"}, status=status.HTTP_200_OK)
    elif IPPORT in proxy_nodes:
        return Response({"result": "success", "replica": "No"}, status=status.HTTP_200_OK)
    else:
        return Response({"result": "error", "msg": "Node not found"}, status=status.HTTP_404_NOT_FOUND)

@api_view(['GET'])
def get_all_replicas(request):
    return Response({"result": "success", "replicas": replica_nodes}, status=status.HTTP_200_OK)

# CORRECT KEYS
@api_view(['GET', 'PUT'])
def kvs_response(request, key):
    method = request.method
    existing_entry     = None
    existing_timestamp = None

    # MAIN RESPONSE
    if is_replica():
        # MAIN PUT
        if method == 'PUT':
            new_entry = False
            # ERROR HANDLING: INVALID KEY TYPE (NONE)
            if 'val' not in request.data:
                return Response({'result':'Error','msg':'No value provided'},status=status.HTTP_400_BAD_REQUEST)
            input_value = request.data['val']

            # ERROR HANDLING: EMPTY VALUE or TOO LONG VALUE
            if 'val' not in request.data or sys.getsizeof(input_value) > 1024 * 1024 * 256:
                return Response({'result':'Error','msg':'No value provided'},status=status.HTTP_400_BAD_REQUEST)

            # Maybe comment this out b/c causal payload can be '' in case if no reads have happened yet?
            if 'causal_payload' not in request.data:
                return Response({'result': 'Error', 'msg': 'No causal_payload provided'}, status=status.HTTP_400_BAD_REQUEST)

            causal_payload = str(request.data['causal_payload'])
            node_id        = list(current_vc.keys()).index(IPPORT)
            new_timestamp  = int(time.time())
            # len(causal_payload) == 0 if the user hasn't done ANY reads yet. 
            # TODO: MAKE SEPARATE CASE for len(causal_payload) == 0

            cp_list = causal_payload.split('.')
            # Need to do a GET to either compare values or confirm this entry is being
            # entered for the first time.
            try:
                existing_entry = Entry.objects.get(key=key)
                existing_timestamp = existing_entry.timestamp
            except:
                new_entry = True

            # if causal_payload > current_vc
            if compare_vc(cp_list, list(current_vc.values())) == 1:
                print ("OLD VC:")
                print (current_vc)
                # Gross-ass way to update current_vc
                i = 0
                for k,v in current_vc.items():
                    if current_vc[k] != None:
                        current_vc[k] = cp_list[i]
                        i += 1
                print ("NEW VC:")
                print (current_vc)
                if new_entry:
                    existing_timestamp = new_timestamp
                entry, created = Entry.objects.update_or_create(key=key, defaults={'value': input_value,
                                                                                   'causal_payload': causal_payload,
                                                                                   'node_id': node_id,
                                                                                   'timestamp': existing_timestamp})
                return Response(
                    {'result': 'success', "value": input_value, "node_id": node_id, "causal_payload": causal_payload,
                     "timestamp": existing_timestamp}, status=status.HTTP_200_OK)
            # Vector clocks are same value, have to compare timestamps.
            elif compare_vc(cp_list, list(current_vc.values())) == 0:
                # if existing_timestamp < new_timestamp:
                entry, created = Entry.objects.update_or_create(key=key, defaults={'value': input_value,
                                                                                   'causal_payload': causal_payload,
                                                                                   'node_id': node_id,
                                                                                   'timestamp': existing_timestamp})
                return Response({'result': 'success', 'value': input_value, 'node_id': node_id,
                                 'causal_payload': causal_payload, 'timestamp': existing_timestamp},
                                status=status.HTTP_200_OK)
                # Can't go back in time, reject incoming PUT
                # elif existing_timestamp > timestamp:
                    #return Response({'result': 'failure', 'msg': 'Can\'t go back in time.'},
                                    #status=status.HTTP_406_NOT_ACCEPTABLE)

            # causal payload < current_vc
            else:
                return Response({'result': 'failure', 'msg': 'Can\'t go back in time.'},
                                status=status.HTTP_406_NOT_ACCEPTABLE)

                # TODO: Figure out what to do in case incoming_timestamp == timestamp


        # MAIN GET
        elif method == 'GET':
            try:
                # KEY EXISTS
                existing_entry = Entry.objects.get(key=key)
                return Response(
                    {'result': 'success', "value": existing_entry.value, "node_id": existing_entry.node_id,
                     "causal_payload": existing_entry.causal_payload,
                     "timestamp": existing_entry.timestamp}, status=status.HTTP_200_OK)
            except:
                # ERROR HANDLING: KEY DOES NOT EXIST
                return Response({'result':'Error','msg':'Key does not exist'},status=status.HTTP_400_BAD_REQUEST)

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
                return Response({'result': 'error', 'msg': 'Server unavailable'}, status=501)

        return response

# CORRECT KEYS
@api_view(['PUT'])
def update_view(request):
    new_ipport = request.data['ip_port']

    if request.GET.get('type', '') == 'add':
        # Added node should be a replica.
        all_nodes.append(new_ipport)
        if len(all_nodes) <= K:
            replica_nodes.append(new_ipport)
            # Check if we're resurrecting a node that was previously in our OrderedDict current_vc
            if new_ipport not in current_vc:
                # Init new entry into our dictionary.
                current_vc.update({new_ipport: 0})
            else:
                current_vc[new_ipport] = 0
        elif len(all_nodes) > K:
            proxy_nodes.append(new_ipport)
            degraded_mode = False

        return Response(
            {"msg": "success", "node_id": list(current_vc.keys()).index(new_ipport), "number_of_nodes": len(all_nodes)})

    elif request.GET.get('type', '') == 'remove':
        all_nodes.remove(new_ipport)
        if new_ipport in replica_nodes:
            replica_nodes.remove(new_ipport)
            # Instead of deleting the node from the OrderedDict, we will just set it's value to None to indicate
            # that it's been removed, but this way we're still able to preserve accurate node_id's.
            current_vc[new_ipport] = None
            if len(replica_nodes) <= K:
                # If we have any "spare" nodes in proxy_nodes, promote it to a replica.
                if len(proxy_nodes) > 0:
                    promoted = proxy_nodes.pop()
                    replica_nodes.append(promoted)
                    current_vc[promoted] = 0

                    if len(replica_nodes) > K:
                        degraded_mode = False
                else:
                    degraded_mode = True

        elif IPPORT in proxy_nodes:
            proxy_nodes.remove(IPPORT)


    else:
        return Response({'result': 'error', 'msg': 'key value store is not available'}, status=status.HTTP_501_NOT_IMPLEMENTED)



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


def find_min()
    """"
    Find the minimum value of the vector clock,
    returns the IP of the node with the least work,
    used for forwarding
    """

    min = sys.maxsize
    for k, v in current_vc.items():
        if min > current_vc[k]:
            min = v
            key = k
    return key

def check_nodes():
    return Response(status=status.HTTP_200_OK)
