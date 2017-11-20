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
from threading import Event
from .NodeTracker import NodeTracker

# SET DEBUG TO True  IF YOU'RE WORKING LOCALLY
# SET DEBUG TO False IF YOU'RE WORKING THROUGH DOCKER
DEBUG = False

# Environment variables.
K          = int(os.getenv('K', 3))
VIEW       = os.getenv('VIEW', "0.0.0.0:8080,10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080")
IPPORT     = os.getenv('IPPORT', "0.0.0.0:8080")
current_vc = collections.OrderedDict()
# AVAILIP = nodes that are up.
AVAILIP    = {}

all_nodes     = []
replica_nodes = []
proxy_nodes   = []
degraded_mode = False

# if IPPORT != "0.0.0.0":
#     IP = IPPORT.split(':')[0]

if VIEW != None:
    if DEBUG:
        # This is just for testing locally.
        if VIEW != "0.0.0.0:8080":
            all_nodes = VIEW.split(',')
        else:
            all_nodes = [VIEW]
    if not DEBUG:
        all_nodes = VIEW.split(',')
    for node in all_nodes:
        current_vc[node] = 0
        AVAILIP[node]    = True
    if DEBUG:
        print(list(current_vc.values()))
    if len(VIEW) > K:
        replica_nodes = all_nodes[0:K]
        proxy_nodes   = list(set(all_nodes) - set(replica_nodes))
    else:
        degraded_mode = True
        replica_nodes = VIEW


def is_replica():
    return (IPPORT in replica_nodes)


# try:
#     _thread.start_new_thread(NodeTracker.run, AVAILIP)
# except:
#     pass

# stop_flag = Event()
# tracker_thread = NodeTracker(stop_flag)
# if not DEBUG:
#     tracker_thread.start()

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

            # IF DATA HAS NODE_ID, THEN WE'VE RECEIVED NODE-TO-NODE COMMUNICATION
            # AND NEED TO STORE IT.
            if 'timestamp' in request.data:
                # BUILD INCOMING OBJECT.
                try:
                    incoming_key       = str(request.data['key'])
                    incoming_value     = str(request.data['val'])
                    incoming_cp        = str(request.data['causal_payload'])
                    incoming_node_id   = int(request.data['node_id'])
                    incoming_timestamp = int(request.data['timestamp'])
                except:
                    return Response({'result': 'Error', 'msg': 'Cannot construct node-to-node entry'},
                                    status=status.HTTP_400_BAD_REQUEST)


                cp_list = incoming_cp.split('.')
                if DEBUG:
                    print("current_vc.values(): %s" % (list(current_vc.values())))
                # IF INCOMING_CP > CURRENT_VC
                if compare_vc(cp_list, list(current_vc.values())) == 1:
                    update_current_vc(incoming_cp)
                    entry, created = Entry.objects.update_or_create(key=key, defaults={'val': incoming_value,
                                                                                      'causal_payload': incoming_cp,
                                                                                      'node_id': incoming_node_id,
                                                                                      'timestamp': incoming_timestamp})
                    return Response(
                        {'result': 'success', "value": incoming_value, "node_id": incoming_node_id,
                         "causal_payload": incoming_cp,
                         "timestamp": incoming_timestamp}, status=status.HTTP_200_OK)

                elif compare_vc(cp_list, list(current_vc.values())) == 0:
                    new_entry = False
                    try:
                        existing_entry = Entry.objects.get(key=key)
                    except:
                        new_entry = True
                    if new_entry:
                        # FAILURE: KEY DOES NOT EXIST
                        # CREATE ENTRY IN OUR DB SINCE THE ENTRY DOESN'T EXIST.
                        Entry.objects.update_or_create(key=key, defaults={'val': incoming_value,
                                                                         'causal_payload': incoming_cp,
                                                                         'node_id': incoming_node_id,
                                                                         'timestamp': incoming_timestamp})
                        return Response({'result': 'Success', 'msg': 'Key does not exist'},
                                        status=status.HTTP_201_CREATED)
                    # IF WE'VE GOTTEN HERE, KEY EXISTS
                    else:
                        if incoming_timestamp > existing_entry.timestamp:
                            Entry.objects.update_or_create(key=key, defaults={'val': incoming_value,
                                                                             'causal_payload': incoming_cp,
                                                                             'node_id': incoming_node_id,
                                                                             'timestamp': incoming_timestamp})
                            return Response(
                                {'result': 'success', "value": incoming_value, "node_id": incoming_node_id,
                                 "causal_payload": incoming_cp,
                                 "timestamp": incoming_timestamp}, status=status.HTTP_200_OK)
                        else:
                            return Response({'result': 'failure', 'msg': 'Can\'t go back in time.'},
                                            status=status.HTTP_406_NOT_ACCEPTABLE)

                # IF INCOMONG_CP < CURRENT_VC
                #elif compare_vc(cp_list, list(current_vc.values())) == -1:
                else:
                    return Response({'result': 'failure', 'msg': 'Can\'t go back in time.'},
                                    status=status.HTTP_406_NOT_ACCEPTABLE)


            # IF NO TIMESTAMP, WE KNOW THIS PUT IS FROM THE CLIENT.
            else:
                incoming_cp     = str(request.data['causal_payload'])
                node_id         = list(current_vc.keys()).index(IPPORT)
                new_timestamp   = int(time.time())
                const_timestamp = new_timestamp # new_timestamp keeps updating :((

                if DEBUG:
                    print("incoming_cp_CLIENT: %s" % (incoming_cp))
                    print(len(incoming_cp))

                # len(causal_payload) == 0 if the user hasn't done ANY reads yet.
                if len(incoming_cp) <= 2:
                    incoming_cp = ''
                    if DEBUG:
                        print("init triggered")
                    # Initialize vector clock.
                    for k,v in current_vc.items():
                        if v is not None:
                            incoming_cp += str(v) + '.'
                    # STRIP LAST LETTER FROM INCOMING CP
                    incoming_cp = incoming_cp.rstrip('.')

                    if DEBUG:
                        print("zero icp: %s" % (incoming_cp))

                    # TODO: Increment our VC before we broadcast.
                    if not DEBUG:
                        broadcast(key, input_value, incoming_cp, node_id, const_timestamp)

                    Entry.objects.update_or_create(key=key, defaults={'val': input_value,
                                                                      'causal_payload': incoming_cp,
                                                                      'node_id': node_id,
                                                                      'timestamp': const_timestamp})
                    return Response(
                        {'result': 'success', "value": input_value, "node_id": node_id, "causal_payload": incoming_cp,
                         "timestamp": const_timestamp}, status=status.HTTP_201_CREATED)

                cp_list = incoming_cp.split('.')
                # Need to do a GET to either compare values or confirm this entry is being
                # entered for the first time.
                existing_entry = None
                try:
                    existing_entry = Entry.objects.get(key=key)
                    #existing_entry = Entry.objects.latest('timestamp')
                    existing_timestamp = existing_entry.timestamp
                except:
                    new_entry = True

                if DEBUG:
                    print("EXISTING ENTRY: ", existing_entry)

                if not DEBUG:
                    broadcast(key, input_value, incoming_cp, node_id, const_timestamp)

                # if causal_payload > current_vc
                # I SET THIS TO BE "> -1" B/C IT DOES NOT MATTER IF VCS ARE THE SAME B/C CLIENT WILL NOT PASS A TIMESTAMP
                if compare_vc(cp_list, list(current_vc.values())) > -1:
                    # print ("OLD VC: %s" % (current_vc))
                    update_current_vc_client(cp_list)

                    entry, created = Entry.objects.update_or_create(key=key, defaults={'val': input_value,
                                                                                       'causal_payload': incoming_cp,
                                                                                       'node_id': node_id,
                                                                                       'timestamp': const_timestamp})
                    return Response(
                        {'result': 'success', "value": input_value, "node_id": node_id, "causal_payload": incoming_cp,
                         "timestamp": const_timestamp}, status=status.HTTP_200_OK)


                # causal payload < current_vc
                else:
                    return Response({'result': 'failure', 'msg': 'Can\'t go back in time.'},
                                    status=status.HTTP_406_NOT_ACCEPTABLE)


        # MAIN GET
        elif method == 'GET':
            try:
                # KEY EXISTS
                existing_entry = Entry.objects.get(key=key)
                return Response({'result': 'success', "value": existing_entry.val, "node_id": existing_entry.node_id,"causal_payload": existing_entry.causal_payload,"timestamp": existing_entry.timestamp}, status=status.HTTP_200_OK)
            except:
                # ERROR HANDLING: KEY DOES NOT EXIST
                return Response({'result':'Error','msg':'Key does not exist'},status=status.HTTP_404_NOT_FOUND)


    # PROXY RESPONSE
    else:

    # 	# GENERATE BASE URL STRING
    #     url_str = 'http://'+os.environ['MAINIP']+'/kv-store/'+key
        # TODO: Implement some form of gossip or laziest_node() is useless.
        dest_node = laziest_node(current_vc)
        if DEBUG:
            print("SELECTED ", dest_node, " TO FORWARD TO.")

        # Some letters get chopped off when I forward.  Only retaining last letter..?
        url_str = 'http://' + dest_node + '/kv-store/' + key


    # 	# FORWARD GET REQUEST
    # 		# PACKAGE AND RETURN RESPONSE TO CLIENT
        if method == 'GET':
            res = req.get(url=url_str, timeout=3)
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


def broadcast(key, value, cp, node_id, timestamp):
    for dest_node in replica_nodes:
        if dest_node != IPPORT:
            url_str = 'http://' + dest_node + '/kv-store/' + key
            try:
                res = req.put(url=url_str, data={'key': key,
                                                 'val': value,
                                                 'causal_payload': cp,
                                                 'node_id': node_id,
                                                 'timestamp': timestamp})
            except:
                pass

# Gross-ass way to update current_vc
def update_current_vc(new_cp):
    i = 0
    for k, v in current_vc.items():
        if current_vc[k] != None:
            current_vc[k] = new_cp[i]
            i += 1
    if DEBUG:
        print("NEW 1VC: %s" % (current_vc))

# Gross-ass way to update current_vc
def update_current_vc_client(new_cp):
    # Need to cast new_cp to an int list to I can increment it's elements.
    new_cp = list(map(int, new_cp))
    i = 0
    for k, v in current_vc.items():
        if current_vc[k] != None:
            if IPPORT == k:
                new_cp[i] += 1
            current_vc[k] = new_cp[i]
            i += 1
    if DEBUG:
        print("NEW 1VC: %s" % (current_vc))


@api_view(['PUT'])
def update_view(request):
    new_ipport = request.data['ip_port']

    #print("TYPE IS: %s" % (str(request.GET.get('type'))))

    if str(request.GET.get('type')) == 'add':
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
            if new_ipport not in current_vc:
                # Init new entry into our dictionary, and set to None b/c proxy.
                current_vc.update({new_ipport: None})
            degraded_mode = False

        return Response(
            {"msg": "success", "node_id": list(current_vc.keys()).index(new_ipport), "number_of_nodes": len(all_nodes)}, status=status.HTTP_200_OK)

    elif str(request.GET.get('type')) == 'remove':
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

        return Response(
            {"msg": "success", "node_id": list(current_vc.keys()).index(new_ipport), "number_of_nodes": len(all_nodes)},
            status=status.HTTP_200_OK)


    else:
        return Response({'result': 'error', 'msg': 'key value store is not available'}, status=status.HTTP_501_NOT_IMPLEMENTED)

@api_view(['GET'])
def check_nodes():
    # new_ipport = request.data['ip_port']
    return Response(status=status.HTTP_200_OK)

def compare_vc(a, b):
    """
    Compares two vector clocks, returns -1 if ``a < b``,
    1 if ``a > b`` else 0 for concurrent events
    or identical values.
    """
    gt = False
    lt = False
    for j, k in zip(a, b):
        if j == '.' or k == '.':
            return 1
        gt |= int(j) > int(k)
        lt |= int(j) < int(k)
        if gt and lt:
            break
    return int(gt) - int(lt)


def find_min():
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

def laziest_node(r_nodes):
    return min(r_nodes.items(), key=lambda x: x[1])[0]




