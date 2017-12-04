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

def chunk_list(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

# SET DEBUG TO True  IF YOU'RE WORKING LOCALLY
# SET DEBUG TO False IF YOU'RE WORKING THROUGH DOCKER
DEBUG = False

# Environment variables.
K = int(os.getenv('K', 3))
VIEW = os.getenv('VIEW', "0.0.0.0:8080,10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080")
if DEBUG:
    print("VIEW is of type: %s" % (type(VIEW)))
IPPORT = os.getenv('IPPORT', None)
current_vc = collections.OrderedDict()
# AVAILIP = nodes that are up.
AVAILIP = {}

all_nodes = []
replica_nodes = []
proxy_nodes = []
degraded_mode = False

# if IPPORT != "0.0.0.0":
#     IP = IPPORT.split(':')[0]

if DEBUG:
    # This is just for testing locally.
    if VIEW != "0.0.0.0:8080":
        all_nodes = VIEW.split(',')
    else:
        all_nodes = [VIEW]

if not DEBUG:
    all_nodes = VIEW.split(',')

if DEBUG:
    print("all_nodes: %s" % (all_nodes))
    print("len of all_n: %d" % (len(all_nodes)))

if DEBUG:
    print(list(current_vc.values()))

if DEBUG:
    print("proxy_nodes: %s" % (proxy_nodes))
    print("len of prox_n: %d" % (len(proxy_nodes)))
    print("replica_nodes: %s" % (replica_nodes))
    print("len of rep_n: %d" % (len(replica_nodes)))


# INITIAL NUMBER OF PARTITIONS
num_groups = len(all_nodes) // K  # Integer division.
num_replicas = len(all_nodes) - (len(all_nodes) % K)
BASE = 2
#POWER = 9
#POWER = num_groups**2
MAX_HASH_NUM = BASE**9


groups_dict = {}

upper_bound = (MAX_HASH_NUM // num_groups)
chunked = chunk_list(all_nodes, K)

my_upper_bound = -1

for chunk in chunked:
    if len(chunk) >= K:
        groups_dict[upper_bound] = chunk
        for node in chunk:
            if IPPORT == node:
                my_upper_bound = upper_bound
            replica_nodes.append(node)
        upper_bound += (MAX_HASH_NUM // num_groups)
    else:
        for node in chunk:
            proxy_nodes.append(node)

if MAX_HASH_NUM > upper_bound:
    MAX_HASH_NUM = upper_bound

groups_sorted_list = [(k, groups_dict[k]) for k in sorted(groups_dict, key=int)]

# I think this can be replica_nodes and not
# all nodes b/c only the client is going to
# interacting with a proxy.
for node in replica_nodes:
    current_vc[node] = 0
    AVAILIP[node] = True


def is_replica():
    return (IPPORT in replica_nodes)


# FAILURE RESPONSE -- BAD KEY INPUT
@api_view(['GET', 'PUT'])
def failure(request, key):
    return Response({'result': 'error', 'msg': 'Key not valid'}, status=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE)


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
    existing_entry = None
    existing_timestamp = None

    # MAIN RESPONSE
    if is_replica():
        # MAIN PUT
        if method == 'PUT':
            new_entry = False
            # ERROR HANDLING: INVALID KEY TYPE (NONE)
            if 'val' not in request.data:
                return Response({'result': 'error', 'msg': 'No value provided'}, status=status.HTTP_400_BAD_REQUEST)
            input_value = request.data['val']

            # ERROR HANDLING: EMPTY VALUE or TOO LONG VALUE
            if 'val' not in request.data or sys.getsizeof(input_value) > 1024 * 1024 * 256:
                return Response({'result': 'error', 'msg': 'No value provided'}, status=status.HTTP_400_BAD_REQUEST)
            # Maybe comment this out b/c causal payload can be '' in case if no reads have happened yet?
            if 'causal_payload' not in request.data:
                return Response({'result': 'error', 'msg': 'No causal_payload provided'},
                                status=status.HTTP_400_BAD_REQUEST)

            # IF DATA HAS node_id, THEN WE'VE RECEIVED NODE-TO-NODE COMMUNICATION
            # AND NEED TO STORE IT.
            if 'node_id' in request.data:
                # BUILD INCOMING OBJECT.
                try:
                    # incoming_key = str(request.data['key'])
                    incoming_value = str(request.data['val'])
                    incoming_cp = str(request.data['causal_payload'])
                    incoming_node_id = int(request.data['node_id'])
                    incoming_timestamp = int(request.data['timestamp'])
                    is_GET_broadcast = int(request.data['is_GET_broadcast'])
                except:
                    return Response({'result': 'error', 'msg': 'Cannot construct node-to-node entry'},
                                    status=status.HTTP_428_PRECONDITION_REQUIRED)

                cp_list = incoming_cp.split('.')

                if is_GET_broadcast == 1:
                    try:
                        existing_entry = Entry.objects.get(key=key)
                        my_cp = str(existing_entry.causal_payload).split('.')
                        my_timestamp = int(existing_entry.timestamp)
                        # Incoming cp > my cp
                        if (compare_vc(cp_list, my_cp) == 1) or (
                                    (compare_vc(cp_list, my_cp) == 0) and (incoming_timestamp >= my_timestamp)):
                            update_current_vc(cp_list)
                            Entry.objects.update_or_create(key=key, defaults={'val': incoming_value,
                                                                              'causal_payload': incoming_cp,
                                                                              'node_id': incoming_node_id,
                                                                              'timestamp': incoming_timestamp})
                            return Response({'result': 'Success', 'msg': 'Replaced'},
                                            status=status.HTTP_202_ACCEPTED)
                        else:
                            return Response({'result': 'failure', 'msg': 'Can\'t go back in time.'},
                                            status=status.HTTP_406_NOT_ACCEPTABLE)


                    except:
                        # FAILURE: KEY DOES NOT EXIST
                        # CREATE ENTRY IN OUR DB SINCE THE ENTRY DOESN'T EXIST.
                        Entry.objects.update_or_create(key=key, defaults={'val': incoming_value,
                                                                          'causal_payload': incoming_cp,
                                                                          'node_id': incoming_node_id,
                                                                          'timestamp': incoming_timestamp})
                        return Response({'result': 'Success', 'msg': 'Key does not exist'},
                                        status=status.HTTP_201_CREATED)

                # NOT A GET BROADCAST, SO HANDLE THE PUT NORMALLY.
                # IF INCOMING_CP > CURRENT_VC
                elif compare_vc(cp_list, list(current_vc.values())) == 1:
                    update_current_vc(cp_list)
                    Entry.objects.update_or_create(key=key, defaults={'val': incoming_value,
                                                                      'causal_payload': incoming_cp,
                                                                      'node_id': incoming_node_id,
                                                                      'timestamp': incoming_timestamp})
                    return Response(
                        {'result': 'success', "value": incoming_value, "node_id": incoming_node_id,
                         "causal_payload": incoming_cp,
                         "timestamp": incoming_timestamp}, status=203)  # status.HTTP_200_OK

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
                                        status=204)  # status.HTTP_201_CREATED
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
                # elif compare_vc(cp_list, list(current_vc.values())) == -1:
                else:
                    return Response({'result': 'failure', 'msg': 'Can\'t go back in time.'},
                                    status=status.HTTP_406_NOT_ACCEPTABLE)


            # IF NO TIMESTAMP, WE KNOW THIS PUT IS FROM THE CLIENT.
            else:
                incoming_cp = str(request.data['causal_payload'])
                node_id = list(current_vc.keys()).index(IPPORT)
                new_timestamp = int(time.time())

                if DEBUG:
                    print("incoming_cp_CLIENT: %s" % (incoming_cp))
                    print(len(incoming_cp))

                # FIRST ATTEMPT AT MAPPING A KEY TO A GROUP, AND FORWARDING IF THE HASHED KEY DOES NOT
                # MATCH OUR GROUP.
                # if key_to_group_hash(key) != groups_dict[IPPORT]:
                #     for k in groups_dict:
                #         url_str = 'http://' + k + '/kv-store/' + key
                #         try:
                #             # ACT AS A PSEUDO-PROXY.
                #             res = req.put(url=url_str, data={'val': input_value,
                #                                              'causal_payload': incoming_cp,
                #                                              'timestamp': new_timestamp}, timeout=0.5)
                #             response = Response(res.json())
                #             response.status_code = res.status_code
                #             return response
                #         except:
                #             continue
                # else:
                #     broadcast(key, input_value, incoming_cp, node_id, new_timestamp, 0)
                #     Entry.objects.update_or_create(key=key, defaults={'val': input_value,
                #                                                       'causal_payload': incoming_cp,
                #                                                       'node_id': node_id,
                #                                                       'timestamp': new_timestamp})
                #     return Response(
                #         {'result': 'success', "value": input_value, "node_id": node_id, "causal_payload": incoming_cp,
                #          "timestamp": new_timestamp}, status=209)  # status.HTTP_201_CREATED
                # # END ATTEMPT.


                    # len(causal_payload) == 0 if the user hasn't done ANY reads yet.
                if len(incoming_cp) <= 2:
                    incoming_cp = ''
                    if DEBUG:
                        print("init triggered")
                    # Initialize vector clock.
                    for k, v in current_vc.items():
                        if AVAILIP[k]:
                            # incoming_cp += str(v) + '.'
                            # INCREMENT OUR LOCATION IN THE CP
                            if IPPORT == str(k):
                                v += 1
                            # BUILD INCOMING_CP SINCE WE'RE NOT PROVIDED ONE
                            incoming_cp += ''.join([str(v), '.'])

                    # STRIP LAST PERIOD FROM INCOMING CP
                    incoming_cp = incoming_cp.rstrip('.')

                    if DEBUG:
                        print("zero icp: %s" % (incoming_cp))

                    if not DEBUG:
                        # ping_nodes()
                        broadcast(key, input_value, incoming_cp, node_id, new_timestamp, 0)

                    Entry.objects.update_or_create(key=key, defaults={'val': input_value,
                                                                      'causal_payload': incoming_cp,
                                                                      'node_id': node_id,
                                                                      'timestamp': new_timestamp})
                    return Response(
                        {'result': 'success', "value": input_value, "node_id": node_id, "causal_payload": incoming_cp,
                         "timestamp": new_timestamp}, status=205)  # status.HTTP_201_CREATED

                # USER HAS DONE READS BEFORE
                else:
                    cp_list = incoming_cp.split('.')
                    # Need to do a GET to either compare values or confirm this entry is being
                    # entered for the first time.
                    existing_entry = None
                    try:
                        existing_entry = Entry.objects.get(key=key)
                        # existing_entry = Entry.objects.latest('timestamp')
                        # existing_timestamp = existing_entry.timestamp
                    except:
                        new_entry = True

                    if DEBUG:
                        print("EXISTING ENTRY: ", existing_entry)

                    if not DEBUG:
                        # ping_nodes()
                        broadcast(key, input_value, incoming_cp, node_id, new_timestamp, 0)

                    # if causal_payload > current_vc
                    # I SET THIS TO BE "> -1" B/C IT DOES NOT MATTER IF VCS ARE THE SAME B/C CLIENT WILL NOT PASS A TIMESTAMP
                    if compare_vc(cp_list, list(current_vc.values())) > -1:
                        # print ("OLD VC: %s" % (current_vc))
                        update_current_vc_client(cp_list)
                        incoming_cp = '.'.join(list(map(str, current_vc.values())))
                        if DEBUG:
                            print("cp_list: %s" % (cp_list))
                            for i in cp_list:
                                print("type: %s" % (type(i)))
                            print("incoming_cp: %s" % (incoming_cp))

                        Entry.objects.update_or_create(key=key, defaults={'val': input_value,
                                                                          'causal_payload': incoming_cp,
                                                                          'node_id': node_id,
                                                                          'timestamp': new_timestamp})
                        return Response(
                            {'result': 'success', "value": input_value, "node_id": node_id,
                             "causal_payload": incoming_cp,
                             "timestamp": new_timestamp}, status=206)  # status.HTTP_200_OK


                    # causal payload < current_vc
                    else:
                        return Response({'result': 'failure', 'msg': 'Can\'t go back in time.'},
                                        status=status.HTTP_406_NOT_ACCEPTABLE)


        # MAIN GET
        elif method == 'GET':
            for entry in Entry.objects.all():
                if DEBUG:
                    print("ENTRY INFO:")
                    print(entry.key)
                    print(entry.val)
                    print("END")
                if not DEBUG:
                    # ping_nodes()
                    broadcast(entry.key, entry.val, entry.causal_payload, entry.node_id, entry.timestamp, 1)

            try:
                # KEY EXISTS
                # TODO: There's an issue here where when a node does a PUT, ping_nodes() gets called, which calls
                # TODO: a GET, and the entry gets made here instead of actually in the PUT.
                existing_entry = Entry.objects.get(key=key)
                return Response({'result': 'success', "value": existing_entry.val, "node_id": existing_entry.node_id,
                                 "causal_payload": existing_entry.causal_payload,
                                 "timestamp": existing_entry.timestamp}, status=207)  # status.HTTP_200_OK
            except:
                # ERROR HANDLING: KEY DOES NOT EXIST
                return Response({'result': 'error', 'msg': 'Key does not exist'}, status=status.HTTP_404_NOT_FOUND)


    # PROXY RESPONSE
    else:

        # 	# GENERATE BASE URL STRING
        #     url_str = 'http://'+os.environ['MAINIP']+'/kv-store/'+key
        dest_node = laziest_node(current_vc)
        if DEBUG:
            print("SELECTED ", dest_node, " TO FORWARD TO.")

        # Some letters get chopped off when I forward.  Only retaining last letter..?
        url_str = 'http://' + dest_node + '/kv-store/' + key

        # 	# FORWARD GET REQUEST
        # 		# PACKAGE AND RETURN RESPONSE TO CLIENT
        if method == 'GET':
            res = req.get(url=url_str, timeout=0.5)
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
                AVAILIP[dest_node] = False
                return Response({'result': 'error', 'msg': 'Server unavailable'}, status=501)

        return response


def broadcast(key, value, cp, node_id, timestamp, is_GET_broadcast):
    for k in AVAILIP:
        # IF THE NODE IS UP, AND THE NODE IS NOT ME, AND WE'RE IN THE SAME GROUP
        if AVAILIP[k] and k != IPPORT:
            url_str = 'http://' + k + '/kv-store/' + key
            try:
                req.put(url=url_str, data={'val': value,
                                           'causal_payload': cp,
                                           'node_id': node_id,
                                           'timestamp': timestamp,
                                           'is_GET_broadcast': is_GET_broadcast}, timeout=0.5)
            except:
                AVAILIP[k] = False


# Gross-ass way to update current_vc
def update_current_vc(new_cp):
    # Need to cast new_cp to an int list to I can increment it's elements.
    new_cp = list(map(int, new_cp))
    i = 0
    for k, v in current_vc.items():
        if AVAILIP[k]:
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
        if AVAILIP[k]:
            if IPPORT == k:
                new_cp[i] += 1
            current_vc[k] = new_cp[i]
            i += 1
    if DEBUG:
        print("NEW 1VC: %s" % (current_vc))


def ping_nodes():
    for k in all_nodes:
        if repr(k) != IPPORT:
            if DEBUG:
                print("pinging %s" % (k))
            try:
                url_str = 'http://' + k + '/kv-store/check_nodes'
                res = req.get(url_str, timeout=0.5)
                # CASE 2
                # SUCCESSFUL COMMUNICATION WITH NODE
                if res.status_code == 200:
                    # CASE 2C
                    # IF dict[k] WAS ALREADY EQUAL TO True THEN WE GOOD, JUST AN UP NODE THAT'S STILL UP
                    if AVAILIP[k] is False:
                        AVAILIP[k] = True

            # THIS IS A CHECK TO KNOW IF THE NODE USED TO BE UP AND
            # NOW IT IS DOWN, THEREFORE A PARTITION JUST HAPPENED
            # SINCE LAST MESSAGE SENT
            # except requests.exceptions.RequestException as e
            except Exception:
                # CHECK IF THE IP USED TO BE UP
                if AVAILIP[k] is True:
                    # CASE 1A:
                    # IF IT WAS A PROXY THEN WE ARE COOL, REMOVE FROM AVAIL_IP
                    AVAILIP[k] = False
                    if k in replica_nodes:
                        replica_nodes.remove(k)


@api_view(['PUT'])
def update_view(request):
    new_ipport = request.data['ip_port']
    # ping_nodes()
    node_num = 0
    # print("TYPE IS: %s" % (str(request.GET.get('type'))))

    if request.GET.get('type') == 'add':
        node_num = 0
        if DEBUG:
            print("\t\t\tFor update_view before add")
            print("proxy_nodes: %s" % (proxy_nodes))
            print("len of prox_n: %d" % (len(proxy_nodes)))
            print("replica_nodes: %s" % (replica_nodes))
            print("len of rep_n: %d" % (len(replica_nodes)))
        # Added node should be a replica.
        all_nodes.append(new_ipport)
        AVAILIP[new_ipport] = True
        if len(replica_nodes) < K:
            if DEBUG:
                print("\t\t\tIN DEGRADED MODE\nAPPENDING %s ONTO R_N LIST" % (new_ipport))
            replica_nodes.append(new_ipport)
            # Check if we're resurrecting a node that was previously in our OrderedDict current_vc
            if new_ipport not in current_vc:
                # Init new entry into our dictionary.
                current_vc.update({new_ipport: 0})
                if DEBUG:
                    print("\t\t\tUPDATING CURRENT_VC")
                    print("current_vc: %s" % (current_vc.items()))
                    print("%s should be in current_vc" % (new_ipport))
        # Added node should be a proxy
        elif len(replica_nodes) >= K:
            if DEBUG:
                print("K = %d \t len(r_n) = %d" % (K, len(replica_nodes)))
                print("APPENDING %s onto proxy_nodes" % (new_ipport))
            proxy_nodes.append(new_ipport)
            if new_ipport not in current_vc:
                if DEBUG:
                    print("Never seen %s before. Appending %s to current_vc (AKA global vc)" % (new_ipport, new_ipport))
                # Init new entry into our dictionary.
                current_vc.update({new_ipport: 0})
            degraded_mode = False
        if DEBUG:
            print("\t\t\tFor update_view after add")
            print("proxy_nodes: %s" % (proxy_nodes))
            print("len of prox_n: %d" % (len(proxy_nodes)))
            print("replica_nodes: %s" % (replica_nodes))
            print("len of rep_n: %d" % (len(replica_nodes)))

        for k in AVAILIP:
            if AVAILIP[k] is True:
                node_num += 1

        return Response(
            {"msg": "success", "node_id": list(current_vc.keys()).index(new_ipport),
             "number_of_nodes": node_num},
            status=status.HTTP_200_OK)


    elif request.GET.get('type') == 'remove':
        node_num = 0
        # all_nodes.remove(new_ipport)
        AVAILIP[new_ipport] = False
        if new_ipport in replica_nodes:
            replica_nodes.remove(new_ipport)
            current_vc[new_ipport] = 0
            if len(replica_nodes) < K:
                # If we have any "spare" nodes in proxy_nodes, promote it to a replica.
                if len(proxy_nodes) > 0:
                    promoted = proxy_nodes.pop()
                    replica_nodes.append(promoted)
                    if promoted not in current_vc:
                        current_vc.update({promoted: 0})
                    else:
                        current_vc[promoted] = 0

                    if len(replica_nodes) > K:
                        degraded_mode = False
                    else:
                        degraded_mode = True

        elif new_ipport in proxy_nodes:
            proxy_nodes.remove(new_ipport)

        for k in AVAILIP:
            if AVAILIP[k] is True:
                node_num += 1

        return Response(
            {"msg": "success", "node_id": list(current_vc.keys()).index(new_ipport),
             "number_of_nodes": node_num},
            status=status.HTTP_200_OK)

    return Response({'result': 'error', 'msg': 'key value store is not available'},
                    status=status.HTTP_501_NOT_IMPLEMENTED)


@api_view(['GET'])
def check_nodes(request):
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


#def key_to_group_hash(str):
#    return hash(str) % num_groups

def seeded_hash(str):
    return hash(str) % MAX_HASH_NUM


