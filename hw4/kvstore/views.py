import sys
import os
import collections
import time
from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework import status
from .models import Entry
import requests as req
import hashlib


def chunk_list(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


# SET DEBUG TO True  IF YOU'RE WORKING LOCALLY
# SET DEBUG TO False IF YOU'RE WORKING THROUGH DOCKER
DEBUG = False

TEST = False

# Environment variables.
K = int(os.getenv('K', 1))

VIEW = os.getenv('VIEW', None)

if TEST:
    IPPORT = sys.argv[-1]
else:
    IPPORT = os.getenv('IPPORT', 'localhost:8080')

current_vc = collections.OrderedDict()
# AVAILIP = nodes that are up.
AVAILIP = {}

all_nodes = []
replica_nodes = []
proxy_nodes = []
degraded_mode = False

if DEBUG:
    # This is just for testing locally.
    if VIEW != "0.0.0.0:8080":
        all_nodes = VIEW.split(',')
    else:
        all_nodes = [VIEW]

if not DEBUG:
    if VIEW is not None and ',' in VIEW:
        all_nodes = VIEW.split(',')
    elif VIEW is not None:
        all_nodes.append(repr(VIEW))
    else:
        all_nodes = [IPPORT]

for node in all_nodes:
    current_vc[node] = 0
    AVAILIP[node] = True

if DEBUG:
    print("all_nodes: %s" % (all_nodes))
    print("len of all_n: %d" % (len(all_nodes)))
    # print(list(current_vc.values()))
    print("proxy_nodes: %s" % (proxy_nodes))
    print("len of prox_n: %d" % (len(proxy_nodes)))
    print("replica_nodes: %s" % (replica_nodes))
    print("len of rep_n: %d" % (len(replica_nodes)))

# INITIAL NUMBER OF PARTITIONS
num_groups = len(all_nodes) // K  # Integer division.
if num_groups <= 0:
    num_groups = 1
num_replicas = len(all_nodes) - (len(all_nodes) % K)
if num_replicas <= 0:
    num_replicas = 1
BASE = 2
# POWER = 9
# POWER = num_groups**2
MAX_HASH_NUM = BASE ** 9

groups_dict = {}
groups_sorted_list = []
# range of accepted hashed keys for a group
step = (MAX_HASH_NUM // num_groups)
# initial upper
upper_bound = step
# list of lists of nodes and proxies
chunked = None
my_upper_bound = -1
lower_bound = -1


# for each list of nodes in our list of lists of IPPORTS
def chunk_assign():
    global upper_bound
    global my_upper_bound
    global lower_bound
    global step
    global num_groups
    global chunked
    global all_nodes
    global replica_nodes
    global proxy_nodes
    global groups_sorted_list

    chunked = []
    proxy_nodes = []
    replica_nodes = []
    groups_dict = {}

    num_groups = len(all_nodes) // K
    if num_groups <= 0:
        num_groups = 1
    step = (MAX_HASH_NUM // num_groups)
    upper_bound = step
    chunked = chunk_list(all_nodes, K)

    for chunk in chunked:
        if DEBUG:
            print("chunk: %s" % (chunk))
        # if the current list is comprised of enough nodes
        # to be considered a fully functional group
        # if len(chunk) >= K:
        if len(chunk) == K:
            # we associate the list of IPPORTS with an upper bound
            groups_dict[upper_bound] = chunk
            # for each IPPORT in list
            for node in chunk:
                # if node is myself
                if IPPORT == node:
                    if DEBUG:
                        print("found myself.")
                    # i set my upper bound
                    my_upper_bound = upper_bound
                    if DEBUG:
                        print("my_upper_bound: %s" % (my_upper_bound))
                        print("lower_bound: %s" % (lower_bound))
                    # Need this to confirm a key is within our range, and not JUST less than our value.
                    lower_bound = (upper_bound - step) + 1
                    # add node to our view
                    # replica_nodes.append(node)
            # increment the upper range for the next cluster of IPPORTS
            upper_bound += step
        else:
            # list of IPPORTS is not long enough to be a full cluster
            for node in chunk:
                # so we add them the our proxies
                proxy_nodes.append(node)

    # list of our group_dict sorted by the keys -- (key = upper bound) --
    groups_sorted_list = [[k, groups_dict[k]] for k in sorted(groups_dict, key=int)]

    # HACKY FIX FOR K = 1 EDGE CASE
    if K == 1:
        lower_bound = 0
        upper_bound = MAX_HASH_NUM
        my_upper_bound = MAX_HASH_NUM
        replica_nodes = [IPPORT]
        return

    for tup in groups_sorted_list:
        if my_upper_bound == int(tup[0]):
            replica_nodes = tup[1]
            break


chunk_assign()

# take care of inconsistent max allowable hash
# that way a key doesn't get hashed out of range
if MAX_HASH_NUM > upper_bound:
    MAX_HASH_NUM = upper_bound

# if DEBUG:
print("g_s_l: %s" % (groups_sorted_list))

for node in replica_nodes:
    current_vc[node] = 0
    AVAILIP[node] = True


def is_replica():
    return (IPPORT in replica_nodes)


# FAILURE RESPONSE -- BAD KEY INPUT
@api_view(['GET', 'PUT'])
def failure(request, key):
    return Response({'result': 'error', 'error': 'Key not valid'}, status=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE)


@api_view(['GET'])
def get_node_details(request):
    if IPPORT in replica_nodes:
        return Response({"result": "success", "replica": "Yes"}, status=status.HTTP_200_OK)
    elif IPPORT in proxy_nodes:
        return Response({"result": "success", "replica": "No"}, status=status.HTTP_200_OK)
    else:
        return Response({"result": "error", "error": "Node not found"}, status=status.HTTP_404_NOT_FOUND)


@api_view(['GET'])
def get_all_replicas(request):
    return Response({"result": "success", "replicas": replica_nodes}, status=status.HTTP_200_OK)


@api_view(['GET'])
def get_state(request):
    data = {'IP': IPPORT,
            'GSL ': str(groups_sorted_list),
            'ALL NODES': str(all_nodes),
            'PROXIES': str(proxy_nodes),
            'MY_LB': lower_bound,
            'MY_UB': my_upper_bound
            }
    return Response(data=data, status=200)


@api_view(['GET'])
def get_entries(request):
    entries = {}
    if len(Entry.objects.all()) > 0:
        for entry in Entry.objects.all():
            entries[entry.key] = entry.val
    else:
        entries = {"msg": "no entries :("}
    return Response(entries, status=status.HTTP_200_OK)


# CORRECT KEYS
@api_view(['GET', 'PUT'])
def kvs_response(request, key):
    method = request.method
    existing_entry = None
    existing_timestamp = None
    global current_vc

    # MAIN RESPONSE
    if is_replica():
        # MAIN PUT
        if method == 'PUT':
            # if not DEBUG:
            #     ping_nodes()
            new_entry = False
            # ERROR HANDLING: INVALID KEY TYPE (NONE)
            if 'val' not in request.data:
                return Response({'result': 'error', 'error': 'No value provided'}, status=status.HTTP_400_BAD_REQUEST)
            input_value = request.data['val']

            # ERROR HANDLING: EMPTY VALUE or TOO LONG VALUE
            if 'val' not in request.data or sys.getsizeof(input_value) > 1024 * 1024 * 256:
                return Response({'result': 'error', 'error': 'No value provided'}, status=status.HTTP_400_BAD_REQUEST)
            # Maybe comment this out b/c causal payload can be '' in case if no reads have happened yet?
            if 'causal_payload' not in request.data:
                return Response({'result': 'error', 'error': 'No causal_payload provided'},
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
                    return Response({'result': 'error', "error": "key value store is not available"},
                                    status=status.HTTP_428_PRECONDITION_REQUIRED)

                cp_list = incoming_cp.split('.')

                if i_should_store(key):
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
                                return Response({'result': 'success', 'msg': 'replaced'},
                                                status=status.HTTP_202_ACCEPTED)
                            else:
                                return Response({'result': 'error', "error": "key value store is not available"},
                                                status=status.HTTP_406_NOT_ACCEPTABLE)


                        except:
                            # FAILURE: KEY DOES NOT EXIST
                            # CREATE ENTRY IN OUR DB SINCE THE ENTRY DOESN'T EXIST.
                            Entry.objects.update_or_create(key=key, defaults={'val': incoming_value,
                                                                              'causal_payload': incoming_cp,
                                                                              'node_id': incoming_node_id,
                                                                              'timestamp': incoming_timestamp})
                            return Response(
                                {'result': 'success', 'msg': 'Key does not exist', "partition_id": my_upper_bound},
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
                            {'result': 'success', "value": incoming_value, "partition_id": my_upper_bound,
                             "causal_payload": incoming_cp, "timestamp": incoming_timestamp},
                            status=203)  # status.HTTP_200_OK

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
                            return Response(
                                {'result': 'success', 'msg': 'Key does not exist', 'value': incoming_value,
                                 "partition_id": my_upper_bound,
                                 'causal_payload': incoming_cp, 'timestamp': incoming_timestamp},
                                status=204)  # status.HTTP_201_CREATED
                        # IF WE'VE GOTTEN HERE, KEY EXISTS
                        else:
                            if incoming_timestamp > existing_entry.timestamp:
                                Entry.objects.update_or_create(key=key, defaults={'val': incoming_value,
                                                                                  'causal_payload': incoming_cp,
                                                                                  'node_id': incoming_node_id,
                                                                                  'timestamp': incoming_timestamp})
                                return Response(
                                    {'result': 'success', "value": incoming_value, "partition_id": my_upper_bound,
                                     "causal_payload": incoming_cp, "timestamp": incoming_timestamp},
                                    status=status.HTTP_200_OK)
                            else:
                                return Response({'result': 'error', "error": "key value store is not available"},
                                                status=status.HTTP_406_NOT_ACCEPTABLE)

                    # IF INCOMONG_CP < CURRENT_VC
                    # elif compare_vc(cp_list, list(current_vc.values())) == -1:
                    else:
                        return Response({'result': 'error', "error": "key value store is not available"},

                                        status=status.HTTP_406_NOT_ACCEPTABLE)
                # IF I SHOULD NOT STORE KEY.
                else:
                    return Response({'result': 'error', "error": "key value store is not available"},
                                    status=status.HTTP_412_PRECONDITION_FAILED)


            # =====================================================
            # IF NO NODE_ID, WE KNOW THIS PUT IS FROM THE CLIENT.
            # =====================================================
            else:
                incoming_cp = str(request.data['causal_payload'])
                node_id = list(current_vc.keys()).index(IPPORT)
                new_timestamp = int(time.time())

                if DEBUG:
                    entry_list = []
                    for entry in Entry.objects.all():
                        entry_list.append(entry.toJSON())
                    print("\n\nJSON LIST: %s\n\n" % (entry_list))

                if DEBUG:
                    print("incoming_cp_CLIENT: %s" % (incoming_cp))
                    print(len(incoming_cp))
                # CHECK IF WE WANT TO CREATE AN ENTRY AND STORE IN DB
                if i_should_store(key):
                    # len(causal_payload) == 0 if the user hasn't done ANY reads yet.
                    if len(incoming_cp) <= 2:
                        incoming_cp = ''
                        if DEBUG:
                            print("init triggered")
                        # Initialize vector clock.
                        for k, v in current_vc.items():
                            # incoming_cp += str(v) + '.'
                            # INCREMENT OUR LOCATION IN THE CP
                            if IPPORT == str(k):
                                current_vc[IPPORT] = int(current_vc[IPPORT]) + 1
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
                            {'result': 'success', "value": input_value, "partition_id": my_upper_bound,
                             "causal_payload": incoming_cp, "timestamp": new_timestamp},
                            status=205)  # status.HTTP_201_CREATED

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
                                {'result': 'success', "value": input_value, "partition_id": my_upper_bound,
                                 "causal_payload": incoming_cp, "timestamp": new_timestamp},
                                status=206)  # status.HTTP_200_OK


                        # causal payload < current_vc
                        else:
                            return Response({'result': 'error', "error": "key value store is not available"},
                                            status=status.HTTP_412_PRECONDITION_FAILED)
                else:
                    return selective_broadcast(key, input_value, incoming_cp)
                    # return Response({'msg': 'hashed key is not in my range.', 'my_upper_bound': my_upper_bound}, status=status.HTTP_412_PRECONDITION_FAILED)

        # MAIN GET
        elif method == 'GET':
            # if not DEBUG:
            #     ping_nodes()
            if len(Entry.objects.all()) > 0:
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

            except Entry.DoesNotExist:
                # ERROR HANDLING: KEY DOES NOT EXIST
                return Response({'result': 'error', "error": "key value store is not available"},
                                status=status.HTTP_412_PRECONDITION_FAILED)

            return Response({'result': 'success', "value": existing_entry.val, "partition_id": my_upper_bound,
                             "causal_payload": existing_entry.causal_payload,
                             "timestamp": existing_entry.timestamp}, status=207)  # status.HTTP_200_OK


    # PROXY RESPONSE
    else:

        # 	# GENERATE BASE URL STRING
        #     url_str = 'http://'+os.environ['MAINIP']+'/kv-store/'+key
        # dest_node = laziest_node(replica_nodes)

        # NEED TO GET PROPER CLUSTER TO FORWARD TO.
        dest_node = ''
        sh = seeded_hash(key)
        for tup in groups_sorted_list:
            if sh <= tup[0]:
                ip_list = tup[1]
                # JUST GRABBING THE FIRST NODE, MAYBE REPLACE WITH RANDOM
                dest_node = ip_list[0]
                break

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
                return Response({'result': 'error', "error": "key value store is not available"},
                                status=status.HTTP_412_PRECONDITION_FAILED)

        return response


def broadcast(key, value, cp, node_id, timestamp, is_GET_broadcast):
    global AVAILIP

    for node in replica_nodes:
        if node != IPPORT:
            url_str = 'http://' + node + '/kv-store/' + key
            try:
                req.put(url=url_str, data={'val': value,
                                           'causal_payload': cp,
                                           'node_id': node_id,
                                           'timestamp': timestamp,
                                           'is_GET_broadcast': is_GET_broadcast}, timeout=0.5)
            except:
                AVAILIP[node] = False
                return Response(
                    {"result": "error", "error": "key value store is not available", "partition_id": my_upper_bound},
                    status=status.HTTP_424_FAILED_DEPENDENCY)
    return Response(status=status.HTTP_200_OK)


def selective_broadcast(key, value, cp):
    sh = seeded_hash(key)
    for tup in groups_sorted_list:
        if sh <= tup[0]:
            for dest_node in tup[1]:
                try:
                    url_str = 'http://' + dest_node + '/kv-store/' + key
                    res = req.put(url=url_str, data={'val': value,
                                                     'causal_payload': cp}, timeout=0.5)
                    response = Response(res.json())
                    response.status_code = res.status_code
                    # return response
                except Exception:
                    AVAILIP[dest_node] = False
                    # continue
                    return Response(
                        {"result": "error", "error": "key value store is not available",
                         "partition_id": my_upper_bound},
                        status=status.HTTP_424_FAILED_DEPENDENCY)
            break
    # return Response({"msg": "selective_broadcast"}, status=299)
    return response


def i_should_store(key):
    sh = seeded_hash(key)
    if DEBUG:
        print("hashed key: %s" % (sh))
        print("my_ub: %s" % (my_upper_bound))
    return (sh >= lower_bound and sh <= my_upper_bound)


@api_view(['GET'])
def db_prune(request):
    for entry in Entry.objects.all():
        if not i_should_store(entry.key):
            entry.key = "000000"
    # return Response({"msg": "prune successful"}, status=status.HTTP_205_RESET_CONTENT)
    return Response(
        {"result": "success", "partition_id": my_upper_bound,
         "number_of_partitions": len(groups_sorted_list)},
        status=status.HTTP_200_OK)


def prune_me_and_others(request):
    db_prune(request)
    for dest_node in all_nodes:
        if dest_node != IPPORT:
            url_str = 'http://' + dest_node + '/kv-store/db_prune'
            try:
                res = req.get(url=url_str, data=None)
                response = Response(res.json())
                response.status_code = res.status_code
            except Exception as e:
                return e
                # return Response(
                #     {"result": "error", "error": "key value store is not available",
                #      "partition_id": my_upper_bound},
                #     status=status.HTTP_424_FAILED_DEPENDENCY)
    return response


# Gross-ass way to update current_vc
def update_current_vc(new_cp):
    global current_vc
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
    global current_vc
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
    global AVAILIP

    for k in AVAILIP:
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


# THIS FUNCTION WILL CREATE A NEW VIEW-OF-THE-WORLD AND MODIFY OUR LISTS OF NODES IN ACCORDANCE TO
# HOW MANY NODES WE HAVE (len(all_nodes)) AND HOW MANY NODES ARE IN A CLUSTER.
@api_view(['PUT'])
def update_view(request):
    global replica_nodes
    global proxy_nodes
    global all_nodes
    global current_vc
    global AVAILIP
    global chunked
    global groups_sorted_list
    global my_upper_bound

    new_ipport = request.data['ip_port']

    pusher_e = ''
    prune_e = ''

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
        all_nodes = list(set([item for item in all_nodes]))
        AVAILIP[new_ipport] = True
        if new_ipport not in current_vc:
            # Init new entry into our dictionary.
            current_vc.update({new_ipport: 0})
            if DEBUG:
                print("\t\t\tUPDATING CURRENT_VC")
                print("current_vc: %s" % (current_vc.items()))
                print("%s should be in current_vc" % (new_ipport))

        for k in AVAILIP:
            if AVAILIP[k]:
                node_num += 1

        # replica_nodes = []
        # proxy_nodes = []
        # groups_sorted_list = []
        # REEVALUATE OUR UPPERBOUND AND RE-CHUNK OUR NODES.
        chunk_assign()
        # SEND OUR NEW VIEW-OF-THE-WORLD TO ALL OTHER NODES.
        pusher_e = update_view_pusher()

        # TIME TO PRUNE OUR DB OF KEYS WE ARE NOT SUPPOSED TO CONTAIN.
        time.sleep(0.1)
        prune_e = prune_me_and_others(request)

        if DEBUG:
            print("\t\t\tFor update_view after add")
            print("proxy_nodes: %s" % (proxy_nodes))
            print("len of prox_n: %d" % (len(proxy_nodes)))
            print("replica_nodes: %s" % (replica_nodes))
            print("len of rep_n: %d" % (len(replica_nodes)))
            print("all_nodes: %s" % (all_nodes))

        arg = len(groups_sorted_list)
        if arg == None:
            arg = -1

        if my_upper_bound == None:
            my_upper_bound = -1

        # if len(str(prune_e)) > 0:
        #     return Response({"foo": str(prune_e)}, status=420)
        # if len(str(pusher_e)) > 0:
        #     return Response({"bar": str(pusher_e)}, status=421)

        return Response(
            {"result": "success", "partition_id": my_upper_bound, 'number_of_partitions': arg},
            status=status.HTTP_200_OK)

    elif request.GET.get('type') == 'remove':
        node_num = 0
        all_nodes.remove(new_ipport)
        del AVAILIP[new_ipport]

        for k in AVAILIP:
            if AVAILIP[k]:
                node_num += 1

        replica_nodes = []
        proxy_nodes = []
        groups_sorted_list = []
        # REEVALUATE OUR UPPERBOUND AND RE-CHUNK OUR NODES.
        chunk_assign()
        # SEND OUR NEW VIEW-OF-THE-WORLD TO ALL OTHER NODES.
        update_view_pusher()

        # TIME TO PRUNE OUR DB OF KEYS WE ARE NOT SUPPOSED TO CONTAIN.
        time.sleep(0.1)
        prune_me_and_others(request)

        return Response(
            {"result": "success", "number_of_partitions": len(groups_sorted_list)},
            status=status.HTTP_200_OK)

    return Response({'result': 'error', "error": "key value store is not available"},
                    status=status.HTTP_412_PRECONDITION_FAILED)


# THIS FUNCTION WILL PACKAGE OUR all_nodes list, AVAILIP list, and groups_sorted_list,
# PACKAGE THEM UP AND SEND THEM TO EVERY NODE.  THESE NODES WILL THEN ACCEPT THESE LISTS
# AND ACCEPT THEM AS THE NEW VIEW-OF-THE-WORLD IN update_view_receiver()
def update_view_pusher():
    global all_nodes
    data_dict = {}

    all_nodes = list(set([item for item in all_nodes]))
    if not DEBUG:
        print(all_nodes)
        for dest_node in all_nodes:
            url_str = 'http://' + dest_node + '/kv-store/update_view_receiver'
            data_dict = {'AN': all_nodes,
                         'AIP': AVAILIP,
                         'GSL': groups_sorted_list}
            try:
                print("Sending to : " + dest_node)
                # HAD TO DO JSON= INSTEAD OF DATA= BC WE'RE PASSING A COMPLICATED STRUCTURE
                res = req.put(url=url_str, json=data_dict)
                # response = Response(res.json())
                # response.status_code = res.status_code
            except Exception as e:
                AVAILIP[dest_node] = False
                print(e)
                # continue
                return e
                # return Response({'result': 'error', 'msg': 'Server unavailable'}, status=501)
        # time.sleep(0.025)
        for dest_node in all_nodes:
            url_str = 'http://' + dest_node + '/kv-store/db_broadcast'
            req.put(url=url_str, data=None)


# SET MY LISTS TO THE NEW VIEW-OF-THE-WORLD
@api_view(['PUT'])
def update_view_receiver(request):
    global all_nodes
    global AVAILIP
    global groups_sorted_list
    global my_upper_bound
    global lower_bound

    try:
        new_all_nodes = request.data['AN']
        new_AVAILIP = request.data['AIP']
        new_gsl = request.data['GSL']

        if DEBUG:
            print("new_an: %s" % (new_all_nodes))
            print("new_AVAIL: %s" % (new_AVAILIP))
            print("new_gsl: %s" % (new_gsl))

        all_nodes = new_all_nodes
        AVAILIP = new_AVAILIP
        groups_sorted_list = new_gsl
        chunk_assign()

        print("IP: " + IPPORT + " GSL : " + str(groups_sorted_list))
        print("++++++++++++++++++++")
        print("PRINTING NEW UPDATE VIEW")
        print("++++++++++++++++++++")
        print("GROUPS")
        print("IP: " + IPPORT + " GSL : " + str(groups_sorted_list))
        print(str(all_nodes))
        return Response({'msg': 'shits totally not fucked'}, status=200)

    except Exception as e:
        print(e)
        return Response({'msg': 'shits fucked'}, status=status.HTTP_451_UNAVAILABLE_FOR_LEGAL_REASONS)


@api_view(['GET'])
def get_all_partition_ids(request):
    partition_id_list = []
    for tup in groups_sorted_list:
        partition_id_list.append(tup[0])

    return Response({"result": "success", "partition_id_list": partition_id_list}, status=status.HTTP_200_OK)


@api_view(['GET'])
def get_all_partition_id(request):
    return Response({"result": "success", "partition_id": my_upper_bound}, status=status.HTTP_200_OK)


@api_view(['GET'])
def get_partition_members(request):
    partition_members = []
    try:
        partition_id = int(request.data['partition_id'])
        for tup in groups_sorted_list:
            if partition_id == tup[0]:
                partition_members = tup[1]
                break
        return Response({"result": "success", "partition_members": partition_members})
    except:
        return Response({'result': 'error', "error": "key value store is not available"},
                        status=status.HTTP_412_PRECONDITION_FAILED)


@api_view(['GET'])
def check_nodes(request):
    # new_ipport = request.data['ip_port']
    return Response(status=status.HTTP_200_OK)


# SEND ALL OF MY ENTRIES TO EVERY OTHER NODE.
@api_view(['PUT'])
def db_broadcast(request):
    print("Made it to call broadcast!!!!")
    response = None
    entry_list = []
    e = ''

    # for entry in Entry.objects.all():
    #    entry_list.append(entry.__str__())

    global AVAILIP

    # WE ONLY NEED TO SEND EACH ENTRY TO ONE NODE OUTSIDE OF OUR UPPER_BOUND
    # IF THIS NODE CAN ACCEPT THE ENTRY, IT WILL AND WILL THEN BROADCAST TO OTHER NODES
    # IN IT'S CLUSTER.  IF NOT, IT WILL SELECTIVELY BROADCAST TO A NODE IN THE PROPER CLUSTER.
    if len(Entry.objects.all()) > 0:
        for entry in Entry.objects.all():
            for tup in groups_sorted_list:
                if my_upper_bound != int(tup[0]):
                    ip_list = tup[1]
                    for node in ip_list:
                        url_str = 'http://' + node + '/kv-store/' + entry.key
                        try:
                            res = req.put(url=url_str, data={'val': entry.val,
                                                             'causal_payload': entry.causal_payload,
                                                             'node_id': entry.node_id,
                                                             'timestamp': entry.timestamp,
                                                             'is_GET_broadcast': 0}, timeout=0.5)
                            req.get(url=url_str, data=None)
                            response = Response(res.json())
                            response.status_code = res.status_code
                            break
                            # return response
                        except Exception as e:
                            # continue
                            return Response(
                                {"result": "error", "error": "key value store is not available",
                                 "partition_id": my_upper_bound,
                                 "exception": e},
                                status=status.HTTP_511_NETWORK_AUTHENTICATION_REQUIRED)
            break
            # resp = broadcast(entry.key, entry.val, entry.causal_payload, entry.node_id, entry.timestamp, 1)

            # if response.status_code == 200:
            #     return response
    # IN THIS RETURN, WE HAVE RAN INTO THE CASE: 
    # WHERE MY INSTANCE HAS NOTHING IN ITS DB.
    return Response({"result": "error", "error": "key value store is not available", "partition_id": my_upper_bound,
                     "exception": e}, status=512)

    # return object_broadcast(Entry.objects.all())


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
    for node in replica_nodes:
        if node != IPPORT:
            return node
            # return min(r_nodes.items(), key=lambda x: x[1])[0]


# def key_to_group_hash(str):
#    return hash(str) % num_groups

def seeded_hash(str):
    str = str.encode('utf-8')
    return (int(hashlib.sha1(str).hexdigest(), 16) % MAX_HASH_NUM) + 1
    # return hash(str) % MAX_HASH_NUM
