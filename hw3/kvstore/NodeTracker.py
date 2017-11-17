from threading import Thread, Timer, Event
import requests as req
import time
from rest_framework.response import Response
from rest_framework import status

class NodeTracker(Thread):

    def __init__(self, IPPORT,  AVAILIP, event):
        self.avail_ip = AVAILIP
        self.ip_port = IPPORT
        #Thread.__init__(self)
        self.stopped = event

    # DOESN'T RETURN         = DO NOTHING
    # RETURN TYPE DICTIONARY = UPDATE views.AVAIL_IP with run's OUTPUT
    # RETURN STRING_LIST     = RETURNS STRING_LIST [PROXY_IP , REPLICA_DOWN_IP]
    # RETURN STRING          = RETURNS STRING "REPLICA_IP" WHICH WE NEED TO DIRECT TO MERGING DATA
    # RETURN STRING          = RETURNS STRING 'degraded", BECAUSE WE DON'T HAVE AVAILABLE PROXIES
    def run(self, replica_nodes,):
        while not self.stopped():
            for k, v in self.avail_ip:
                try:
                    url_str = 'http://' + k + '/kv-store/check_nodes'
                    res = req.get(url_str, timeout=1)

                # THIS IS A CHECK TO KNOW IF THE NODE USED TO BE UP AND
                # NOW IT IS DOWN, THEREFORE A PARTITION JUST HAPPENED
                # SINCE LAST MESSAGE SENT
                # except requests.exceptions.RequestException as e
                except Exception:
                    # CHECK IF THE IP USED TO BE UP
                    if self.avail_ip[k] is True:
                        # CASE 1A:
                        # IF IT WAS A PROXY THEN WE ARE COOL, REMOVE FROM AVAIL_IP
                        if k not in replica_nodes:
                            self.avail_ip[k] = False
                            req_str = 'http://' + self.ip_port + '/kv-store/update_AVAILIP'
                            res = req.put(req_str,
                                          json=self.avail_ip
                                          headers={'content-type': 'application/json'},
                                          timeout=1)
                            continue
                        # CASE 1B:
                        # IF IT WAS A REPLICA, WE NEED TO CHECK AND PROMOTE A PROXY IF POSSIBLE
                        else:
                            self.avail_ip[k] = False
                            req_str = 'http://' + self.ip_port + '/kv-store/demote_replica'
                            res = req.put(req_str,
                                          json=self.avail_ip,
                                          headers={'content-type': 'application/json'},
                                          timeout=1)
                            continue
                            '''promote_ip = -1
                            # CHECK IF WE HAVE PROXIES AVAILABLE
                            for ip, status in self.avail_ip:
                                if status == True and ip not in replica_nodes:
                                    promote_ip = ip
                                    break
                            if promote_ip != -1:
                                for n, m in enumerate(replica_nodes):
                                    if m == k:
                                        self.avail_ip[k] = False
                                        list = [promote_ip, replica_nodes[n]]
                                        return list
                                        #replica_nodes[n] = promote_ip
                                        # NOW WE NEED TO MAKE SURE THAT THIS NEW IP GETS ALL THE INFO
                                        # FROM ANOTHER REPLICA
                            else:
                                # GO INTO DEGRADED MODE
                                self.avail_ip[k] = False
                                return 'degraded'''''
                    # CASE 1C
                    else:
                        continue
                    # ALWAYS GOING TO SET KEY FALSE FOR AVAIL_IP[k]
                    #self.avail_ip[k] = False

                #CASE 2
                # SUCCESSFUL COMMUNICATION WITH NODE
                if (res.status_code == 200):
                    # CASE 2C
                    # IF dict[k] WAS ALREADY EQUAL TO True THEN WE GOOD, JUST AN UP NODE THAT'S STILL UP
                    if self.avail_ip[k] == True:
                        continue
                    # IF THE NODE USED TO BE FALSE, AND NOW IT IS TRUE, A PARTITION JUST HEALED
                    # AND WE NEED TO COMPARE VECTOR CLOCKS
                    else:
                        # CASE 2A
                        # SEND REQUEST TO CHANGE AVAILIP
                        if k not in replica_nodes:
                            self.avail_ip[k] = True
                            req_str = 'http://' + self.ip_port + '/kv-store/update_AVAILIP'
                            res = req.put(req_str,
                                            json=self.avail_ip,
                                            headers={'content-type':'application/json'},
                                            timeout=1)
                            continue
                        # CASE 2B
                        else:
                            self.avail_ip[k] = True
                            req_str = 'http://' + self.ip_port + '/kv-store/merge_nodes'
                            res = req.put(req_str,
                                          json=k,
                                          headers={'content-type':'application/json'},
                                          timeout=1)
                            continue
            # AFTER CHECKING ALL NODES TAKE A NAP
            time.sleep(1)

            ''''# FOLLOWING ELSE MIGHT BE REDUNDANT SINCE WE ALREADY CHECKED IN EXCEPTION
            else:
                # NODE IS DEAD
                # NOW WE NEED TO PROMOTE A PROXY AND DEMOTE OUR REPLICATED NODE
                dict[k] = False'''