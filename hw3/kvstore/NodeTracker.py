from threading import Thread, Timer, Event
import requests as req
import time
from rest_framework.response import Response
from rest_framework import status

class NodeTracker(Thread):

    def __init__(self, IPPORT, event):
        self.ip_port = IPPORT
        #Thread.__init__(self)
        self.stopped = event

    # DOESN'T RETURN         = DO NOTHING
    # RETURN TYPE DICTIONARY = UPDATE views.AVAIL_IP with run's OUTPUT
    # RETURN STRING_LIST     = RETURNS STRING_LIST [PROXY_IP , REPLICA_DOWN_IP]
    # RETURN STRING          = RETURNS STRING "REPLICA_IP" WHICH WE NEED TO DIRECT TO MERGING DATA
    # RETURN STRING          = RETURNS STRING 'degraded", BECAUSE WE DON'T HAVE AVAILABLE PROXIES
    def run(self):
        global AVAILIP
        global replica_nodes

        while not self.stopped().wait(1.0):
            for k, v in AVAILIP.items():
                try:
                    url_str = 'http://' + k + '/kv-store/check_nodes'
                    res = req.get(url_str, timeout=1)

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

                    # CASE 1C
                    continue
                    # ALWAYS GOING TO SET KEY FALSE FOR AVAIL_IP[k]
                    #AVAILIP[k] = False

                #CASE 2
                # SUCCESSFUL COMMUNICATION WITH NODE
                if res.status_code == 200:
                    # CASE 2C
                    # IF dict[k] WAS ALREADY EQUAL TO True THEN WE GOOD, JUST AN UP NODE THAT'S STILL UP
                    if AVAILIP[k] is False:
                        AVAILIP[k] = True
                    # IF THE NODE USED TO BE FALSE, AND NOW IT IS TRUE, A PARTITION JUST HEALED
                    # AND WE NEED TO COMPARE VECTOR CLOCKS
                    # CASE 2A
                    # SEND REQUEST TO CHANGE AVAILIP
                        if k not in replica_nodes:
                            replica_nodes.append(k)
                        # CASE 2B
                        else:
                            req_str = 'http://' + self.ip_port + '/kv-store/merge_nodes'
                            req.put(req_str,
                                    data={'key':k},
                                    timeout=1)
            # AFTER CHECKING ALL NODES TAKE A NAP
            # time.sleep(1)

            ''''# FOLLOWING ELSE MIGHT BE REDUNDANT SINCE WE ALREADY CHECKED IN EXCEPTION
            else:
                # NODE IS DEAD
                # NOW WE NEED TO PROMOTE A PROXY AND DEMOTE OUR REPLICATED NODE
                dict[k] = False'''