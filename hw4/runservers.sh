#!/bin/bash
# Author: Shane Dalton
# Created for UCSC undergrad course CMPS128, Fall 2017

# Initializes all servers listed in the ports list

echo "test"
echo "From server:"
num_servers=$1
echo $num_servers
K=4
num_servers=4
#VIEW1="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080"
#fill in this list with the addresses of all servers you want to spawn
#VIEW="localhost:5000,localhost:5001,localhost:5002,localhost:5003"
#starting port range
#port=5000


# Starting port range
port=8080
port2=8080
#echo "putting foo:add on server"
#echo "getting foo:add from server"


echo "+++++++++++++++++++++++++++++++++++++++++++++++"
echo "                BEFORE ADD                     "
echo "+++++++++++++++++++++++++++++++++++++++++++++++"

for i in $(seq 1 $num_servers)		#"${ports[@]}"
do
	echo "Getting State of localhost:$port"
	curl -X GET "localhost:$port/kv-store/get_state"
	let "port=port+1"
    echo " "
    echo " "
done
sleep .5
port=8080
curl -X PUT "localhost:8080/kv-store/update_view?type=add" -d ip_port=localhost:8083
echo "Adding localhost:8083"
echo "+++++++++++++++++++++++++++++++++++++++++++++++"
echo "                AFTER ADDING                   "
echo "+++++++++++++++++++++++++++++++++++++++++++++++"
for i in $(seq 1 $num_servers)		#"${ports[@]}"
do
	echo "Getting State of localhost:$port"
	curl -X GET "localhost:$port/kv-store/get_state"
	let "port=port+1"
    echo " "
done

sleep .5
port=8080
curl -X PUT "localhost:8080/kv-store/update_view?type=remove" -d ip_port=localhost:8083
echo " Removing localhost:8083"

echo "+++++++++++++++++++++++++++++++++++++++++++++++"
echo "                REMOVING                       "
echo "+++++++++++++++++++++++++++++++++++++++++++++++"

# REMOVE HERE

for i in $(seq 1 $num_servers)		#"${ports[@]}"
do
	echo "Getting State of localhost:$port"
	curl -X GET "localhost:$port/kv-store/get_state"
	let "port=port+1"
    echo " "
done
port=8080
sleep .5
echo " "
echo "+++++++++++++++++++++++++++++++++++++++++++++++"
echo "            AFTER REMOVING                     "
echo "+++++++++++++++++++++++++++++++++++++++++++++++"


for i in $(seq 1 $num_servers)		#"${ports[@]}"
do
	echo "Getting State of localhost:$port"
	curl -X GET "localhost:$port/kv-store/get_state"
	let "port=port+1"
    echo " "
done
port=$port2

#curl -X PUT localhost:5000/kv-store/foo -d val=add






#curl -X PUT "localhost:808"



    # d1 = json.loads(d) string->dictionary  d - > d1
    # d2 = json.dumps(d)  dictionary -> string     d2 - > d
    # d3 = json.dumps(json.loads(d))  # 'dumps' gets the dict from 'loads' this time