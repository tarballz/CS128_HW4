#!/usr/bin/env bash

sudo docker run -d -p 8083:8080 --net=mynet -e K=2 --ip=10.0.0.3 -e VIEW="10.0.0.3:8080,10.0.0.4:8080,10.0.0.5:8080,10.0.0.6:8080" -e IPPORT="10.0.0.3:8080" hw4

sudo docker run -d -p 8084:8080 --net=mynet -e K=2 --ip=10.0.0.4 -e VIEW="10.0.0.3:8080,10.0.0.4:8080,10.0.0.5:8080,10.0.0.6:8080" -e IPPORT="10.0.0.4:8080" hw4

sudo docker run -d -p 8085:8080 --net=mynet -e K=2 --ip=10.0.0.5 -e VIEW="10.0.0.3:8080,10.0.0.4:8080,10.0.0.5:8080,10.0.0.6:8080" -e IPPORT="10.0.0.5:8080" hw4

sudo docker run -d -p 8086:8080 --net=mynet -e K=2 --ip=10.0.0.6 -e VIEW="10.0.0.3:8080,10.0.0.4:8080,10.0.0.5:8080,10.0.0.6:8080" -e IPPORT="10.0.0.6:8080" hw4

sudo docker run -d -p 8087:8080 --net=mynet -e K=2 --ip=10.0.0.7 -e IPPORT="10.0.0.7:8080" hw4

sudo docker run -d -p 8088:8080 --net=mynet -e K=2 --ip=10.0.0.8 -e IPPORT="10.0.0.8:8080" hw4

sudo docker run -d -p 8089:8080 --net=mynet -e K=2 --ip=10.0.0.9 -e IPPORT="10.0.0.9:8080" hw4