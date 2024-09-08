#!/bin/bash

curl -S -X POST -H "Content-Type: application/json" -d '{ 
  "metric": "testM4",
  "timestamp": 12345678901, 
  "value": 11, 
  "tags": {
      "host": "server1"
  }
}' http://localhost:6182/api/put
