#!/bin/bash

# 8 digits
echo "value:"
curl -s 'http://localhost:4242/api/query?start=12345678&m=avg:testM4'
echo "count:"
curl -s 'http://localhost:4242/api/query?start=1970/05/23-14:21&m=count:testM4'

# 9 digits
#curl -s 'http://localhost:4242/api/query?start=123456789000&m=avg:testM3'
echo ""
#curl -s 'http://localhost:4242/api/query?start=1973/11/29-13:33&m=avg:testM3'

#curl -s 'http://localhost:4242/api/query?start=1234567890&m=avg:testM3'
#curl -s 'http://localhost:4242/api/query?start=2009/02/13-15:33&m=avg:testM3'
