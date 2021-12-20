#!/usr/bin/env python3

import socket
import sys
import time

if len(sys.argv) != 3:
   print("Usage: python tcp_plain_writer.py <host> <port>")
   sys.exit(1);
else:
   HOST = sys.argv[1]
   PORT = int(sys.argv[2])

try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
    s.connect((HOST, PORT))
    print('connect to ', HOST, PORT)
    put_data_point1 = 'put tcp.cpu.usr 1633412275 20 host=foo cpu=1'; 
    put_data_point2 = 'put tcp.cpu.sys 1633412275 20 host=foo cpu=1'; 
    # Note each PUT request must be ended with '\n'
    req = put_data_point1 +'\n'+put_data_point2+'\n';

    s.sendall(req.encode('utf-8'));

    # We need to sleep a few seconds before close the socket in this example. 
    # Otherwise TickTock server might not be able to read data as the socket is closed too early.
    time.sleep(5)
 
    print("Done sending two put reqeuests:\n"+req);
    s.close();
except socket.error as e:
    print("Exception: %s", e)

