# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import sys
import socket
import time
HOST="localhost"
PORT=int(sys.argv[1])
httpheader = "GET /testcase1.html HTTP1.1\r\nHost: www.hjc.com\r\nConnection: close\r\n\r\n"
httpheader2 = "GET /image1.png HTTP1.1\r\nHost: www.hjc.com\r\nConnection: close\r\n\r\n"
httpheader3 = "GET /testcase1.html HTTP1.1\r\nHost: www.hjc.com\r\n\r\n"


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST,PORT))
    s.sendall(httpheader3.encode())
    while True:
        data=s.recv(1024)
        if not data:
            break
        #data_decode=data.decode('utf-8')
        print ('Received-',data)
     
#    time.sleep(2)
#    s.sendall(httpheader3.encode())
#    while True:
#        data=s.recv(1024)
#        if not data:
#            break
#        data_decode=data.decode('utf-8')
#        print ('Received-',data_decode)
        

