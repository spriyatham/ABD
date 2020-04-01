from concurrent import futures

import grpc

import abd_pb2_grpc
#import ABDServer
import time
import sys

from ABDServer import ABDServer

"""
usage: ABDServerManager.py <port> <server_name> <directory to save the registers>
"""
port = sys.argv[1]
server_name = sys.argv[2]
register_path = sys.argv[3]

abd_server = ABDServer(server_name, register_path)

server = grpc.server(futures.ThreadPoolExecutor())
abd_pb2_grpc.add_ABDServiceServicer_to_server(abd_server, server)
server.add_insecure_port("[::]:"+port)
server.start()
# for now i will just let it run
while True:
    time.sleep(60)
