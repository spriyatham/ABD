import grpc

import abd_pb2
import abd_pb2_grpc
import sys
import threading
import math
import concurrent.futures
import time
from ctypes import c_ulonglong

"""
 usage:
    ABDClient.py <csv s of host:port pairs> <read/write> <register_name> [<value for write>]
"""

"""
send_request_sync()  takes in a stub and request object,
calls the appropriate function synchronously based on the type of the request.
and returns the the response.
"""


def send_request_sync(stub, request_obj, responses, majority):
    read1 = False
    is_fault = False
    try:
        if isinstance(request_obj, abd_pb2.Read1Request):
            response = stub.read1(request_obj)
            read1 = True
        elif isinstance(request_obj, abd_pb2.Read2Request):
            response = stub.read2(request_obj)

        elif isinstance(request_obj, abd_pb2.WriteRequest):
            response = stub.write(request_obj)
            print('response : ACKResponse ', type(response))
    except grpc.RpcError as e:
        print('Error occurred...print details', e)
        #TODO
        # if e is UNAVILABLE - FAILED TO CONNECT or UKNOWN - Stream removed then it means the
        # a server failure ..so a faulty NODE..increment fault count.
        # NOTE: No need to return the ERROR
        print(e.details())
        code = e.code()
        print('name = {}'.format(code.name))
        print('value = {}'.format(code.value))
        if code.value[1] == 'unavailable': # and e.details() == 'failed to connect to all addresses':
            print('Unable to connect to a particular server')
            is_fault = True
        elif code.value[1] == 'unknown': # and e.details() == 'Stream removed'
            print('Server failure while executing the call')
            is_fault = True
        else:
            print('Some other error occurred, still considering fault')
            is_fault = True

        if is_fault:
            fault_count_lock.acquire()
            global num_faults
            num_faults += 1
            fault_count_lock.release()
            return
    ack_count_lock.acquire()
    global num_acks
    num_acks += 1
    print('send_request_sync : new ack received : num_acks= {} '.format(str(num_acks)))
    if read1:
        # this check allows me to just save the responses of just the majority.
        print('send_request_sync : saving read1 ack : num_acks= {} '.format(str(num_acks)))
        if num_acks <= majority:
            responses.append(response)
    ack_count_lock.release()


def communicate_threading_sync(request, channel_list, stub_list):
    global num_acks
    num_acks = 0
    global num_faults
    num_faults = 0
    num_of_servers = len(channels)
    majority = math.ceil((num_of_servers+1)/2.0)
    responses = [] #list of responses i.e info[]
    executor = concurrent.futures.ThreadPoolExecutor()

    # send the message to all the servers
    # futures = [executor.submit(send_request_sync, request, stub_list[i], responses) for i in range(num_of_servers)]
    for j in range(num_of_servers):
        # TODO : check if the channel corresponding to the stub is still active.
        # TODO : You may need future objects, in case of exceptions - "Since I am not controlling any logic with ,
        #  exception ...I dont think...I will need them as of now.."
        executor.submit(send_request_sync, stub_list[j], request, responses, majority)

    while num_acks < majority and num_faults < majority:
        time.sleep(1)
    executor.shutdown(wait=False)

    if num_faults >= majority:
        print('Received {} faults, communicate() failed'.format(num_faults))
        return False
    print('Received {} acknowledgements'.format(num_acks))
    # only read1 will use the responses, remaining calls will ignore the returned response .
    return responses


"""
    This function takes a list of responses to read1request and returns the response with 
    the largest label
"""
def select_max_label(info):
    max_label = -1
    max_response = None
    for response in info:
        if response.timestamp > max_label:
            max_label = response.timestamp
            max_response = response
    return max_response.value, max_response.timestamp


print("Client : initiating client process..")
print("Client : parsing command line input")

connection_params = sys.argv[1]
print("Client : connections params = {}".format(connection_params))

operation = sys.argv[2]
print("Client : operation = {}".format(operation))

register_name = sys.argv[3]
print("Client : register name = {}".format(register_name))

value = None
if operation.lower() == 'write':
    value = sys.argv[4]
    print("Client : value = {}".format(value))

current_milli_time = lambda: int(round(time.time() * 1000))
server_list = connection_params.split(',')
channels = {}
stubs = {}
num_acks=0
num_faults = 0
ack_count_lock = threading.Lock()
fault_count_lock = threading.Lock()

for i, server in enumerate(server_list, 1):

    print("Client : creating channel to {}".format(server))
    channels[i-1] = grpc.insecure_channel(server)
    print("Client : channel created to {}".format(server))
    print("Client : creating stub to {}".format(server))
    stubs[i-1] = abd_pb2_grpc.ABDServiceStub(channels[i-1])
    print("Client : stub created to {}".format(server))
    # TODO: Subscribe() a callback to see if the connection state changes and look at how to handle ChannelConnectivity state changes.
    #  Note: Not doing it as of now


if operation.lower() == 'write':
    write_request = abd_pb2.WriteRequest(register=register_name, timestampe=current_milli_time(), value=value)
    result = communicate_threading_sync(write_request, channels, stubs)
    if isinstance(result, bool): # I only return false, so this check is enough.
        print('write failure')
    else:
        print('write successful')
elif operation.lower() == 'read':
    print('begin read operation...')
    print('begin read1 operation of {}...'.format(register_name))
    read1_request = abd_pb2.Read1Request(register=register_name)
    responses = communicate_threading_sync(read1_request, channels, stubs)
    if isinstance(responses, bool):
        print('read failed')
        sys.exit(-1)
    print('read1 of {} complete : got {} responses'.format(register_name, len(responses)))
    # select the response with the largest label
    value, label = select_max_label(responses)
    print('read1 of {} : selected label : {} and value : {}'.format(register_name, label, value))
    print('begin read2 operation of {}...'.format(register_name))
    read2_request = abd_pb2.Read2Request(register=register_name, timestamp=label, value=value)
    result = communicate_threading_sync(read2_request, channels, stubs)
    if isinstance(result, bool):
        print('read failed')
        sys.exit(-1)
    print('end read2')
    print('({} : {} : {} )'.format(register_name, value, label))
    print('read completed.')