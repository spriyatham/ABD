

import abd_pb2
import abd_pb2_grpc
from ctypes import c_ulonglong
import os.path
import shutil
import shelve


class ABDServer(abd_pb2_grpc.ABDServiceServicer):
    REGISTER_NAME = "register_name"
    TIMESTAMP = "timestamp"
    VALUE = "value"
    # Shelved File extensions
    bak = ".bak"
    dat = ".dat"
    dir = ".dir"
    backup = "_backup"
    status = "_status"

    def __init__(self, server_name, register_path):
        self.register_path = register_path
        self.server_name = server_name
        self.register_value_map = None
        self.register_timestamp_map = None

        self.file_name = self.register_path + '\\' + self.server_name
        # TODO: Call validate function.
        self.load_register_2()

    def name(self, request, context):
        print('received name request, returning the name of server')
        return abd_pb2.NameResponse(name=str(self.server_name))

    def read1(self, request, context):
        print("{} : got read1 of : {}".format(self.server_name, request))
        register_name = request.register.lower()
        # These values will be returned when, the requested register is not present
        # in the server.
        value = ''
        timestamp = 0
        if register_name in self.register_value_map:
            value = self.register_value_map[register_name]
            timestamp = self.register_timestamp_map[register_name]
        read1_response = abd_pb2.Read1Response(value=value, timestamp=timestamp)
        print("{} : read1 request processed".format(self.server_name))
        return read1_response

    def read2(self, request, context):
        print("got : read2 of  {}".format(request))
        self.common_write(request.register.lower(), request.timestamp, request.value)
        print("{} : read 2 request completed..".format(self.server_name))
        read2_ack = abd_pb2.AckResponse()
        return read2_ack

    def write(self, request, context):
        print("got : write of {}".format(request))
        self.common_write(request.register.lower(), request.timestampe, request.value)
        print("{} : write request completed..".format(self.server_name))
        return abd_pb2.AckResponse()

    # A common function used by both read2 and write() functions
    def common_write(self, register_name, timestamp, value):

        print('{} : common_write : register_name : {} : value : {} : timestamp : {}'.format(self.server_name,
                                                                                            register_name,
                                                                                            value,
                                                                                            timestamp
                                                                                            ))
        if register_name in self.register_value_map:
            print('{} : common_write : register {} present'.format(self.server_name, register_name))
            current_timestamp = self.register_timestamp_map[register_name]
            if timestamp > current_timestamp:
                print('{} : common_write : request timestamp {} > current timestamp {} , so updating'
                      'the register'.format(self.server_name, timestamp, current_timestamp))
                self.register_timestamp_map[register_name] = timestamp
                self.register_value_map[register_name] = value
            else:
                print('{} : common_write : request timestamp {} <= current timestamp {} , so ignoring the request'
                  'the register'.format(self.server_name, timestamp, current_timestamp))
                return
        else:
            print('{} : common_write : register {} not present so adding it.'.format(self.server_name, register_name))
            self.register_value_map[register_name] = value
            self.register_timestamp_map[register_name] = timestamp
        self.backup_and_write_2()


    """def load_register(self):
        file_name = self.register_path+self.server_name
        if os.path.exists(file_name):
            self.file_handle = open(file_name, 'r')
            self.initialize_variables()
        elif os.path.exists(file_name+"_backup"):
            backup_file = file_name+"_backup"
            self.file_handle = open(backup_file, 'r')
            self.initialize_variables()
            self.file_handle.close()
            shutil.copyfile(backup_file, file_name)
            os.remove(backup_file)
        else:
            self.value = "empty"
            self.time_stamp = c_ulonglong(0)
            print('print')
            f = open(file_name, 'w')
            f.close()

    def initialize_variables(self):
        separator = "="
        for line in self.file_handle:
            if separator in line:
                name, value = line.split(separator, 1)
                if name == self.REGISTER_NAME:
                    self.register_name = value
                elif name == self.VALUE:
                    self.value = value
                else:
                    self.time_stamp = c_ulonglong(value)
        self.file_handle.close()"""

    """
     load_register_2 is called during startup of the server, to load the current values of registers in the shelved
     files to the memory.
     This method takes care of recovery scenarios where a server might have failed in various stages 
     of execution
    """
    def load_register_2(self):
        print('begin load_register')
        # file_name = self.register_path + self.server_name
        with shelve.open(self.file_name+self.status, 'c') as status:
            if 'status' in status:
                status_val = status['status']
                print('{}: load_register_2 : status_val {}'.format(self.server_name, status_val))
                if status_val == 1 or status_val == 4 or status_val == 5:
                    # load from original
                    print('{}: load_register_2 : status_val {} : load from original'.format(self.server_name, status_val))
                    with shelve.open(self.file_name, 'c') as shelved:
                        self.register_value_map = shelved['register_value_map']
                        self.register_timestamp_map =  shelved['register_timestamp_map']
                    # delete backup if exists
                    print('{}: load_register_2 : status_val {} : delete backup if exists'.format(self.server_name, status_val))
                    self.delete_files(True, self.file_name)

                elif status_val == 6:
                    # just load from original
                    print('{}: load_register_2 : status_val : just load from original'.format(self.server_name,
                                                                                              status_val))
                    with shelve.open(self.file_name, 'c') as shelved:
                        self.register_value_map = shelved['register_value_map']
                        self.register_timestamp_map = shelved['register_timestamp_map']
                elif status_val == 2 or status_val == 3:
                    # load from backup
                    print('{}: load_register_2 : status_val {} : load from backup'.format(self.server_name,
                                                                                              status_val))
                    with shelve.open(self.file_name+self.backup, 'c') as backed_up:
                        self.register_value_map = backed_up['register_value_map']
                        self.register_timestamp_map = backed_up['register_timestamp_map']
                    # delete original if exists
                    print('{}: load_register_2 : status_val {} : delete original if exists'.format(self.server_name,
                                                                                       status_val))
                    self.delete_files(False, self.file_name)
                    # copy backup as original and delete backup.
                    print('{}: load_register_2 : status_val {} : copy backup as original and delete backup.'.format(self.server_name,
                                                                                                status_val))
                    shutil.copyfile(self.file_name + "_backup.dat", self.file_name + ".dat")
                    shutil.copyfile(self.file_name + "_backup.bak", self.file_name + ".bak")
                    shutil.copyfile(self.file_name + "_backup.dir", self.file_name + ".dir")
                    self.delete_files(True, self.file_name)
                else: # status will be 0, do basic init
                    print('{}: load_register_2 : status_val : status will be 0, do basic init'.format(
                        self.server_name,
                        status_val))
                    self.register_value_map = {}
                    self.register_timestamp_map = {}
                    with shelve.open(self.file_name, 'c') as shelved:
                        shelved['register_value_map'] = self.register_value_map
                        shelved['register_timestamp_map'] = self.register_timestamp_map
            else:
                # initial status
                print('{}: load_register_2 : initial status , do basic init'.format(
                    self.server_name))
                status['status'] = 0
                self.register_value_map = {}
                self.register_timestamp_map = {}
                with shelve.open(self.file_name, 'c') as shelved:
                    shelved['register_value_map'] = self.register_value_map
                    shelved['register_timestamp_map'] = self.register_timestamp_map
        print('end load register')

    def backup_and_write_2(self):

        """
        The code flow goes like this,
          when a write request is received,
            a. copy the current shelved file set to _backup extensions
            b. update in memory copy of the two maps name_value and name_timestamp
            c. place the updated copies of registers in the original set of shelved files.
            d. close the shelved files.
            e. deleted the backup files.
        #TODO Instead of writing status to the same shelf file, maintain a separate status shelf file..that just holds the following status
         1. backup_begin,
         2. backup_completed
         3. shelving_begin
         4.shelving_completed
         5.backup_delete_begin
         6. backup_delete_completed
        """
        print("{} : backup_and_write begin".format(self.server_name))
        #TODO: Make a check if this is the first write...then backup not required.
        #file_name = self.register_path + self.server_name
        with shelve.open(self.file_name+self.status, 'c') as status:
            status['status'] = 1
        # 1. copy the current shelved file set to _backup extensions
        print("{} : backup begin".format(self.server_name))
        shutil.copyfile(self.file_name+".dat", self.file_name+"_backup.dat")
        shutil.copyfile(self.file_name+".bak", self.file_name+"_backup.bak")
        shutil.copyfile(self.file_name+".dir", self.file_name+"_backup.dir")
        with shelve.open(self.file_name + self.backup, 'w') as backed_up:
            backed_up['status'] = 'backup successful'
        with shelve.open(self.file_name+self.status, 'c') as status:
            status['status'] = 2
        print("{} : backup complete".format(self.server_name))

        # 2. update in memory copy of the two maps name_value and name_timestamp - already completed before
        # the backup_and_write call.

        # 3. place the updated copies of registers in the original set of shelved files.
        print("{} : shelving begin".format(self.server_name))
        with shelve.open(self.file_name+self.status, 'c') as status:
            status['status'] = 3
        with shelve.open(self.file_name, 'c') as shelved:
            shelved['status'] = 'shelving begin'
            shelved['register_timestamp_map'] = self.register_timestamp_map
            shelved['register_value_map'] = self.register_value_map
            shelved['status'] = 'shelving successful'
        with shelve.open(self.file_name+self.status, 'c') as status:
            status['status'] = 4
        print("{} : shelving complete".format(self.server_name))

        print("{} : Delete backup begin".format(self.server_name))
        with shelve.open(self.file_name+self.status, 'c') as status:
            status['status'] = 5
        os.remove(self.file_name + "_backup.dat")
        os.remove(self.file_name + "_backup.bak")
        os.remove(self.file_name + "_backup.dir")
        with shelve.open(self.file_name+self.status, 'c') as status:
            status['status'] = 6
        print("{} : Delete backup complete".format(self.server_name))
        print("{} : backup_and_write complete".format(self.server_name))

    def delete_files(self, is_backup, file_name):
        if is_backup:
            if os.path.exists(file_name + "_backup.dat"):
                os.remove(file_name + "_backup.dat")
            if os.path.exists(file_name + "_backup.bak"):
                os.remove(file_name + "_backup.bak")
            if os.path.exists(file_name + "_backup.dir"):
                os.remove(file_name + "_backup.dir")
        else:
            if os.path.exists(file_name + ".dat"):
                os.remove(file_name + ".dat")
            if os.path.exists(file_name + ".bak"):
                os.remove(file_name + ".bak")
            if os.path.exists(file_name + ".dir"):
                os.remove(file_name + ".dir")
"""     
# TODO:

"""