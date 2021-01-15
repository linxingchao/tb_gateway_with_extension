#from build.lib.thingsboard_gateway.connectors import connector
import logging
import time
import threading
from random import choice
from string import ascii_lowercase

import snap7


from thingsboard_gateway.tb_utility.tb_utility import TBUtility
# Try import Pymodbus library or install it and import
try:
    from snap7 import client
except ImportError:
    print("snap7 library not found - installing...")
    TBUtility.install_package("python-snap7")
    #TBUtility.install_package('pyserial')
    from snap7 import client

from snap7 import snap7types
from snap7.snap7types import S7AreaPA, S7AreaTM, S7CpuInfo, S7WLDWord
import struct
'''
#from pymodbus.client.sync import ModbusTcpClient, ModbusUdpClient, ModbusSerialClient, ModbusRtuFramer, ModbusSocketFramer
#from pymodbus.bit_write_message import WriteSingleCoilResponse, WriteMultipleCoilsResponse
#from pymodbus.register_write_message import WriteMultipleRegistersResponse, \
                                            WriteSingleRegisterResponse
from pymodbus.register_read_message import ReadRegistersResponseBase
from pymodbus.bit_read_message import ReadBitsResponseBase
from pymodbus.exceptions import ConnectionException
'''
from thingsboard_gateway.connectors.connector import Connector, log

from thingsboard_gateway.connectors.s7.bytes_s7_uplink_converter import BytesS7UplinkConverter
from thingsboard_gateway.connectors.s7.bytes_s7_downlink_converter import BytesS7DownlinkConverter


class S7Connector(Connector,threading.Thread):
    def __init__(self, gateway, config, connector_type):
        
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        super().__init__()
        self.__gateway = gateway
        self._connector_type = connector_type
        self.__config = config.get("server")
        self.__current_master, self.__host,self.__port,self.__rack,self.__slot,self.__available_functions = self.__configure_master()
        self.__default_config_parameters = ['host', 'port', 'timeout', 'strict', 'type','method','rack','slot']
        self.__byte_order = self.__config.get("byteOrder")
        self.__word_order = self.__config.get("wordOrder")
        # self.__rack = self.__config.get("rack")
        # self.__slot = self.__config.get("slot")
        # self.__host = self.__config.get("host")
        # self.__port = self.__config.get("port")
        # self.__client = snap7.client.Client()
        self.__configure_master()
        self.__devices = {}
        self.setName(self.__config.get("name",
                                       'S7 Default ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.__load_converters()
        self.__connected = False
        self.__stopped = False
        self.daemon = True

    def is_connected(self):
        return self.__connected

    def open(self):
        self.__stopped = False
        self.start()
        log.info("Starting S7 connector")

    def run(self):
        self.__connect_to_current_master()
        self.__connected = True

        while True:
            time.sleep(.01)
            self.__process_devices()
            if self.__stopped:
                break

    def __load_converters(self):
        try:
            for device in self.__config["devices"]:
                if self.__config.get("converter") is not None:
                    converter = TBUtility.check_and_import(self._connector_type, self.__config["converter"])(device)
                else:
                    converter = BytesS7UplinkConverter(device)
                if self.__config.get("downlink_converter") is not None:
                    downlink_converter = TBUtility.check_and_import(self._connector_type, self.__config["downlink_converter"])(device)
                else:
                    downlink_converter = BytesS7DownlinkConverter(device)
                if device.get('deviceName') not in self.__gateway.get_devices():
                    self.__gateway.add_device(device.get('deviceName'), {"connector": self}, device_type=device.get("deviceType"))
                self.__devices[device["deviceName"]] = {"config": device,
                                                        "converter": converter,
                                                        "downlink_converter": downlink_converter,
                                                        "next_attributes_check": 0,
                                                        "next_timeseries_check": 0,
                                                        "telemetry": {},
                                                        "attributes": {},
                                                        "last_telemetry": {},
                                                        "last_attributes": {}
                                                        }
        except Exception as e:
            log.exception(e)

    def close(self):
        self.__stopped = True
        self.__stop_connections_to_masters()
        log.info('%s has been stopped.', self.get_name())

    def get_name(self):
        return self.name

    def __process_devices(self):
        #log.info(self.__devices)
        for device in self.__devices:
            current_time = time.time()
            device_responses = {"timeseries": {},
                                "attributes": {},
                                }
            #log.info(device)
            to_send = {}
            try:
                for config_data in device_responses: 
                    if device not in ('slot','rack','port') and self.__devices[device]["config"].get(config_data) is not None:
                        unit_id = self.__devices[device]["config"]["unitId"]
                        
                        if self.__devices[device]["next_"+config_data+"_check"] < current_time:
                            #log.info("device:" % device)
                            #log.info("***********************************************")
                            self.__connect_to_current_master(device)
                            #  Reading data from device
                            for interested_data in range(len(self.__devices[device]["config"][config_data])):
                                current_data = self.__devices[device]["config"][config_data][interested_data]
                                current_data["deviceName"] = device
                                #log.info("%s,%s,%s" % (device,config_data,interested_data))
                                #log.info("current_data:" % current_data)
                                input_data = self.__registerType_to_device(current_data, unit_id)
                                # if not isinstance(input_data, ReadRegistersResponseBase) and input_data.isError():
                                #     log.exception(input_data)
                                #     continue
                                device_responses[config_data][current_data["tag"]] = {"data_sent": current_data,
                                                                                    "input_data": input_data}

                            log.debug("Checking %s for device %s", config_data, device)
                            self.__devices[device]["next_"+config_data+"_check"] = current_time + self.__devices[device]["config"][config_data+"PollPeriod"]/1000
                            log.debug(device_responses)
                            converted_data = {}
                            try:
                                converted_data = self.__devices[device]["converter"].convert(config={**self.__devices[device]["config"],
                                                                                                    "byteOrder": self.__devices[device]["config"].get("byteOrder", self.__byte_order),
                                                                                                    "wordOrder": self.__devices[device]["config"].get("wordOrder", self.__word_order)},
                                                                                            data=device_responses)
                            except Exception as e:
                                log.error(e)

                            if converted_data and self.__devices[device]["config"].get("sendDataOnlyOnChange"):
                                self.statistics['MessagesReceived'] += 1
                                to_send = {"deviceName": converted_data["deviceName"], "deviceType": converted_data["deviceType"]}
                                if to_send.get("telemetry") is None:
                                    to_send["telemetry"] = []
                                if to_send.get("attributes") is None:
                                    to_send["attributes"] = []
                                for telemetry_dict in converted_data["telemetry"]:
                                    for key, value in telemetry_dict.items():
                                        if self.__devices[device]["last_telemetry"].get(key) is None or \
                                        self.__devices[device]["last_telemetry"][key] != value:
                                            self.__devices[device]["last_telemetry"][key] = value
                                            to_send["telemetry"].append({key: value})
                                for attribute_dict in converted_data["attributes"]:
                                    for key, value in attribute_dict.items():
                                        if self.__devices[device]["last_attributes"].get(key) is None or \
                                        self.__devices[device]["last_attributes"][key] != value:
                                            self.__devices[device]["last_attributes"][key] = value
                                            to_send["attributes"].append({key: value})
                                        # to_send["telemetry"] = converted_data["telemetry"]
                                # if converted_data["attributes"] != self.__devices[device]["attributes"]:
                                    # self.__devices[device]["last_attributes"] = converted_data["attributes"]
                                    # to_send["attributes"] = converted_data["attributes"]
                                if not to_send.get("attributes") and not to_send.get("telemetry"):
                                    # self.__gateway.send_to_storage(self.get_name(), to_send)
                                    # self.statistics['MessagesSent'] += 1
                                    log.debug("Data has not been changed.")
                            elif converted_data and self.__devices[device]["config"].get("sendDataOnlyOnChange") is None or not self.__devices[device]["config"].get("sendDataOnlyOnChange"):
                                self.statistics['MessagesReceived'] += 1
                                to_send = {"deviceName": converted_data["deviceName"],
                                        "deviceType": converted_data["deviceType"]}
                                # if converted_data["telemetry"] != self.__devices[device]["telemetry"]:
                                self.__devices[device]["last_telemetry"] = converted_data["telemetry"]
                                to_send["telemetry"] = converted_data["telemetry"]
                                # if converted_data["attributes"] != self.__devices[device]["attributes"]:
                                self.__devices[device]["last_attributes"] = converted_data["attributes"]
                                to_send["attributes"] = converted_data["attributes"]
                                # self.__gateway.send_to_storage(self.get_name(), to_send)
                                # self.statistics['MessagesSent'] += 1
                #log.info(to_send)
                # 数据添加到存储队列？
                if to_send.get("attributes") or to_send.get("telemetry"):
                    self.__gateway.send_to_storage(self.get_name(), to_send)
                    self.statistics['MessagesSent'] += 1
            except snap7.error as e:
                time.sleep(5)
                log.error("Connection lost! Reconnecting...")
            except Exception as e:
                log.exception(e)


    def on_attributes_update(self, content):
        pass

    def __connect_to_current_master(self, device=None):
        #log.info("wwwwwwwwwwwwwwwwwwwdevice:%s" % device)
        connect_attempt_count = 5
        connect_attempt_time_ms = 100
        if device is None:
            device = list(self.__devices.keys())[0]
            
        #log.info(self.__devices)
        #log(device)
        if self.__devices[device].get('master') is None:
            self.__devices[device]['master'],self.__devices[device]['host'],self.__devices['port'],self.__devices['rack'],self.__devices['slot'], self.__devices[device]['available_functions'] = self.__configure_master(
                self.__devices[device]["config"])
        if self.__devices[device]['master'] != self.__current_master:
            self.__current_master = self.__devices[device]['master']
            self.__available_functions = self.__devices[device]['available_functions']
        connect_attempt_count = self.__devices[device]["config"].get("connectAttemptCount", connect_attempt_count)
        connect_attempt_time_ms = self.__devices[device]["config"].get("connectAttemptTimeMs", connect_attempt_time_ms)
        attempt = 0
        if not self.__current_master.get_connected():
            while not self.__current_master.get_connected() and attempt < connect_attempt_count:
                attempt = attempt + 1
                self.__current_master.connect(self.__host,self.__rack,self.__slot,self.__port)
                if not self.__current_master.get_connected():
                    time.sleep(connect_attempt_time_ms / 1000)
                log.debug("S7 trying connect to %s", device)
        if attempt > 0 and self.__current_master.get_connected():
            log.debug("S7 connected.")

    #加载plc的配置文件
    def __configure_master(self, config=None):
        current_config = self.__config if config is None else config
        #log.info(current_config)
        host = current_config['host'] if current_config.get("host") is not None else self.__config.get("host", "localhost")
        try:
            port = int(current_config['port']) if current_config.get("port") is not None else self.__config.get(int("port"), 102)
        except ValueError:
            port = current_config['port'] if current_config.get("port") is not None else self.__config.get("port", 102)
        
        try:
            rack = int(current_config['rack']) if current_config.get("rack") is not None else self.__config.get(int("rack"))
        except ValueError:
            rack = current_config['rack'] if current_config.get("rack") is not None else self.__config.get("rack", 0)

        try:
            slot = int(current_config['slot']) if current_config.get("slot") is not None else self.__config.get(int("slot"))
        except ValueError:
            slot = current_config['slot'] if current_config.get("slot") is not None else self.__config.get("slot", 0)

        timeout = current_config['timeout'] if current_config.get("timeout") is not None else self.__config.get("timeout", 35)
        method = current_config['method'] if current_config.get('method') is not None else self.__config.get('method', 'socket')
        
        
        if current_config.get('type') == 'tcp' or (current_config.get("type") is None and self.__config.get("type") == "tcp"):
            master = snap7.client.Client()

        
        else:
            raise Exception("Invalid s7 transport type.")

        available_functions = {
            'I': self.__read_I,
            'V': self.__read_V,
            'M': self.__read_M,
            'Q': self.__read_Q,
            'DB':self.__read_DB
        }
        #log.info("master:%s, host:%s, port:%s, rack:%s, slot:%s" % (master,host,port,rack,slot))
        return master, host,port,rack,slot,available_functions

    def __stop_connections_to_masters(self):
       for device in self.__devices:
            self.__devices[device]['master'].disconnect()

    def __registerType_to_device(self, config, unit_id):
        register_type = config.get('registerType')
        result = None
        
        #log.info(self.__current_master.get_connected())
        if register_type in ('I', 'V', 'M','Q','DB'):
            
            if config.get('bitnumber') is not None:
                bitnumber = int(config.get('bitnumber'))
            else:
                bitnumber = None
            dataType = config.get('type')
            if register_type in ('DB'):
                dbnumber = int(config.get('dbnumber'))
                result = self.__available_functions[register_type](self.__current_master,dataType,dbnumber,int(config.get('address')),bitnumber)
            else:
                result = self.__available_functions[register_type](self.__current_master,dataType,int(config.get('address')),bitnumber)
            #log.info("#########:%s" % result)
        else:
            log.error("Unknown S7 registerType with code: %i", register_type)
        log.debug("With result %s", str(result))
        if "Exception" in str(result):
            log.exception(result)
            result = str(result)
        return result

    def server_side_rpc_handler(self, content):
        pass

    def __process_rpc_request(self, content, rpc_command_config):
        pass  


    def __read_I(self,client,dataType,offset,bitnumber=None):
        v_data = client.read_area(snap7types.S7AreaPE,0,offset,1)
        if(int(struct.unpack('!B',v_data)[0]) & pow(2,bitnumber) !=0):
            return 1
        else:
            return 0

    def __read_M(self,client,dataType,offset,bitnumber=None):
        if dataType == "bool":
            return self.__read_MB(client,offset,bitnumber)
        elif dataType == "int":
            return self.__read_MW(client,offset)
        elif dataType == "float":
            return self.__read_MD(client,offset)
        else:
            raise Exception('dataType Error:%' % dataType)
    
    def __read_V(self,client,dataType,offset,bitnumber=None):
        if dataType == "bool":
            return self.__read_VB(client,offset,bitnumber)
        elif dataType == "int":
            return self.__read_VW(client,offset)
        elif dataType == "float":
            return self.__read_VD(client,offset)
        else:
            raise Exception('dataType Error:%' % dataType)
    
    def __read_Q(self,client,dataType,offset,bitnumber=None):
        if dataType == "bool":
            return self.__read_QB(client,offset,bitnumber)
        elif dataType == "int":
            return self.__read_QW(client,offset)
        elif dataType == "float":
            return self.__read_QD(client,offset)
        else:
            raise Exception('dataType Error:%' % dataType)

    def __read_DB(self,client,dataType,dbnumber,offset,bitnumber=None):
        
        if dataType == "int":
            return self.__read_DB_Int(client,dbnumber,offset)
        elif dataType == "float":
            return self.__read_DB_Float(client,dbnumber,offset)
        elif dataType == "bool":
            pass
        else:
            raise Exception('dataType Error:%' % dataType)

    
    def __read_DB_Int(self,client,dbnumber,offset):
        v_data = client.read_area(snap7types.S7AreaDB,dbnumber,offset,2)
        data = int.from_bytes(v_data,byteorder='big',signed=False)
        return data

    def __read_DB_Float(self,client:snap7.client.Client,dbnumber,offset):
        v_data = client.read_area(snap7types.S7AreaDB,dbnumber,offset,4)
        data = struct.unpack("!f",v_data)[0]
        return data

    def __read_VD(self,client,offset):
        v_data = client.read_area(snap7types.areas.DB,1,offset,4)
        data = struct.unpack("!f",v_data)[0]
        return data

    def __read_VW(self,client:snap7.client.Client,offset):
        try:
            v_data = client.read_area(snap7types.areas.DB,1,offset,2)
            data = int.from_bytes(v_data,byteorder='big',signed=False)
            return data
            #logging.info('read_VW(%s) succeed' % )
        except Exception as e:
            time.sleep(0.003)
            #data = __read_VW(client,offset)
        

    def __read_VB(self,client:snap7.client.Client,offset:int,bitnumber:int):
        v_data = client.read_area(snap7types.areas.DB,1,offset,1)
        #data = int.from_bytes(v_data,byteorder='big',signed=False)
        data = v_data[0] & (1<<bitnumber)
        if data>0:
            return 1
        else:
            return 0

    def __read_VB(self,client:snap7.client.Client,offset:int):
        vb_data = client.db_read(1,offset,1)
        return vb_data[0]       

    def __read_MD(self,client:snap7.client.Client,offset):
        v_data = client.read_area(snap7types.S7AreaMK,0,offset,4)
        data = struct.unpack("!f",v_data)[0]
        return data

    def __read_MW(self,client:snap7.client.Client,offset):
        v_data = client.read_area(snap7types.S7AreaMK,0,offset,2)
        data = int.from_bytes(v_data,byteorder='big',signed=False)
        return data

    def __read_MB(self,client:snap7.client.Client,offset,bitnumber):
        v_data = client.read_area(snap7types.S7AreaMK,0,offset,1)
        if(int(struct.unpack('!B',v_data)[0]) & pow(2,bitnumber) !=0):
            return 1
        else:
            return 0

    def __read_IB(self,client:snap7.client.Client,offset,bitnumber):
        v_data = client.read_area(snap7types.S7AreaPE,0,offset,1)
        s = struct.unpack('!B',v_data)[0]
        if(int(struct.unpack('!B',v_data)[0]) & pow(2,bitnumber) !=0):
            return 1
        else:
            return 0


    def __read_QB(self,client:snap7.client.Client,offset,bitnumber):
        v_data = client.read_area(snap7types.S7AreaPA,0,offset,1)
        s = struct.unpack('!B',v_data)[0]
        if(int(struct.unpack('!B',v_data)[0]) & pow(2,bitnumber) !=0):
            return 1
        else:
            return 0

    def __read_QW(self,client:snap7.client.Client,offset:int):
        v_data = client.read_area(snap7types.S7AreaPA,1,offset,2)
        data = int.from_bytes(v_data,byteorder='big',signed=False)
        return data
    
    def __read_QD(self,client:snap7.client.Client,offset:int):
        v_data = client.read_area(snap7types.S7AreaPA,0,offset,4)
        data = struct.unpack("!f",v_data)[0]
        return data