# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
from deye_config import DeyeLoggerConfig
from deye_mqtt import DeyeMqttClient
from deye_modbus import DeyeModbus
from deye_sensor import Sensor
from deye_events import DeyeEventProcessor
from paho.mqtt.client import Client, MQTTMessage

class DeyeGridChargeControlEventProcessor(DeyeEventProcessor):
    def __init__(self, logger_config: DeyeLoggerConfig, mqtt_client: DeyeMqttClient, sensors: list[Sensor], modbus: DeyeModbus):
        self.__log = logger_config.logger_adapter(logging.getLogger(DeyeGridChargeControlEventProcessor.__name__))
        self.__logger_config = logger_config
        self.__mqtt_client = mqtt_client
        self.__modbus = modbus
        self.__topic_suffix = "grid_charge"
        matching_sensors = [s for s in sensors if s.mqtt_topic_suffix == self.__topic_suffix and "deye_sg01hp3_grid_charge" in s.groups]
        if len(matching_sensors) == 0:
            self.__log.error("Grid charge sensor not found. Enable appropriate metric group.")
            return
        elif len(matching_sensors) > 1:
            self.__log.error("Too many grid charge sensors found. Check your metric groups configuration.")
            return
        self.__sensor = matching_sensors[0]

    def get_id(self):
        return "grid_charge"

    def get_description(self):
        return "Grid charge over MQTT"

    def initialize(self):
        self.__mqtt_client.subscribe_command_handler(
            self.__logger_config.index, self.__topic_suffix, self.handle_command
        )

    def handle_command(self, client: Client, userdata, msg: MQTTMessage):
        try:
            value = float(msg.payload)
        except ValueError:
            self.__log.error("Invalid grid charge value: %s", msg.payload)
            return
        self.__log.info("Setting grid charge to %f", value)
        reg_addr, reg_value = self.__sensor.write_value(msg.payload).popitem()
        self.__modbus.write_register(reg_addr, reg_value)
