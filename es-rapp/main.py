#  ============LICENSE_START===============================================
#  Copyright (C) 2024 Intel, Rimedo Labs and Tietoevry. All rights reserved.
#  SPDX-License-Identifier: Apache-2.0
#  ========================================================================
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#  ============LICENSE_END=================================================
#  !/usr/bin/env python3

import numpy as np
import requests
import random
import logging
import time
import json
import os
from operator import itemgetter
from enum import Enum

def get_example_per_slice_policy(cell_id: str, qos: int, preference: str):
    ## hardcoded values used for SMaRT-5G demo
    return {
        "scope": {
            "sliceId": {
                "sst": 1,
                "sd": "000000",
                "plmnId": {
                    "mcc": "001",
                    "mnc": "01"
                }
            },
            "qosId": {
                "5qI": qos
            }
        },
        "tspResources": [
            {
                "cellIdList": [
                    {
                        "plmnId": {
                            "mcc": "001",
                            "mnc": "01"
                        },
                        "cId": {
                            "ncI": 268783936
                        }
                    }
                ],
                "preference": preference
            }
        ]
    }

log = logging.getLogger('main')

FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)  # , stream=sys.stdout
log.setLevel(logging.INFO)
log.info(f'rApp START')
allow_disabled = [True, False]

class States(Enum):
    ENABLED = 1
    DISABLED = 2
    DISABLING = 3

class Application:
    def __init__(self, sleep_time_sec: float, sleep_after_decision_sec: float, avg_slots: int):
        self.sleep_time_sec = sleep_time_sec
        self.sleep_after_decision_sec = sleep_after_decision_sec
        self.avg_slots = avg_slots

        self.cells = {}
        self.cell_urls = {}
        self.ready_time = time.time() + sleep_after_decision_sec
        self.source_name = ""
        self.meas_entity_dist_name = ""

        # self.prb_history = []
        # self.switch_off = False
        # self.index = 0
        # self.load_predictor = 'http://' + os.environ['LOAD_PREDICTOR'] + ':' + os.environ['LOAD_PREDICTOR_PORT'] + '/' + os.environ['LOAD_PREDICTOR_API']

        self.a1_url = 'http://' + os.environ['A1T_ADDRESS'] + ':' + os.environ['A1T_PORT']
        self.ransim_data_path = os.environ['RANSIM_DATA_PATH']

        self.sdn_controller_address = os.environ['SDN_CONTROLLER_ADDRESS']
        self.sdn_controller_port = os.environ['SDN_CONTROLLER_PORT']
        self.sdn_controller_auth = (os.environ['SDN_CONTROLLER_USERNAME'], os.environ['SDN_CONTROLLER_PASSWORD'])
        
    def work(self):
        # policies = self.get_policies()
        # if policies is None:
        #     log.error('Unable to connect to A1.')
        #     return


        # # legacy judge, need?
        # # for policy in policies:
        # #     # assumed that all policies with ID >= 1000 are from this rApp
        # #     if int(policy) >= 1000:
        # self.delete_policy()

        payload = self.fetch_cell_urls()
        if payload is None:
            log.error('Unable to fetch cell URLs')
        #    return

        while True:
            time.sleep(self.sleep_time_sec)

            data = self.read_data()
            if not data:
                log.info('No data')
                continue
            
            log.info('update_local_data')
            self.update_local_data(data)

            if time.time() >= self.ready_time:
                self.ready_time = time.time() + self.sleep_after_decision_sec
                if self.cells:
                    self.make_decision()

    def read_data(self):
        data = None
        try:
            reports_list = os.listdir(self.ransim_data_path)
            reports_list_fullpath = ["{}/{}".format(self.ransim_data_path, report) for report in reports_list]
            oldest_report = min(reports_list_fullpath, key=os.path.getmtime)
            log.debug(f'Opening report file: {oldest_report}')
            with open(oldest_report) as file:
                data = json.load(file)
            os.remove(oldest_report)
        except Exception as ex:
            pass
        return data

    def update_local_data(self, data):
        report_type = data['event']['perf3gppFields']['measDataCollection']['measuredEntityDn']
        self.source_name = data['event']['commonEventHeader']['sourceName']
        self.meas_entity_dist_name = report_type

        cells = data['event']['perf3gppFields']['measDataCollection']['measInfoList'][0]['measValuesList']

        if not cells:
            log.warning("PM report is lacking measurements")
            return

        for index, cell in enumerate(cells):
            cId = str(cell['measObjInstId'].partition("NRCellDU=")[-1])

            sValue = cell['measResults'][0]['sValue']

            if cId not in self.cells:
                self.cells[cId] = {
                    "id": cId,
                    "allow_disabled": allow_disabled[index],
                    "state": States.ENABLED if float(cell['measResults'][0]['sValue']) > 0.0 else States.DISABLED,
                    "prb_usage": np.nan * np.zeros((self.avg_slots, )),
                    "avg_prb_usage": np.nan,
                    "policy_list": []
                }
                if self.cells[cId]['state'] == States.ENABLED:
                    self.toggle_cell_administrative_state(cId, locked=False)

            store = self.cells[cId]
            store['prb_usage'] = np.roll(store['prb_usage'], 1)
            store['prb_usage'][0] = float(sValue)

            if not np.isnan(store['prb_usage']).any():
                store['avg_prb_usage'] = np.mean(store['prb_usage'])

        status = "PRB usage: ["
        for key in self.cells.keys():
            cell = self.cells[key]
            status += f'{cell["id"]}: {cell["avg_prb_usage"]:.3f}, '
        status = status[:-2] + "] avg: "
        try:
            avg_prb = sum(self.cells[cell]["avg_prb_usage"] for cell in self.cells) / sum((self.cells[cell]['state'] != States.DISABLED) for cell in self.cells)
        except ZeroDivisionError:
            log.info("ZeroDivisionError: no prb usage")
        else:
            status += f'{avg_prb:.3f}'
            energy_all = (300 + 4 * 150) / 1e3
            energy = (300 + (sum((self.cells[cell]['state'] != States.DISABLED) for cell in self.cells) - 1) * 150) / 1e3
            energy_per_day = 24 * energy
            energy_save = (energy_all - energy) * 24
            status += f' (energy consumption: {energy:.2f}/{energy_all:.2f} W; per day: {energy_per_day:.2f} Wh; per day savings: {energy_save:.2f} Wh)'
            log.info(status)

    def make_decision(self) :
        data = []
        for i, key in enumerate(self.cells.keys()):
            record = [None] * 3
            record[0] = self.cells[key]['id']
            record[1] = self.cells[key]['avg_prb_usage']
            record[2] = self.cells[key]['state']
            data.append(record)

        if any(np.nan in record for record in data):
            return

        total_prb_usage = sum(record[1] for record in data)
        enabled_cells = sum((record[2] != States.DISABLED) for record in data)
        disabling_cell = sum((record[2] == States.DISABLING) for record in data)

        log.info(f'Checking if a cell should be switched off')
        for record in data:
            if record[2] == States.DISABLING:
                cell_id = record[0]
                self.toggle_cell_administrative_state(cell_id, locked=True)
                self.cells[cell_id]['state'] = States.DISABLED
                enabled_cells-=1

        current_avg_prb_usage = total_prb_usage / enabled_cells

        log.info(f'Making decision...')
        log.info(f'Current average PRB usage: {current_avg_prb_usage}')
        if (max(record[1] for record in data) > 40) and (current_avg_prb_usage > 30):
            log.info('Max and average PRB usage above thresholds - trying to enable one cell.')
            self.enable_one_cell()
        elif (current_avg_prb_usage < 20) and (disabling_cell == 0):
            log.info('Average PRB usage below threshold - trying to disable one cell.')
            future_avg_prb_usage = total_prb_usage / (enabled_cells - 1)
            log.info(f'Expected PRB usage {future_avg_prb_usage}.')
            self.disable_one_cell()
        else:
            log.info('Make cell on/off decision - no action - balance achieved.')

    def toggle_cell_administrative_state(self, cell_id, locked):
        # cell_id is 9 hex digits
        string_cell_id = '{}'.format(cell_id)
        while len(string_cell_id) < 9:
            string_cell_id = '0' + string_cell_id
        full_cell_id = '138426{}'.format(string_cell_id)
        # full_cell_id = '{}'.format(cell_id)
        sOff='off' if locked else 'on'
        log.info(f'Switching {sOff} cell {full_cell_id}')
        path_base = '/rests/data/network-topology:network-topology/topology=topology-netconf'
        path_tail = '/node={node}/yang-ext:mount/o-ran-sc-du-hello-world:network-function/distributed-unit-functions={duf}/cell={cell}/administrative-state'
        url = 'http://' + self.sdn_controller_address + ':' + self.sdn_controller_port + path_base + path_tail.format(node=self.source_name, duf=self.meas_entity_dist_name, cell=full_cell_id)
        log.info(f'{url}')
        headers = {
            'Accept': 'application/yang-data+json',
            'Content-Type': 'application/yang-data+json'
        }
        payload = {"administrative-state": "locked" if locked else "unlocked"}
        response = requests.put(url, verify=False, auth=self.sdn_controller_auth, json=payload, headers=headers)
        log.info(f'Cell-{sOff} response status:{response.status_code}')

    def enable_one_cell(self):
        options = []
        for key in self.cells.keys():
            option = [None] * 3
            option[0] = self.cells[key]['id']
            option[1] = self.cells[key]['avg_prb_usage']
            option[2] = (self.cells[key]['state'] == States.DISABLED) and self.cells[key]['allow_disabled']
            options.append(option)

        options_filtered = [option for option in options if option[2] == True]
        if len(options_filtered) == 0:
            log.info('There are no cells that could be enabled...')
            return

        cell_id = random.choice(options)[0]
        self.cells[cell_id]['state'] = States.ENABLED
        log.info(f'enable cell with cell_id {cell_id}')
        self.toggle_cell_administrative_state(cell_id, locked=False)
        # self.send_command_enable_cell(cell_id)

    def send_command_enable_cell(self, cell_id):
        log.info(f'Enabling cell with id {cell_id}')
        for policy in self.cells[cell_id]['policy_list']:
            self.delete_policy(str(policy))
        self.cells[cell_id]['policy_list'] = []

    def disable_one_cell(self):
        options = []
        for key in self.cells.keys():
            option = [None] * 3
            option[0] = self.cells[key]['id']
            option[1] = self.cells[key]['avg_prb_usage']
            option[2] = (self.cells[key]['state'] == States.ENABLED) and self.cells[key]['allow_disabled']
            options.append(option)

        options_filtered = [option for option in options if option[2] == True]
        if len(options_filtered) == 0:
            log.info('There are no cells that could be disabled...')
            return

        ind = sorted(options_filtered, key=itemgetter(1))[0]
        cell_id = ind[0]
        self.cells[cell_id]['state'] = States.DISABLING
        log.info(f'disable cell with cell_id {cell_id}')
        # self.send_command_disable_cell(cell_id)

    def send_command_disable_cell(self, cell_id):
        log.info(f'Disabling cell with id {cell_id}')
        current_policies = self.get_policies()

        index = 1000
        while str(index) in current_policies:
            index += 1

        # put new policy with AVOID based on scope
        response = requests.put(self.a1_url +
                                '/A1-P/v2/policytypes/ORAN_TrafficSteeringPreference_2.0.0/policies/'
                                + str(index), params=dict(notification_destination='test'),
                                json=get_example_per_slice_policy(cell_id, qos=1, preference='FORBID'))
        log.info(f'Sending policy (id={index}) for cell with id {cell_id} (FORBID): status_code: {response.status_code}')
        self.cells[cell_id]['policy_list'].append(index)

        index += 1
        while str(index) in current_policies:
            index += 1
        response = requests.put(self.a1_url +
                                '/A1-P/v2/policytypes/ORAN_TrafficSteeringPreference_2.0.0/policies/'
                                + str(index), params=dict(notification_destination='test'),
                                json=get_example_per_slice_policy(cell_id, qos=2, preference='FORBID'))
        log.info(f'Sending policy (id={index}) for cell with id {cell_id} (FORBID): status_code: {response.status_code}')
        self.cells[cell_id]['policy_list'].append(index)

    def delete_policy(self):
        log.info(f'Deleting policy with id: {policy_id}')
        try:
            requests.delete(self.a1_url +
                            '/A1-P/v2/policytypes/ORAN_TrafficSteeringPreference_2.0.0/policies/1000')
            requests.delete(self.a1_url +
                            '/A1-P/v2/policytypes/ORAN_TrafficSteeringPreference_2.0.0/policies/1001')
        except Exception as ex:
            log.error(ex)

    def get_policies(self):
        try:
            response = requests.get(self.a1_url +
                                    '/A1-P/v2/policytypes/ORAN_TrafficSteeringPreference_2.0.0/policies').json()
            return response
        except Exception as ex:
            log.error(ex)
            return None

    def fetch_cell_urls(self):
        path_base = '/rests/data/network-topology:network-topology/topology=topology-netconf'
        url = 'http://' + self.sdn_controller_address + ':' + self.sdn_controller_port + path_base
        headers = {
            'Accept': 'application/yang-data+json',
            'Content-Type': 'application/yang-data+json'
        }

        try:
            response = requests.get(url, auth=self.sdn_controller_auth, headers=headers)
            log.info(response)
            return response
        except Exception as ex:
            log.error(ex)
            return None

if __name__ == '__main__':
    app = Application(
        sleep_time_sec=1.0,
        sleep_after_decision_sec=10.0,
        avg_slots=10
    )
    app.work()
