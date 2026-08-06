#!/usr/bin/env python3
# Taras Hanis 2026
# topoctrlFwdScaleProf (dual-stack / high-lpm) を収集し PostgreSQL に格納する

import os
import re
import random
import subprocess
from datetime import datetime

import requests
import psycopg2

fabric_name = {
    'kynoym3': 'OYM', 'kyntam5': 'TAM', 'kynchy1': 'CHY', 'kynosc1': 'OSK',
    'kynhkt1': 'HKT', 'kynngy1': 'NGY', 'kynsen2': 'SEN'
}

station_name = {
    'kynoym3': 'oym3', 'kyntam5': 'tam5', 'kynchy1': 'chy1', 'kynosc1': 'osc1',
    'kynhkt1': 'hkt1', 'kynngy1': 'ngy1', 'kynsen2': 'sen2'
}

# 収集対象の profType。全件入れたい場合は None にする
TARGET_PROF_TYPES = {'dual-stack', 'high-lpm'}

now = datetime.now()
log_date = now.strftime("%Y-%m-%d")

username = 'admin-auto'
password = os.environ.get('APIC_PASSWORD')
db_password = os.environ.get('DB_PASSWORD')

tool_path = "/home/kddi/scripts/fwd_scale_prof/"


apic_hostnames = subprocess.check_output(
    f"cat {tool_path}apics.txt | grep -i sdn0001 | awk '{{print $2}}' | awk -F'-' '{{print $1}}'",
    shell=True
)
apic_hostname_parts = apic_hostnames.decode('utf-8').split('\n')[:-1]
print(apic_hostname_parts)


def check_connection(apic_ips):
    successful_apic_ips = []
    for ip in apic_ips:
        try:
            url = f"http://{ip}"
            response = subprocess.call(['ping', '-c', '1', ip], stdout=subprocess.DEVNULL)
            http_response = requests.get(url)
            if response == 0 and http_response.status_code == 200:
                successful_apic_ips.append(ip)
        except Exception as e:
            print(f'Unable to ping {ip}. Exception: {e}')
    return successful_apic_ips


def apic_auth(apic_ip, username, password):
    base_url = 'http://' + apic_ip + '/api/'
    auth_endpoint = 'aaaLogin.json'
    session = requests.Session()
    session.verify = False
    auth_info = {"aaaUser": {"attributes": {"name": username, "pwd": password}}}
    auth_response = session.post(base_url + auth_endpoint, json=auth_info,
                                 proxies={"http": None, "https": None})
    if auth_response.status_code == 200:
        return auth_response.json()["imdata"][0]["aaaLogin"]["attributes"]["token"]
    return None


def node_id_from_dn(dn):
    """topology/pod-1/node-101/sys/topoctrl/fwdprofile -> '101'"""
    m = re.search(r'/node-(\d+)/', dn)
    return m.group(1) if m else None


try:
    timescale_connection = psycopg2.connect(
        host="localhost",
        user="grafana",
        password=db_password,
        database="kyanos_db"
    )

    cursor = timescale_connection.cursor()

    insert_into_db = (
        'INSERT INTO t_fwd_scale_prof '
        '(time, area_network, station, hostname, nodeid, prof_type) '
        'VALUES (%s, %s, %s, %s, %s, %s);'
    )

    for apic in apic_hostname_parts:
        print(apic)
        apics = subprocess.check_output(
            f"cat {tool_path}apics.txt | grep -i sdn | grep {apic}- | awk '{{print $1}}'",
            shell=True
        )
        apic_ips = apics.decode('utf-8').split('\n')[:-1]

        successful_apic_ips = check_connection(apic_ips)
        if not successful_apic_ips:
            print(f"[SKIP] {apic}: reachable APIC not found")
            continue
        random_apic_ip = random.choice(successful_apic_ips)

        token = apic_auth(random_apic_ip, username, password)
        if not token:
            print(f"[SKIP] {apic}: auth failed")
            continue

        session = requests.Session()
        session.verify = False
        session.headers.update({'Cookie': 'APIC-Cookie=' + token})

        # --- 1. node_id -> hostname の辞書を作る -------------------------
        fabricNode_url = 'http://' + random_apic_ip + '/api/class/fabricNode.json'
        node_response = session.get(fabricNode_url, proxies={"http": None, "https": None})
        if node_response.status_code != 200:
            print(f"[SKIP] {apic}: fabricNode {node_response.status_code}")
            continue

        nodes = node_response.json()['imdata']
        nodes_dict = {
            n["fabricNode"]["attributes"]['id']: n["fabricNode"]["attributes"]['name']
            for n in nodes
        }

        # --- 2. Fwd Scale Profile を取得して格納 -------------------------
        prof_url = 'http://' + random_apic_ip + '/api/class/topoctrlFwdScaleProf.json'
        prof_response = session.get(prof_url, proxies={"http": None, "https": None})
        if prof_response.status_code != 200:
            print(f"[SKIP] {apic}: topoctrlFwdScaleProf {prof_response.status_code}")
            continue

        for item in prof_response.json()['imdata']:
            try:
                attrs = item["topoctrlFwdScaleProf"]["attributes"]
                prof_type = attrs["profType"]

                if TARGET_PROF_TYPES and prof_type not in TARGET_PROF_TYPES:
                    continue

                node_id = node_id_from_dn(attrs["dn"])
                if node_id is None or node_id not in nodes_dict:
                    print(f"[SKIP] unknown node in dn: {attrs['dn']}")
                    continue

                hostname = nodes_dict[node_id]
                hostname_prefix = hostname.split("-")[0]
                area_network = fabric_name.get(hostname_prefix)
                station = station_name.get(hostname_prefix)

                print(f"{log_date},{area_network},{station},{hostname},{node_id},{prof_type}")
                cursor.execute(insert_into_db,
                               (log_date, area_network, station, hostname, node_id, prof_type))
                timescale_connection.commit()

            except psycopg2.Error as e:
                print(f"[DB ERROR] {apic}: {e}")
                timescale_connection.rollback()
                continue
            except Exception as e:
                print(f"[ERROR] {apic}: {e}")
                continue

    cursor.close()
    timescale_connection.close()

except KeyboardInterrupt:
    print("Exiting script...")
    
    
    
"""
CREATE TABLE t_fwd_scale_prof (
    time         date,
    area_network text,
    station      text,
    hostname     text,
    nodeid       text,
    prof_type    text
);
"""