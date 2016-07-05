import os
import sys
import time
import sqlite3
import logging
import __main__
import subprocess
import ConfigParser

def insert_to_db(data):
    db_file_path = '/var/lib/env_monitoring.sqlite'
    conn = sqlite3.connect(db_file_path, timeout=5)
    c = conn.cursor()
    try:
      new_data = (int(time.time()), 'lora', gw_serial(), data['payload'], 0, data['rssi'], data['node_id'])
      if __main__.args.verbose:
          print new_data
      c.execute("INSERT INTO data (timestamp, source, gateway_serial, payload, state, rssi, node_id) VALUES (?,?,?,?,?,?,?)", (new_data))
      conn.commit()
      conn.close()
    except Exception as e:
      conn.close()
      raise
    return True

def gw_serial():
    serial = os.popen('cat /proc/cpuinfo | grep Serial | awk \'{ print $NF }\'').read().rstrip()
    if not serial:
        serial = 'unknown'
    return serial
