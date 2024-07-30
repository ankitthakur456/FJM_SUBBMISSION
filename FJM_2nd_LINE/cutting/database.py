import sqlite3
import time
import logging
import datetime
from conversions import get_hour, get_shift
import ast

log = logging.getLogger("LOGS")


class DBHelper:
    def __init__(self):
        self.connection = sqlite3.connect("HIS_JBM.db")
        self.cursor = self.connection.cursor()
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_data_table(ts INTEGER, payload STRING)""")  # sync_data_table
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS queue(id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL, serial_number STRING)""")
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS running_data(timestamp INTEGER, tube_length REAL,
            energy_used REAL, squareness REAL, serial_number STRING)""")
        self.cursor.execute(""" 
        CREATE TABLE IF NOT EXISTS sync_data_table2 (ts INTEGER, payload STRING)""")

    # region queue functions
    def enqueue_serial_number(self, serial_number):
        try:
            self.cursor.execute("""SELECT serial_number FROM queue where serial_number = ?""", (serial_number,))
            if self.cursor.fetchone() is None:
                self.cursor.execute("""INSERT INTO queue(serial_number, timestamp) VALUES(?,?)""",
                                    (serial_number, time.time()))
                self.connection.commit()
                log.info(f"[+] Successful, Serial Number Enqueued to the database")
            else:
                log.info(f"[-] Failed, Serial Number Already Enqueued to the database")
        except Exception as e:
            log.error(f"[-] Failed to enqueue serial number Error {e}")

    def get_first_serial_number(self):
        try:
            self.cursor.execute("""SELECT serial_number FROM queue ORDER BY timestamp ASC LIMIT 1""")
            serial_number = self.cursor.fetchone()
            if serial_number:
                return serial_number[0]
            else:
                return None
        except Exception as e:
            log.error(f"[-] Failed to get first serial number Error {e}")
            return None

    def purge_queue(self):
        try:
            self.cursor.execute("""DELETE FROM queue""")
            self.connection.commit()
            log.info(f"[+] Successful, Serial Number Deleted from the database")
        except Exception as e:
            log.error(f"[-] Failed to delete serial number Error {e}")

    # endregion

    def delete_Queue(self):
        try:
            # List all tables in the database
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = self.cursor.fetchall()

            # Delete all data from all tables
            for table in tables:
                self.cursor.execute(f"DELETE FROM {table[0]};")

            # Commit the changes and close the connection
            self.connection.commit()
        except Exception as e:
            log.error(f"[-] Failed to delete Table Error {e}")

    def delete_serial_number(self, serial_number):
        try:
            self.cursor.execute("""DELETE FROM queue where serial_number =?""", (serial_number,))
            self.connection.commit()
            log.info(f"[+] Successful, Serial Number Deleted from the database")
        except Exception as e:
            log.error(f"[-] Failed to delete serial number Error {e}")

    # endregion

    # region running data functions
    def save_running_data(self, tube_length, energy_used, squareness, serial_number):
        try:
            self.cursor.execute("""SELECT * FROM running_data where serial_number =?""",
                                (serial_number,))
            data = self.cursor.fetchone()
            if data:
                self.cursor.execute("""UPDATE running_data SET squareness = ?, tube_length = ?,
                energy_used =? WHERE serial_number =?""",
                                    (squareness, tube_length, energy_used, serial_number))
            else:
                self.cursor.execute("""INSERT INTO running_data(timestamp, tube_length, energy_used, squareness, 
                serial_number) VALUES(?,?,?,?,?)""",
                                    (time.time(), tube_length, energy_used, squareness, serial_number))
            self.connection.commit()
            log.info(f"[+] Successful, Running Data Saved to the database")
        except Exception as error:
            log.error(f"[-] Failed to save running data Error {error}")

    # endregion

    # region Sync data TB database
    def add_sync_data(self, payload):
        try:
            ts = int(time.time() * 1000)
            self.cursor.execute("""
            INSERT INTO sync_data_table(ts, payload)
            VALUES(?,?)""", (ts, str(payload)))

            log.info(f"[+] Successful, Sync Payload Added to the database")
            self.connection.commit()
        except Exception as e:
            log.error(e)
            return False

    def get_sync_data(self):
        try:
            self.cursor.execute("""SELECT * FROM sync_data_table""")
            data = self.cursor.fetchall()
            if len(data):
                data_payload = [{
                    "ts": item[0],
                    "payload": ast.literal_eval(item[1])
                } for item in data]
                return data_payload
        except Exception as e:
            log.error(e)
            return False

    def clear_sync_data(self, ts):
        try:
            self.cursor.execute("""DELETE FROM sync_data_table where ts=?""",
                                (ts,))
            self.connection.commit()
            log.info(f"Successful, Cleared Sync payload from the database for - {ts}")
        except Exception as e:
            log.error(f'Error in clear_sync_data {e} No sync Data to clear')
            return False

    def add_sync_data2(self, payload):
        try:
            ts = int(time.time() * 1000)
            self.cursor.execute("""
            INSERT INTO sync_data_table2 (ts, payload)
            VALUES(?,?)""", (ts, str(payload)))

            log.info(f"[+] Successful, Sync Payload Added to the database")
            self.connection.commit()
        except Exception as e:
            log.error(e)
            return False

    def get_sync_data2(self):
        try:
            self.cursor.execute("""SELECT * FROM sync_data_table2""")
            data = self.cursor.fetchall()
            if len(data):
                data_payload = [{
                    "ts": item[0],
                    "payload": ast.literal_eval(item[1])
                } for item in data]
                return data_payload
        except Exception as e:
            log.error(e)
            return False

    def clear_sync_data2(self, ts):
        try:
            self.cursor.execute("""DELETE FROM sync_data_table2 where ts=?""",
                                (ts,))
            self.connection.commit()
            log.info(f"Successful, Cleared Sync payload from the database for - {ts}")
        except Exception as e:
            log.error(f'Error in clear_sync_data2 {e} No sync Data to clear')
            return False

    # endregion
