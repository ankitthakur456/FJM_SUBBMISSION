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
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS running_data(timestamp INTEGER, max_temperature REAL,
            energy_used REAL, heating_time REAL, serial_number STRING)""")
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS misc(id INTEGER NOT NULL DEFAULT "1",
                                                current_date_ DATE NOT NULL DEFAULT (date('now','localtime')),
                                                current_shift VARCHAR(1) NOT NULL, current_hour INTEGER)''')
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

    def delete_serial_number(self, serial_number):
        try:
            self.cursor.execute("""DELETE FROM queue where serial_number =?""", (serial_number,))
            self.connection.commit()
            log.info(f"[+] Successful, Serial Number Deleted from the database")
        except Exception as e:
            log.error(f"[-] Failed to delete serial number Error {e}")
    # endregion

    # region running data functions
    def save_running_data(self, max_temperature, energy_used, heating_time, serial_number):
        try:
            self.cursor.execute("""SELECT * FROM running_data where serial_number =?""",
                                (serial_number,))
            data = self.cursor.fetchone()
            if data:
                self.cursor.execute("""UPDATE running_data SET heating_time = ?, max_temperature = ?,
                energy_used =? WHERE serial_number =?""",
                                    (heating_time, max_temperature, energy_used, serial_number))
            else:
                self.cursor.execute("""INSERT INTO running_data(timestamp, max_temperature, energy_used, heating_time, 
                serial_number) VALUES(?,?,?,?,?)""",
                                (time.time(), max_temperature, energy_used, heating_time, serial_number))
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


    def add_misc_data(self):
        try:
            shift = 'A'
            hour_ = datetime.datetime.now().hour
            today_ = (datetime.datetime.now() - datetime.timedelta(hours=7)).strftime("%F")
            self.cursor.execute("""INSERT INTO misc(id, current_shift, current_date_, current_hour)
                                VALUES (?,?,?,?)""",
                                (1, shift, today_,
                                 hour_))
            self.connection.commit()
            log.info("Successful: Misc data added to the database.")
        except Exception as e:
            log.error(f'Error {e} Could not add Misc data to the Database')

    def get_misc_data(self):
        self.cursor.execute('''SELECT * FROM misc''')
        check = self.cursor.fetchone()
        if check is None:
            self.add_misc_data()
        self.cursor.execute('''SELECT current_date_,current_shift,current_hour FROM misc WHERE id=1''')
        try:
            data = self.cursor.fetchone()
            prevD = data[0]
            prevS = data[1]
            prevH = data[2]
            return prevD, prevS, prevH
        except Exception as e:
            log.error(f'ERROR: fetching misc data {e}')
            return 'N', 'N', 0
    # endregion

