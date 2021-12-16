import psycopg2
import os
import logging, logging.config
import yaml
from datetime import datetime
import random
import json
from pygtail import Pygtail

from read_logs import is_stopped
# ролбэки добавить в эксептах

log_config = {
    "version":1,
    "root":{
        "handlers" : ["console", "file"],
        "level": "DEBUG"
    },
    "handlers":{
        "console":{
            "formatter": "std_out",
            "class": "logging.StreamHandler",
            "level": "DEBUG"
        },
        "file":{
            "formatter":"std_out",
            "class":"logging.FileHandler",
            "level":"DEBUG",
            "filename":"all_messages.log"
        }
    },
    "formatters":{
        "std_out": {
            # "()": "logging.pythonjsonlogger.jsonlogger.JsonFormatter",
            "format": "%(message)s",
        }
    },
}

logging.config.dictConfig(log_config)
log = logging.getLogger('database')

def start_database_process(connection):
    
    cursor = connection.cursor()
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE vendors1 (
            vendor_id SERIAL PRIMARY KEY,
            vendor_name VARCHAR(255) NOT NULL
        )
        """,
        """ CREATE TABLE vendors2 (
            vendor_id SERIAL PRIMARY KEY,
            vendor_name VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE vendors3 (
            vendor_id SERIAL PRIMARY KEY,
            vendor_name VARCHAR(255) NOT NULL
        )
        """,)
    try:
        for command in commands:
            cursor.execute(command)
        cursor.close()
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()

def choose_max_oid(connection, table_name):
    sql = f"""SELECT * from {table_name} order by 1 desc limit 1"""
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        connection.commit()
        cursor.close()
        return result[0]
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        connection.rollback()


def choose_min_oid(connection, table_name):
    sql = f"""SELECT * from {table_name} order by 1 limit 1"""
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        connection.commit()
        cursor.close()
        return result[0]
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        connection.rollback()


def insert_value(table_name, connection):
    sql = f"""INSERT INTO {table_name}
             VALUES(%s,%s);"""
    vendor_name = (5, 'dfd')
    try:
        cursor = connection.cursor()
        cursor.execute(sql, vendor_name)
        connection.commit()
        log.info(f'table:{table_name}, datetime:{datetime.now()}, mode:UPDATE, changing:{vendor_name}')
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        connection.rollback()

def update_value(table_name,  connection):
    sql = f""" UPDATE {table_name}
                SET vendor_name = %s
                WHERE vendor_id = %s"""
    updated_rows = 0
    id = choose_min_oid(connection, table_name)
    message = 'Обновление'
    vendor_info = (message, id)
    try:
        cur = connection.cursor()
        cur.execute(sql,vendor_info)
        updated_rows = cur.rowcount
        connection.commit()
        log.info(f'table:{table_name}, datetime:{datetime.now()}, mode:UPDATE, changing:{vendor_info}')
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        connection.rollback()

    return updated_rows

def delete_value(table_name, conn):
    """ delete part by part id """
    part_id = choose_max_oid(connection, table_name)
    rows_deleted = 0
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table_name} WHERE vendor_id = %s", (part_id,))
        rows_deleted = cur.rowcount
        conn.commit()
        log.info(f'table:{table_name}, datetime:{datetime.now()}, mode:DELETE, changing:{part_id}')
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        connection.rollback()

    return rows_deleted


def choose_action(connection):
    table = random.choice(['vendors1', 'vendors2', 'vendors3'])
    action = random.choice([insert_value, update_value, delete_value])
    action(table, connection) 

is_stopped = False
class Replication():
    def __init__(self, board):
        self.repl_list = []
        self.count = 0
        self.start_repl = board
        self.path = 'all_messages.log'


    def return_log(self):
        tail = Pygtail(self.path, paranoid=True, offset_file='log.offset')
        try:
            line = tail.next()
            log = json.loads(line)
            return log
        except:
            tail._update_offset_file()
    
    def start_replication(self):
        self.count = 0
        for event in self.repl_list:
            self.copy_event(event)
            self.repl_list.pop(event)

    def check_for_replication(self):
        while not is_stopped:
            log = self.return_log()
            if log != None:
                self.repl_list.append(log)
                self.count += 1
            if self.count == self.start_repl:
                self.start_replication()



connection = psycopg2.connect(database="students",
                            user="pmi-b8502",
                            password="32Schero",
                            host="students.ami.nstu.ru",
                            port="5432",
                            options="-c search_path={0},public".format("pmib8502"))

for _ in range(6):
    choose_action(connection)
repl = Replication(5)
repl.check_for_replication()