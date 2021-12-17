import psycopg2
import os
import logging, logging.config
import yaml
from datetime import datetime
import random
import json
from pygtail import Pygtail
import pythonjsonlogger

from read_logs import is_stopped

message = ['RockStart', 'Fast']

def start_database_process(connection):
    
    cursor = connection.cursor()
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE vendors1 (
            vendor_id serial primary key,
            vendor_name varchar(255) NOT NULL,
            mode varchar(255) NOT NULL,
            old_name varchar(255) NOT NULL,
            event_time timestamp without time zone NOT NULL
        )
        """,
        """ CREATE TABLE vendors2 (
            vendor_id serial primary key,
            vendor_name varchar(255) NOT NULL,
            mode varchar(255) NOT NULL,
            old_name varchar(255) NOT NULL,
            event_time timestamp without time zone NOT NULL
        )
        """,
        """
        CREATE TABLE vendors3 (
            vendor_id serial primary key,
            vendor_name varchar(255) NOT NULL,
            mode varchar(255) NOT NULL,
            old_name varchar(255) NOT NULL,
            event_time timestamp without time zone NOT NULL
        )
        """,
        """
        CREATE TABLE log_table (
            time timestamp without time zone NOT NULL,
            table_name varchar(255) NOT NULL,
            mode varchar(255) NOT NULL,
            old varchar(255),
            new varchar(255),
            change_id integer NOT NULL
        )
        """,)
    try:
        for command in commands:
            cursor.execute(command)
        cursor.close()
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

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
    sql = f"""SELECT  from {table_name} order by 1 limit 1"""
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

def get_old_value(connection, table_name, id):
    sql = f"""SELECT vendor_id, vendor_name from {table_name} WHERE vendor_id={id}"""
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
    id = choose_max_oid(connection, table_name)
    id += 1
    try:
        cursor = connection.cursor()
        mes = random.choice(message)
        cursor.execute(sql, (id, mes))
        connection.commit()
        add_log(connection, table_name, datetime.now(), 'INSERT', 'NULL', mes, id)
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
    mes = random.choice(message)
    old = get_old_value(connection, table_name, id)
    vendor_info = (message, id)
    try:
        cur = connection.cursor()
        cur.execute(sql,vendor_info)
        updated_rows = cur.rowcount
        connection.commit()
        add_log(connection, table_name, datetime.now(), 'UPDATE', old, mes, id)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        connection.rollback()

    return updated_rows

def delete_value(table_name, conn):
    """ delete part by part id """
    part_id = choose_max_oid(connection, table_name)
    rows_deleted = 0
    old = get_old_value(connection, table_name, id)
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table_name} WHERE vendor_id = %s", (part_id,))
        rows_deleted = cur.rowcount
        conn.commit()
        add_log(conn, table_name, datetime.now(), 'DELETE', old, 'NULL', part_id)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        connection.rollback()

    return rows_deleted

def choose_action(connection):
    table = random.choice(['vendors1', 'vendors2', 'vendors3'])
    action = random.choice([insert_value, update_value, delete_value])
    action(table, connection) 

def add_log(connection, table, datetime, mode, old, new, id):
    sql = f"""INSERT INTO log_table VALUES(%s,%s,%s,%s,%s,%s);"""
    try:
        cursor = connection.cursor()
        cursor.execute(sql, (datetime, table, mode, old, new, id))
        connection.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        connection.rollback()

def start_replication(connection, id):
    print(id)

class Replication():
    def __init__(self):
        self.c


connection = psycopg2.connect(database="students",
                            user="pmi-b8502",
                            password="32Schero",
                            host="students.ami.nstu.ru",
                            port="5432",
                            options="-c search_path={0},public".format("pmib8502"))

# start_database_process(connection)
# count_transaction = 0
# while True:
#     choose_action(connection)
#     count_transaction += 1
#     if count_transaction % 6:
#         start_replication(connection, count_transaction)