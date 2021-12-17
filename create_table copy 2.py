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

message = ['RockStart', 'Fast', 'Ozon', 'Amazon', 'Computeruniverse', 'Brandshop', 'Asos', 'Mega', 'Avito', 'Apple']

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


class Replication():
    def __init__(self):
        self.connection= psycopg2.connect(database="students",
                            user="pmi-b8502",
                            password="32Schero",
                            host="students.ami.nstu.ru",
                            port="5432",
                            options="-c search_path={0},public".format("pmib8502"))
        self.log_id = 1
        self.trans_time = 0
        self.count_for_start = 6
    

    def choose_max_oid(self, table_name):
        sql = f"""SELECT * from {table_name} order by 1 desc limit 1"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            result = cursor.fetchone()
            self.connection.commit()
            cursor.close()
            if result == None:
                return 0
            return result[0]
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()

    def choose_min_oid(self, table_name):
        sql = f"""SELECT  from {table_name} order by 1 limit 1"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            result = cursor.fetchone()
            self.connection.commit()
            cursor.close()
            return result[0]
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()

    def get_old_value(self, table_name, id):
        sql = f"""SELECT vendor_id, vendor_name from {table_name} WHERE vendor_id={id}"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            result = cursor.fetchone()
            self.connection.commit()
            cursor.close()
            return result[0]
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()


    def insert_value(self, table_name):
        sql = f"""INSERT INTO {table_name}
                VALUES(%s,%s,%s,%s,%s);"""
        id = self.choose_max_oid(table_name)
        id += 1
        old = 'NULL'
        try:
            cursor = self.connection.cursor()
            mes = random.choice(message)
            cursor.execute(sql, (id, mes, 'INSERT', old, datetime.now()))
            self.connection.commit()
            self.add_log(table_name, datetime.now(), 'INSERT', 'NULL', mes, id)
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()

# TODO
    def update_value(self, table_name):
        sql = f""" UPDATE {table_name}
                    SET vendor_name = %s
                    WHERE vendor_id = %s"""
        updated_rows = 0
        id = self.choose_min_oid(table_name)
        mes = random.choice(message)
        old = self.get_old_value(table_name, id)
        vendor_info = (message, id)
        try:
            cur = self.connection.cursor()
            cur.execute(sql,vendor_info)
            updated_rows = cur.rowcount
            self.connection.commit()
            self.add_log(table_name, datetime.now(), 'UPDATE', old, mes, id)
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()

        return updated_rows

    def delete_value(self, table_name):
        """ delete part by part id """
        part_id = self.choose_max_oid(table_name)
        rows_deleted = 0
        old = self.get_old_value(table_name, id)
        try:
            cur = self.connection.cursor()
            cur.execute(f"DELETE FROM {table_name} WHERE vendor_id = %s", (part_id,))
            rows_deleted = cur.rowcount
            self.connection.commit()
            self.add_log(table_name, datetime.now(), 'DELETE', old, 'NULL', part_id)
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()

        return rows_deleted

    def choose_action(self):
        self.trans_time += 1
        table = random.choice(['vendors1', 'vendors2', 'vendors3'])
        action = random.choice([self.insert_value, self.update_value, self.delete_value])
        action(table) 

    def add_log(self, table, datetime, mode, old, new, id):
        sql = f"""INSERT INTO log_table VALUES(%s,%s,%s,%s,%s,%s);"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql, (datetime, table, mode, old, new, id))
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()

    def replication(self):
        sql = f"""SELECT * from log_table where"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            result = cursor.fet()
            self.connection.commit()
            cursor.close()
            return result
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()


    def start_replication(self):
        self.choose_action()
        if self.trans_time % self.count_for_start == 0:
            self.replication



def main():
    # start_database_process(connection)
    rd = Replication()
    for _ in range(7):
        rd.insert_value('vendors3')
    # while True:
    #     rd.start_replication()

main()