import psycopg2
import random
from datetime import datetime

url = {
    'Ozon' : 'https://www.ozon.ru/',
    'Computeruniverse' : 'https://www.computeruniverse.net/ru',
    'Asos' : 'https://www.asos.com',
    'Madshop' : 'https://www.madshop.com',
    'Amazon' : 'https://www.amazon.com',
    'Avito' : 'https://www.avito.ru',
    'Basketshop' : 'https://www.basketshop.com',
    'Brandshop' : 'https://www.brandshop.com',
    'Sneakerhead' : 'https://www.sneakerhead.com',
    'Traektoria' : 'https://www.traektoria.com',
    'Oktyabr' : 'https://www.oktyabr.com',
    'Slamdunk' : 'https://www.slamdunk.com',
    'Destroy' : 'https://www.destroy.com',
    'Tsum' : 'https://www.tsum.com',
}
message = [
    'Ozon',
    'Computeruniverse',
    'Asos',
    'Madshop',
    'Amazon',
    'Avito',
    'Basketshop',
    'Brandshop',
    'Sneakerhead',
    'Traektoria',
    'Oktyabr',
    'Slamdunk',
    'Destroy',
    'Tsum'
]

# программа инициализации данных
def start_database_process(connection):
    cursor = connection.cursor()
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE vendors1 (
            vendor_id serial primary key,
            vendor_name varchar(255) NOT NULL,
            url varchar(255) NOT NULL,
            mode varchar(255) NOT NULL,
            old_name varchar(255),
            event_time timestamp without time zone NOT NULL
        )
        """,
        """ CREATE TABLE vendors2 (
            vendor_id serial primary key,
            vendor_name varchar(255) NOT NULL,
            url varchar(255) NOT NULL,
            mode varchar(255) NOT NULL,
            old_name varchar(255),
            event_time timestamp without time zone NOT NULL
        )
        """,
        """
        CREATE TABLE vendors3 (
            vendor_id serial primary key,
            vendor_name varchar(255) NOT NULL,
            url varchar(255) NOT NULL,
            mode varchar(255) NOT NULL,
            old_name varchar(255),
            event_time timestamp without time zone NOT NULL
        )
        """,
        """
        CREATE TABLE log_table (
            log_id serial primary key,
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
        connection.rollback()

class Replication():
    def __init__(self):
        self.connection= psycopg2.connect(database="students",
                            user="pmi-b8502",
                            password="32Schero",
                            host="students.ami.nstu.ru",
                            port="5432",
                            options="-c search_path={0},public".format("pmib8502"))
        self.log_id = 0
        self.trans_time = 0
        self.count_for_start = 6
        self.begin = False
    

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
        sql = f"""SELECT * from {table_name} order by 1 limit 1"""
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
            return result[1]
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()

    def next_val(self, table_name, id_name):
        sql = f"""select nextval('{table_name}_{id_name}_seq'::regclass)"""
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
                VALUES(%s,%s,%s,%s,%s,%s);"""
        id = self.choose_max_oid(table_name) + 1
        old = 'NULL'
        try:
            cursor = self.connection.cursor()
            mes = random.choice(message)
            cursor.execute(sql, (id, mes, url[mes], 'INSERT', old, datetime.now()))
            self.connection.commit()
            self.trans_time += 1
            self.add_log(table_name, datetime.now(), 'INSERT', None, mes, id)
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()

    def update_value(self, table_name):
        sql = f""" UPDATE {table_name}
                    SET vendor_name = %s, url= %s, mode = %s, old_name = %s, event_time = %s
                    WHERE vendor_id = %s"""
        updated_rows = 0
        id = self.choose_min_oid(table_name)
        mes = random.choice(message)
        old = self.get_old_value(table_name, id)
        vendor_info = (mes, url[mes], 'UPDATE', old, datetime.now(), id)
        try:
            cur = self.connection.cursor()
            cur.execute(sql,vendor_info)
            updated_rows = cur.rowcount
            self.connection.commit()
            self.trans_time += 1
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
        old = self.get_old_value(table_name, part_id)
        try:
            cur = self.connection.cursor()
            cur.execute(f"DELETE FROM {table_name} WHERE vendor_id = %s", (part_id,))
            rows_deleted = cur.rowcount
            self.connection.commit()
            self.trans_time += 1
            self.add_log(table_name, datetime.now(), 'DELETE', old, None, part_id)
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()
        return rows_deleted

    def choose_action(self):
        table = random.choice(['vendors1', 'vendors2', 'vendors3'])
        action = random.choice([self.insert_value, self.update_value, self.delete_value])
        action(table) 

    def add_log(self, table, datetime, mode, old, new, id):
        sql = f"""INSERT INTO log_table VALUES(%s,%s,%s,%s,%s,%s,%s);"""
        log_id = self.next_val('log_table', 'log_id')
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql, (log_id, datetime, table, mode, old, new, id))
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()

    def replication(self):
        sql = f"""SELECT * from log_table where log_id > {self.log_id} and log_id <= {self.trans_time} order by time desc"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
            self.connection.commit()
            cursor.close()
            return result
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()

    def decide_collision(self, logs):
        result = []
        id_result = []
        for log in logs:
            if result == []:
                result.append(log)
                id_result.append(log[6])
                continue
            if log[6] not in id_result:
                result.append(log)
                id_result.append(log[6])
        if len(result) != len(logs):
            print('Возникла коллизия. Коллизия ращрешена в пользу последнего обновления.')
        return result

    def start_replication(self):
        try:
            if not self.begin:
                self.choose_action()
            if self.trans_time % self.count_for_start == 0:
                self.begin = False
                result = self.replication()
                result = self.decide_collision(result)
                for r in result:
                    print(r[0], '\t', r[1], '\t', r[2], '\t',r[3], '\t',r[4], '\t',r[5], '\t',r[6])
                for r in result:
                    self.copy(r)
                self.log_id = self.trans_time
        except Exception:
            self.connection.rollback()
            raise Exception
            
    
    def replication_insert(self, table_name, id_record, mes, old, time):
        sql = f"""INSERT INTO {table_name}
                VALUES(%s,%s,%s,%s,%s,%s);"""
        # id_record = self.next_val(table_name, 'vendor_id')
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql, (id_record, mes, url[mes], 'INSERT', old, time))
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()

    def replication_update(self, table_name, id_record, mes, old, time):
        sql = f""" UPDATE {table_name}
                    SET vendor_name = %s, url=%s, mode = %s, old_name = %s, event_time = %s
                    WHERE vendor_id = %s"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql, (mes, url[mes], 'UPDATE', old, time, id_record))
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()


    def replication_delete(self, table_name, id_record):
        try:
            cur = self.connection.cursor()
            cur.execute(f"DELETE FROM {table_name} WHERE vendor_id = %s", (id_record,))
            rows_deleted = cur.rowcount
            self.connection.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.connection.rollback()

    def copy (self, copy_string):
        copy_id = copy_string[0]
        time = copy_string[1]
        table = copy_string[2]
        mode = copy_string[3]
        old = copy_string[4]
        new = copy_string[5]
        id_record = copy_string[6]
        if table == 'vendors1':
            if mode == 'INSERT':
                self.replication_insert('vendors2', id_record, new, old, time)
                self.replication_insert('vendors3', id_record, new, old, time)
            if mode == 'UPDATE':
                self.replication_update('vendors2', id_record, new, old, time)
                self.replication_update('vendors3', id_record, new, old, time)
            if mode == 'DELETE':
                self.replication_delete('vendors2', id_record)
                self.replication_delete('vendors3', id_record)
        
        if table == 'vendors2':
            if mode == 'INSERT':
                self.replication_insert('vendors1', id_record, new, old, time)
                self.replication_insert('vendors3', id_record, new, old, time)
            if mode == 'UPDATE':
                self.replication_update('vendors1', id_record, new, old, time)
                self.replication_update('vendors3', id_record, new, old, time)
            if mode == 'DELETE':
                self.replication_delete('vendors1', id_record)
                self.replication_delete('vendors3', id_record)
        
        if table == 'vendors3':
            if mode == 'INSERT':
                self.replication_insert('vendors1', id_record, new, old, time)
                self.replication_insert('vendors2', id_record, new, old, time)
            if mode == 'UPDATE':
                self.replication_update('vendors1', id_record, new, old, time)
                self.replication_update('vendors2', id_record, new, old, time)
            if mode == 'DELETE':
                self.replication_delete('vendors1', id_record)
                self.replication_delete('vendors2', id_record)




def main():
    rd = Replication()
    start_database_process(rd.connection)
    for _ in range(6):
        rd.insert_value('vendors1')
    while True:
        rd.start_replication()

main()