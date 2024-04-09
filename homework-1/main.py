"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv


def main_foo(file):
    """Функция принимает csv файл и добавляет его данные в соответствующую таблицу базы данных Postgres """
    conn = {
        "host": "localhost",
        "database": "north",
        "user": "postgres",
        "password": "KrizopolZ0505"
    }
    with psycopg2.connect(**conn) as connect:
        with connect.cursor() as cursor:
            with open(file) as csv_file:
                next(csv_file)
                csv_reader = csv.reader(csv_file)
                for row in csv_reader:
                    if file == 'north_data/customers_data.csv':
                        query = """INSERT INTO customers 
                            VALUES (%s, %s, %s)"""
                        cursor.execute(query, row)
                    elif file == 'north_data/employees_data.csv':
                        query = """INSERT INTO employees 
                                                    VALUES (%s, %s, %s, %s, %s, %s)"""
                        cursor.execute(query, row)
                    elif file == 'north_data/orders_data.csv':
                        query = """INSERT INTO orders 
                                                    VALUES (%s, %s, %s, %s, %s)"""
                        cursor.execute(query, row)
    connect.close()


def general_foo():
    """Функция проходит циклом по файлам csv и добавляет их на вход main_foo"""
    data = ['north_data/customers_data.csv', 'north_data/employees_data.csv', 'north_data/orders_data.csv']
    for file in data:
        main_foo(file)


general_foo()