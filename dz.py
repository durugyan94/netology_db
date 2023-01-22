import psycopg2
from psycopg2 import OperationalError


def create_client_db():
    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS client (
        id serial PRIMARY KEY,
        name varchar NOT NULL,
        surname varchar NOT NULL,
        "e-mail" varchar NOT NULL UNIQUE CHECK ("e-mail" ILIKE '_%@_%._%' AND "e-mail" NOT LIKE '%@%@%')
        );
        CREATE TABLE IF NOT EXISTS phone_num (
            number char(10) UNIQUE PRIMARY KEY CHECK (number SIMILAR TO '[0-9]{10}'),
            client_id integer REFERENCES client(id)
        );
        """)
        print('Таблицы клиентов и телефонных номеров созданы успешно')
    except OperationalError as e:
        print(f"Произошла ошибка '{e}'")

def insert_phone(phone, email):
    try:
        cur.execute("""
            INSERT 
            INTO 
                phone_num 
            SELECT
                    %s,
                    id
            FROM
                    client
            WHERE
                    "e-mail" = %s;
            """,(phone, email))
        print(f'Телефон {phone} для {email} успешно добавлен')
    except OperationalError as e:
        print(f"Произошла ошибка '{e}'")

def insert_client(name, surname, email):
    try:
        cur.execute("""
            INSERT
                INTO
                client (name,
                        surname,
                        "e-mail")
            VALUES 
                (%s, %s, %s);
            """,(name, surname, email))
        print(f'Клиент {name} {surname} {email} успешно добавлен')
    except OperationalError as e:
        print(f"Произошла ошибка '{e}'")

def update_name(id, name):
    try:
        cur.execute("""
            UPDATE
                client
            SET
                name = %s
            WHERE
                "id" = %s;
            """,(name, id,))
        if cur.rowcount == 0:
            print(f'Запись {id} не найдена')
        else:
            print(f'Данные о клиенте c ID {id} успешно изменены.')
    except OperationalError as e:
        print(f"Произошла ошибка '{e}'")

def update_surname(id, surname):
    try:
        cur.execute("""
            UPDATE
                client
            SET
                surname = %s
            WHERE
                "id" = %s;
            """,(surname, id,))
        if cur.rowcount == 0:
            print(f'Запись {id} не найдена')
        else:
            print(f'Данные о клиенте c ID {id} успешно изменены.')
    except OperationalError as e:
        print(f"Произошла ошибка '{e}'")

def update_email(id, email):
    try:
        cur.execute("""
            UPDATE
                client
            SET
                "e-mail" = %s
            WHERE
                "id" = %s;
            """,(email, id,))
        if cur.rowcount == 0:
            print(f'Запись {id} не найдена')
        else:
            print(f'Данные о клиенте c ID {id} успешно изменены.')
    except OperationalError as e:
        print(f"Произошла ошибка '{e}'")

def delete_phone(phone):
    try:
        cur.execute("""
            DELETE
                FROM
                    phone_num
                WHERE
                    number = %s;
            """,(phone,))
        if cur.rowcount == 0:
            print(f'Телефон {phone} не найден')
        else:
            print(f'Телефон {phone} успешно удален')
    except OperationalError as e:
        print(f"Произошла ошибка '{e}'")

def delete_client(email):
    try:
        cur.execute("""
            DELETE
            FROM
                phone_num
            WHERE
                client_id IN (
                SELECT
                    id
                FROM
                    client
                WHERE
                    "e-mail" = %s);
            """,(email,))
        print(f'Удалено {cur.rowcount} телефонных номеров')
        cur.execute("""            
            DELETE
            FROM
                client
            WHERE
                "e-mail" = %s;
            """,(email,))
        if cur.rowcount == 0:
            print(f'Клиент {email} не найден')
        else:
            print(f'Клиент {email} успешно удален')
    except OperationalError as e:
        print(f"Произошла ошибка '{e}'")

def find_phone(phone):
    try:
        cur.execute("""
            SELECT
                c.name, c.surname, c."e-mail", p.number
            FROM
                client c
            JOIN 
                phone_num p on c.id = p.client_id
            WHERE
                %s IN (
            SELECT
                number
            FROM
                phone_num
            WHERE
                client_id = id)
            """, (phone,))
        if cur.rowcount == 0:
            print(f'Телефон: {phone} не найден')
        else:
            print(f'Телефон: {phone}')
            print(f'Данные о клиенте: {cur.fetchall()}')
    except OperationalError as e:
        print(f"Произошла ошибка '{e}'")

def find_id(id):
    try:
        cur.execute("""
            SELECT
                c.name, c.surname, c."e-mail"
            FROM
                client c
            WHERE
                c.id = %s
            """, (id,))
        if cur.rowcount == 0:
            print(f'ID: {id} не найден')
        else:
            print(f'ID: {id}')
            print(f'Данные о клиенте: {cur.fetchall()}')
    except OperationalError as e:
        print(f"Произошла ошибка '{e}'")

def find_name(name):
    try:
        cur.execute("""
            SELECT
                c.name, c.surname, c."e-mail"
            FROM 
                client c
            WHERE
                c.name LIKE %s;
            """,(name,))
        index = 0
        for row in cur.fetchall():
            index += 1
            print(f'Данные о клиенте: {row}')
        if index == 0:
            print('Таких данных нет')
    except OperationalError as e:
        print(f"Произошла ошибка '{e}'")

def find_surname(surname):
    try:
        cur.execute("""
            SELECT
                c.name, c.surname, c."e-mail"
            FROM 
                client c
            WHERE
                c.surname LIKE %s;
            """,(surname,))
        index = 0
        for row in cur.fetchall():
            index += 1
            print(f'Данные о клиенте: {row}')
        if index == 0:
            print('Таких данных нет')
    except OperationalError as e:
        print(f"Произошла ошибка '{e}'")

def find_email(email):
    try:
        cur.execute("""
            SELECT
                c.name, c.surname, c."e-mail"
            FROM 
                client c
            WHERE
                c."e-mail" LIKE %s;
            """,(email,))
        index = 0
        for row in cur.fetchall():
            index += 1
            print(f'Данные о клиенте: {row}')
        if index == 0:
            print('Таких данных нет')
    except OperationalError as e:
        print(f"Произошла ошибка '{e}'")

if __name__ == '__main__':

    database = "netology_db"
    user = "postgres"
    password = ""

    with psycopg2.connect(database=database, user=user, password=password) as conn:
        with conn.cursor() as cur:
            
            create_client_db()

            insert_client('Александр', 'Дуругян', 'фв@нф.кг')
            insert_client('Дмитрий', 'Бычков', 'tsuruta@verizon.net')
            insert_client('Герман', 'Маслов', 'engelen@icloud.com')
            insert_client('Вера', 'Дьякова','jsmith@att.net')
            insert_client('Ева', 'Сорокина', 'lridener@optonline.net')
            insert_client('Лука', 'Наумов', 'danny@live.com')
            insert_client('Вера', 'Бондарева', 'fairbank@att.net')
            insert_client('Алексей', 'Лукьянов', 'kudra@aol.com')
            insert_client('Денис', 'Макеев', 'ewaters@aol.com')

            insert_phone('6514549557', 'фв@нф.кг')
            insert_phone('6514549558', 'фв@нф.кг')
            insert_phone('9761452480', 'tsuruta@verizon.net')
            insert_phone('9761452234', 'ewaters@aol.com')

            delete_phone('6514549557')

            delete_client('ad@ya.ru')

            update_name('3', 'Александр')
            update_surname('3', 'Дуругян')
            update_email('3', 'ad@ya.ru')

            find_phone('9761452480')
            find_id('3')