import psycopg2
from psycopg2 import OperationalError


def create_client_db(database, user, password):
    conn = psycopg2.connect(database=database, user=user, password=password)
    with conn.cursor() as cur:
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
            conn.commit()
            print('Таблицы клиентов и телефонных номеров созданы успешно')
        except OperationalError as e:
            print(f"Произошла ошибка '{e}'")
    conn.close()


def insert_phone(database, user, password, phone, email):
    conn = psycopg2.connect(database=database, user=user, password=password)
    with conn.cursor() as cur:
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
            conn.commit()
            print(f'Телефон {phone} для {email} успешно добавлен')
        except OperationalError as e:
            print(f"Произошла ошибка '{e}'")
    conn.close()


def insert_client(database, user, password, name, surname, email):
    conn = psycopg2.connect(database=database, user=user, password=password)
    with conn.cursor() as cur:
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
            conn.commit()
            print(f'Клиент {name} {surname} {email} успешно добавлен')
        except OperationalError as e:
            print(f"Произошла ошибка '{e}'")
    conn.close()


def update_client(database, user, password, name, surname, email, new_email):
    conn = psycopg2.connect(database=database, user=user, password=password)
    with conn.cursor() as cur:
        try:
            cur.execute("""
                UPDATE
                    client
                SET
                    name = %s,
                    surname = %s,
                    "e-mail" = %s
                WHERE
                    "e-mail" = %s;
                """,(name, surname, new_email, email))
            conn.commit()
            if cur.rowcount == 0:
                print(f'Запись {email} не найдена')
            else:
                print(f'Данные о клиенте {name} {surname} {new_email} успешно изменены. Старый e-mail: {email}')
        except OperationalError as e:
            print(f"Произошла ошибка '{e}'")
    conn.close()


def delete_phone(database, user, password, phone):
    conn = psycopg2.connect(database=database, user=user, password=password)
    with conn.cursor() as cur:
        try:
            cur.execute("""
                DELETE
                    FROM
                        phone_num
                    WHERE
                        number = %s;
                """,(phone,))
            conn.commit()
            if cur.rowcount == 0:
                print(f'Телефон {phone} не найден')
            else:
                print(f'Телефон {phone} успешно удален')
        except OperationalError as e:
            print(f"Произошла ошибка '{e}'")
    conn.close()


def delete_client(database, user, password, email):
    conn = psycopg2.connect(database=database, user=user, password=password)
    with conn.cursor() as cur:
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
            conn.commit()
            if cur.rowcount == 0:
                print(f'Клиент {email} не найден')
            else:
                print(f'Клиент {email} успешно удален')
        except OperationalError as e:
            print(f"Произошла ошибка '{e}'")
    conn.close()


def find_client(database, user, password, phone, name, surname, email):
    conn = psycopg2.connect(database=database, user=user, password=password)
    with conn.cursor() as cur:
        try:
            if phone == '':
                cur.execute("""
                    SELECT
                        name, surname, "e-mail"
                    FROM 
                        client
                    WHERE
                        name LIKE %s
                        AND surname LIKE %s
                        AND "e-mail" LIKE %s;
                    """,(name, surname, email))
                for row in cur.fetchall():
                    print(f'Данные о клиенте: {row}')
            else:
                cur.execute("""
                    SELECT
                        name, surname, "e-mail"
                    FROM
                        client
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
                    print(f'Телефон {phone} не найден')
                else:
                    print(f'Телефон {phone}')
                    print(f'Данные о клиенте: {cur.fetchall()}')
            conn.commit()
        except OperationalError as e:
            print(f"Произошла ошибка '{e}'")
    conn.close()

if __name__ == '__main__':

    database = "netology_db"
    user = "postgres"
    password = ""

    create_client_db(database, user, password)

    insert_client(database, user, password, 'Александр', 'Дуругян', 'фв@нф.кг')
    insert_client(database, user, password, 'Дмитрий', 'Бычков', 'tsuruta@verizon.net')
    insert_client(database, user, password, 'Герман', 'Маслов', 'engelen@icloud.com')
    insert_client(database, user, password, 'Вера', 'Дьякова','jsmith@att.net')
    insert_client(database, user, password, 'Ева', 'Сорокина', 'lridener@optonline.net')
    insert_client(database, user, password, 'Лука', 'Наумов', 'danny@live.com')
    insert_client(database, user, password, 'Вера', 'Бондарева', 'fairbank@att.net')
    insert_client(database, user, password, 'Алексей', 'Лукьянов', 'kudra@aol.com')
    insert_client(database, user, password, 'Денис', 'Макеев', 'ewaters@aol.com')

    insert_phone(database, user, password, '6514549557', 'фв@нф.кг')
    insert_phone(database, user, password, '6514549558', 'фв@нф.кг')
    insert_phone(database, user, password, '9761452480', 'tsuruta@verizon.net')

    update_client(database, user, password, name='Александр', surname='Дуругян', email='фв@нф.кг',
                  new_email='ad@ya.ru')

    delete_phone(database, user, password, '6514549557')

    delete_client(database, user, password, 'ad@ya.ru')

    find_client(database, user, password, '', '%', '%', '%@aol%')
