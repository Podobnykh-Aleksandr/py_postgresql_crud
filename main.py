import psycopg2
from pprint import pprint


def create_db(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients(
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(20),
        last_name VARCHAR(30),
        email VARCHAR(254)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phonenumbers(
        number VARCHAR(11) PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id)
    );
    """)
    return


def delete_db(cur):
    cur.execute("""
        DROP TABLE clients, phonenumbers CASCADE;
        """)


def insert_tel(cur, client_id, tel):
    cur.execute("""
        INSERT INTO phonenumbers(number, client_id)
        VALUES (%s, %s)
        """, (tel, client_id))
    return client_id


def insert_client(cur, name=None, surname=None, email=None, tel=None):
    cur.execute("""
        INSERT INTO clients(first_name, last_name, email)
        VALUES (%s, %s, %s)
        """, (name, surname, email))
    cur.execute("""
        SELECT id from clients
        ORDER BY id DESC
        LIMIT 1
        """)
    id = cur.fetchone()[0]
    if tel is None:
        return id
    else:
        insert_tel(cur, id, tel)
        return id


def update_client(cur, id, name=None, surname=None, email=None):
    cur.execute("""
        SELECT * from clients
        WHERE id = %s
        """, (id, ))
    info = cur.fetchone()
    if name is None:
        name = info[1]
    if surname is None:
        surname = info[2]
    if email is None:
        email = info[3]
    cur.execute("""
        UPDATE clients
        SET first_name = %s, last_name = %s, email =%s 
        where id = %s
        """, (name, surname, email, id))
    return id


def delete_phone(cur, number):
    cur.execute("""
        DELETE FROM phonenumbers 
        WHERE number = %s
        """, (number, ))
    return number


def delete_client(cur, id):
    cur.execute("""
        DELETE FROM phonenumbers
        WHERE client_id = %s
        """, (id, ))
    cur.execute("""
        DELETE FROM clients 
        WHERE id = %s
       """, (id,))
    return id


def find_client(cur, name=None, surname=None, email=None, tel=None):
    if name is None:
        name = '%'
    else:
        name = '%' + name + '%'
    if surname is None:
        surname = '%'
    else:
        surname = '%' + surname + '%'
    if email is None:
        email = '%'
    else:
        email = '%' + email + '%'
    if tel is None:
        cur.execute("""
            SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM clients c
            LEFT JOIN phonenumbers p ON c.id = p.client_id
            WHERE c.first_name LIKE %s AND c.last_name LIKE %s
            AND c.email LIKE %s
            """, (name, surname, email))
    else:
        cur.execute("""
            SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM clients c
            LEFT JOIN phonenumbers p ON c.id = p.client_id
            WHERE c.first_name LIKE %s AND c.last_name LIKE %s
            AND c.email LIKE %s AND p.number like %s
            """, (name, surname, email, tel))
    return cur.fetchall()


if __name__ == '__main__':
    with psycopg2.connect(database="clients_db", user="postgres",
                          password="postgres") as conn:
        with conn.cursor() as curs:
            delete_db(curs)
            create_db(curs)
            print("БД создана")
            print("Добавлен клиент id: ", insert_client(curs, "Александр", "Васильев", "mail01@gmail.com"))
            print("Добавлен клиент id: ", insert_client(curs, "Василий", "Теркин", "mail02@mail.ru", 79980321234))
            print("Добавлен клиент id: ", insert_client(curs, "Иван", "Тихвин", "mail03@mail.com", 79110004587))
            print("Добавлен клиент id: ", insert_client(curs, "Алексей", "Жозеф", "mail04@mail.ru", 79010007889))
            print("Добавлена клиент id: ", insert_client(curs, "Елена", "Голованова", "mail05@ya.com"))
            print("Данные в таблицах")
            curs.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())

            print("Телефон добавлен клиенту id: ",
                  insert_tel(curs, 2, 79817876543))
            print("Телефон добавлен клиенту id: ",
                  insert_tel(curs, 1, 79601994802))

            print("Данные в таблицах")
            curs.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())

            print("Изменены данные клиента id: ",
                  update_client(curs, 4, "Иван", None, 'mail00@mail.com'))

            print("Телефон удалён c номером: ",
                  delete_phone(curs, '79601994802'))
            print("Данные в таблицах")
            curs.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())

            print("Клиент удалён с id: ",
                  delete_client(curs, 2))
            curs.execute("""
                            SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM clients c
                            LEFT JOIN phonenumbers p ON c.id = p.client_id
                            ORDER by c.id
                            """)
            pprint(curs.fetchall())

            print('Найденный клиент по имени:')
            pprint(find_client(curs, 'Алексей'))

            print('Найденный клиент по email:')
            pprint(find_client(curs, None, None, 'mail05@ya.com'))

            print('Найденный клиент по имени, фамилии и email:')
            pprint(find_client(curs, 'Иван', 'Тихвин', 'mail03@mail.com'))

            print('Найденный клиент по имени, фамилии, телефону и email:')
            pprint(find_client(curs, 'Алексей', 'Жозеф',
                               'mail04@mail.ru', '79010007889'))

            print('Найденный клиент по имени, фамилии, телефону:')
            pprint(find_client(curs, None, None, None, '79010007889'))