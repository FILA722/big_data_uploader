import settings
import psycopg2


def get_db_credentials():
    for _ in range(10):
        try:
            connection = psycopg2.connect(**settings.POSTGRES_CREDS)
            if connection:
                cursor = connection.cursor()
                return connection, cursor
        except:
            continue


def create_db():
    try:
        connection, cursor = get_db_credentials()
        cursor.execute("""CREATE TABLE IF NOT EXISTS big_data (
                    i SERIAL,
                    c1 CHAR(10), c2 FLOAT, c3 FLOAT, c4 FLOAT, c5 FLOAT,
                    c6 FLOAT, c7 FLOAT, c8 FLOAT, c9 FLOAT, c10 FLOAT,
                    c11 FLOAT, c12 FLOAT, c13 FLOAT, c14 FLOAT, c15 FLOAT,
                    c16 FLOAT, c17 FLOAT, c18 FLOAT, c19 FLOAT, c20 FLOAT,
                    c21 FLOAT, c22 FLOAT, c23 FLOAT) 
                    """)
        connection.commit()
        print('DB created successfully')
    except Exception as e:
        print(e)
        connection.rollback()
    finally:
        connection.close()


def write_to_db(to_db_list, table_name, id_tag=None, header=[], on_conflict=False):
    try:
        conn, cursor = get_db_credentials()
        if on_conflict:
            update_string = ','.join(["{0} = excluded.{0}".format(e) for e in (header)])
            """
           :param to_db_list: list of lists
           :param table_name: str name
           :param id_tag: primary key
           :param update_string: list of columns
           :param on_conflict: False by default
           :return: None
           """
        signs = '(' + ('%s,' * len(to_db_list[0]))[:-1] + ')'
        try:
            args_str = b','.join(cursor.mogrify(signs, x) for x in to_db_list)
            args_str = args_str.decode()
            insert_statement = """INSERT INTO %s VALUES """ % table_name
            conflict_statement = """ ON CONFLICT DO NOTHING"""
            if on_conflict:
                conflict_statement = """ ON CONFLICT ("{0}") DO UPDATE SET {1};""".format(id_tag, update_string)
            cursor.execute(insert_statement + args_str + conflict_statement)
            conn.commit()
        except Exception as e:
            print(e)
            return False
        return True

    finally:
        conn.close()


def prepare_data_to_db():
    row_num = 0
    data_list = []
    with open('test_data_1.csv') as test_data:
        test_gen = (row.split(',') for row in test_data)
        for row in test_gen:
            try:
                float_data = tuple(map(float, row[2:]))
                upload_data = (int(row[0]), row[1], *float_data)
            except ValueError:
                print(f'{row} cannot be added to data_list becouse of wrong element type')
                continue
            data_list.append(upload_data)
            row_num += 1
    write_to_db(data_list, 'big_data')


prepare_data_to_db()
