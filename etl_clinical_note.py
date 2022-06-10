import pandas as pd
import re
import psycopg2 as pg
import json
from logging import error, info

def read_data():
    df = transaction()
    note_split = df['note'].str.split('\n').str
    df['name'] = note_split[0]
    print(df)


def transaction():
    transaction_result = ''

    # connection info : 설정 json 파일의 connection 정보 활용
    with open('config/connector.json', 'r') as f:
        config = json.load(f)['postgre']

    conn = pg.connect(
        host=config['host'],
        database=config['database'],
        port=config['port'],
        user=config['user'],
        password=config['password']
    )

    try:
        with conn.cursor() as cursor:
            sql = 'select index, note from de.clinical_note limit 10'
            cursor.execute(sql)
            df = pd.DataFrame(cursor.fetchall(), columns=['index', 'note'])

    except:
        transaction_result = 'error'

    else:
        transaction_result = str(cursor.rowcount).__add__(' record was selected')

    finally:
        conn.close()
        print(transaction_result)
        return df

def main():
    df = read_data()

if __name__ == '__main__':
    main()