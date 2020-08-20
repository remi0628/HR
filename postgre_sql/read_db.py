import os
import psycopg2
from sqlalchemy import *
from sqlalchemy import create_engine

# DBへ接続
def engine_generate():
    dsn = os.environ.get('DATABASE_URL_READONLY')
    return create_engine(dsn)

# q 内容表示
def q_display(q):
    print('-------------------------------------------------')
    print(q)
    print('-------------------------------------------------')

'''
result = engine.execute('select * from races limit 10;')

q = (
  'select * from races where id <= 500;'
)

'''
# test
def test_serect_race_id(engine):
    race_id_num = 1
    q = (
        select([
            literal_column('id'),
            literal_column('title'),
        ])
            .select_from(table('races'))
            .where(literal_column('id') >= race_id_num)
        )

    q_display(q)
    result = engine.execute(q)
    for row in result:
        print(row)

# レース毎にファイル作成, 馬のデータを入れる
def create_data_race_id(engine):
    q = ('select * from races where id=4971')
    q_display(q)
    result = engine.execute(q)
    for row in result:
        print(row)



def main():
    engine = engine_generate()
    create_data_race_id(engine)


if __name__ == '__main__':
    main()
