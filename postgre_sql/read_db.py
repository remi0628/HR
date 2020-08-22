import os
import psycopg2
from sqlalchemy import *
from sqlalchemy import create_engine
import sys
sys.path.append('../')
import settings
SAVE_FILE_PATH = settings.SAVE_FILE_PATH

# DB接続
def engine_generate():
    dsn = os.environ.get('DATABASE_URL_READONLY')
    return create_engine(dsn)

# q 内容表示
def q_display(q):
    print('-------------------------------------------------')
    print(q)
    print('-------------------------------------------------')




# q_test
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


# 渡されたレース毎にファイル作成, 馬のデータを入れる
def create_data_race_id(engine, race_id):
    race_list = []
    q_select = 'select * from races where id={}'.format(race_id)
    q = (q_select)
    # q_display(q)
    result = engine.execute(q)
    for row in result:
        race_list.append(row)
    rank1 = race_horse_rank1(engine, race_id) # 1位の馬番取得
    # レース日-R-距離-土状態-1位馬番
    file_name = '{}-{}-{}-{}-{}'.format( str(race_list[0][5]), str(race_list[0][12]).replace('R', '').zfill(2), str(race_list[0][4]).replace('m', ''), str(race_list[0][2]), str(rank1) )
    print(file_name)
    os.makedirs(SAVE_FILE_PATH + file_name, exist_ok=True)
    horse_id_list = horse_id_acquisition(engine, race_id)
    print(horse_id_list)

# レースidを渡すと一着の馬番を返す
def race_horse_rank1(engine, race_id):
    rank_list = []
    ranking_1 = 1
    q = (
        select([
            literal_column('number'),
            literal_column('res_rank'),
        ])
            .select_from(table('race_horses'))
            .where(literal_column('race_id') == race_id)
            .where(literal_column('res_rank') == ranking_1)
    )
    # q_display(q)
    result = engine.execute(q)
    for row in result:
        rank_list.append(row)
    rank1 = rank_list[0][0]  # 1位の馬番
    return rank1

# レースidから馬のidリスト取得
def horse_id_acquisition(engine, race_id):
    list = []
    horse_id_list = []
    q = (
        select([
            literal_column('horse_id'),
            literal_column('jockey'),
            literal_column('number'),
            literal_column('race_id'),
            literal_column('res_rank'),
        ])
            .select_from(table('race_horses'))
            .where(literal_column('race_id') == race_id)
    )
    # q_display(q)
    result = engine.execute(q)
    for row in result:
        # list.append(row)  # horse_id, jockey, number, race_id, res_rank
        horse_id_list.append(row[0])
    return horse_id_list

def create_past_race_data(engine, horse_id_list):
    for horse_id in horse_id_list:
        q = (
            select([
                literal_column('')
            ])
        )

def main():
    engine = engine_generate()
    race_id = 4971
    create_data_race_id(engine, race_id)


if __name__ == '__main__':
    main()
