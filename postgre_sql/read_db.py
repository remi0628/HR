import os
import time
import itertools
import psycopg2
import pandas as pd
from sqlalchemy import *
from sqlalchemy import create_engine
import inspect
import sys
sys.path.append('../')
import settings
SAVE_FILE_PATH = settings.SAVE_FILE_PATH

# 空のrace_id数, データ引き抜き最中にエラーを起こした数, 正常に取得できた数
global no_data_num, error_num, perfect_data_num
no_data_num, error_num, perfect_data_num = 0, 0, 0


# DB接続
def engine_generate():
    dsn = os.environ.get('DATABASE_URL_READONLY')
    return create_engine(dsn)

# q 内容表示
def q_display(q, id=None):
    print('-------------------------------------------------')
    print(q)
    if id: # 変数名と値を取得
        callers_local_vars = inspect.currentframe().f_back.f_locals.items()
        name = [var_name for var_name, var_val in callers_local_vars if var_val is id]
        print('{}：{}'.format(name, id))
    print('-------------------------------------------------')

# 最後に動作チェック
def operation_check():
    print('-------------------------------------------------')
    print('正常に取得したレース数：{}'.format(str(perfect_data_num)))
    print('空の[race_id]数：{}　|　エラー数：{}'.format(str(no_data_num - 1), str(error_num)))
    print('-------------------------------------------------')

# idが存在するか
def conf_exist_database(engine, race_id):
    q_select = 'select count(1) from races where id={}'.format(race_id)
    q = (q_select)
    result = engine.execute(q)
    (count,) = result.fetchone()
    return count

# データベースにある最後のレースid取得
def last_record(engine):
        q_select = 'select id from races order by id desc limit 1'
        q = (q_select)
        (id, ) = engine.execute(q)
        print('-------------------------------------------------')
        print('DB最後のレースID：{}'.format(id[0]))
        print('-------------------------------------------------')
        time.sleep(3) # 画面確認の為の待機時間
        return id[0]




# 渡されたレース毎にファイル作成, 馬のデータを入れる
def create_data_race_id(engine, race_id):
    global no_data_num, error_num, perfect_data_num
    flag = conf_exist_database(engine, race_id) # idチェック
    if flag == 1:
        try:
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
            print('|{:5}|{} | R：{:2} | レース距離：{:4} | 1位馬番：{:2} | 土の状態：{:2} '.format(str(race_id), str(race_list[0][5]), str(race_list[0][12]).replace('R', ''), str(race_list[0][4]).replace('m', ''), str(rank1), str(race_list[0][2]) ) )
            save_file = SAVE_FILE_PATH + file_name
            horse_id_list = horse_id_acquisition(engine, race_id)
            create_past_race_data(engine, race_id, horse_id_list, save_file)
            perfect_data_num += 1
        except:
            error_num += 1
    else:
        no_data_num += 1
        print(race_id)
        pass

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
    return rank_list[0][0] #rank_list[0][0]  # 1位の馬番

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

# 馬のidリストを渡してDBから対応するデータをcsv保存
def create_past_race_data(engine, race_id, horse_id_list, save_file=None):
    for horse_id in horse_id_list:
        horse_list = []
        pdList = []
        q = (
            select([
                literal_column('r.date'),
                literal_column('r.res_finished'),
                literal_column('r.place'),
                literal_column('r.course_type'),
                literal_column('r.r'),
                literal_column('r.title'),
                literal_column('r.course_distance'),
                literal_column('r.course_status'),
                literal_column('rh.number'),
                literal_column('rh.res_rank'),
                literal_column('rh.res_time'),
                literal_column('rh.res_tf_time'),
                literal_column('rh.res_corner_indexes'),
                literal_column('rh.weight'),
                literal_column('rh.handicap'),
                literal_column('rh.jockey'),
                literal_column('h.trainer'),
            ]).select_from(
                 table('races').alias('r')
                .join(table('race_horses').alias('rh') , text('rh.race_id = r.id') )
                .join(table('horses').alias('h'), text('rh.horse_id = h.id') )
            )
                .where(literal_column('rh.horse_id') == str(horse_id))
        )
        # q_display(q, horse_id) # q確認
        df = pd.read_sql_query(q, engine)
        # リネーム
        df.rename(columns={'r.date':'年月日', 'r.res_finished':'レース終了済み', 'r.place':'競馬場', 'r.course_type':'コース種別',
                                        'r.r':'R', 'r.title':'レース名', 'r.course_distance':'距離', 'r.course_status':'馬場状態',
                                        'rh.number':'馬番', 'rh.res_rank':'着順', 'rh.res_time':'タイム', 'rh.res_tf_time':'3Fタイム',
                                        'rh.res_corner_indexes':'コーナー通過順', 'rh.weight':'体重', 'rh.handicap':'斤量',
                                        'rh.jockey':'騎手', 'h.trainer':'調教師'}, inplace=True)
        df.sort_values(by=['年月日'], inplace=True, ascending=False)
        # print(df.head(5)) # data確認
        # ファイル名構成
        q = (
            select([
                literal_column('rh.number'),
                literal_column('h.name'),
                literal_column('h.birthday'),
            ]).select_from(
                table('race_horses').alias('rh')
                .join(table('horses').alias('h'), text('rh.horse_id = h.id') )
            )
                .where(literal_column('rh.horse_id') == str(horse_id))
                .where(literal_column('rh.race_id') == str(race_id))
        )
        result = engine.execute(q)
        for row in result:
            n = row
        name = '{}{}{}年{}月{}日'.format(n[0], n[1], n[2].year, n[2].month, n[2].day)
        # ファイル生成
        if save_file:
            os.makedirs(save_file, exist_ok=True)
            with open(save_file + '/' + name + ".csv", mode="w", encoding="cp932", newline="", errors="ignore") as f:
                df.to_csv(f)


# カラム生成　死んでる
def create_clomn(n):
    return [('literal_column(\'{}\')'.format(str(x)) ) for i, x in enumerate(n) ]


# データベースにあるレース全てを取得
def create_csv_data():
    engine = engine_generate()
    max_race_id = last_record(engine) # 最大race_id
    #max_race_id = 10
    min_race_id = 22000
    now = time.time()
    for race_id in range(min_race_id , int(max_race_id)+1):
        create_data_race_id(engine, race_id)
    operation_check()
    print("データ取得処理時間 ：{:.2f}秒".format(time.time() - now) )



def main():
    #engine = engine_generate()
    #race_id = 27465
    #create_data_race_id(engine, race_id)
    create_csv_data()



if __name__ == '__main__':
    main()
