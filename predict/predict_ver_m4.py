import json
import pprint
import datetime
import traceback
import numpy as np
import pandas as pd
import tensorflow as tf
from collections import OrderedDict
from datetime import datetime as dt

JSON_RACE = './sample.json'

def predict(race):
    with open(race, encoding="utf-8_sig") as f:
        race_json = json.load(f)
    x_list = []
    today_data, today_data_horse = today_race(race_json)
    race_horse = latest_races(race_json, today_data, today_data_horse)
    #pprint.pprint(race_json['course_distance'], width=60)
    x_list.append(race_horse)
    X = np.array(x_list)
    X = X.astype("float")
    x_npy = np.load('X.npy')
    y_npy = np.load('Y.npy')
    print(len(x_npy))
    print(len(x_npy[0]))
    print(len(x_npy[0][0]))
    print(len(x_npy[0][0][0]))
    print(len(x_npy[0][0][0][0]))
    print(len(y_npy))
    print(len(y_npy[0]))
    print(len(y_npy[0][0]))
    print(len(y_npy[0][0][0]))
    print(len(y_npy[0][0][0][0]))
    #prediction_result = model_save_predict(X)
    #model_load_predict()
    #print(X)
    #return prediction_result # 予測結果が入っている

# 当日データ
def today_race(race_json):
    today_data = {}
    today_data_horse = []
    race_data = race_json.copy()
    # コース詳細{date, place, type, r, distance, status}
    today_data['date'] = race_data['date']
    today_data['place'] = race_data['place']
    today_data['course_type'] = race_data['course_type']
    today_data['r'] = race_data['r']
    today_data['course_distance'] = race_data['course_distance']
    today_data['course_status'] = race_data['course_status']
    # 馬詳細
    number, weight, handicap, birthday, jockey, trainer = [], [], [], [], [], []
    #pprint.pprint(race_data['horses'][i]['weight'],  width=60) #p
    horse_cnt = len(race_data['horses'])
    for i in range(horse_cnt):
        number.append(i + 1)
        weight.append(race_data['horses'][i]['weight'])
        handicap.append(race_data['horses'][i]['handicap'])
        birthday.append(race_data['horses'][i]['horse']['birthday'])
        #jockey.append(race_data['horses'][i]['jockey'])
        #trainer.append(race_data['horses'][i]['horse']['trainer'])
    # list [cnt, number, weight, handicap, birthday]
    today_data_horse.append(horse_cnt)
    today_data_horse.append(number)
    today_data_horse.append(weight)
    today_data_horse.append(handicap)
    today_data_horse.append(birthday)
    #today_data_horse.append(jockey)
    #today_data_horse.append(trainer)
    return today_data, today_data_horse

# DataFrameへ格納
def latest_races(race_json, today_data, today_data_horse):
    race_data = race_json.copy()
    #print(len(race_data['horses'][i]['latest_races']))
    horse_num = today_data_horse[0]
    race_horse = []
    for h in range(18):
        if horse_num > h:
            race_data['horses'][h]['latest_races'].sort(key=lambda x: x['date'], reverse=True) # 日付降順

            df_ = pd.DataFrame(np.zeros((1, 20)), columns=["horse_cnt", "money", "result_rank", "len", "popularity", "weight",
                                                           "borden_weight", "sec", "diff_accident", "threeF", "birth_days",
                                                           "place_Urawa", "place_Funabashi",
                                                           "place_Ooi", "place_Kawasaki", "place_other", "soil_heavy",
                                                           "soil_s_heavy", "soil_good", "soil_bad"])
            dropList = []
            for i in range(13): # 一応12レース分まで取得
                try:
                    # データで埋めた10レースまで取得
                    if len(df_) > 9:
                        continue
                    horse_id = race_data['horses'][h]['horse_id'] # 馬id
                    if i == 0:
                        # 当日データ
                        course_status = today_data['course_status']
                        course_type = today_data['course_type']

                        r = today_data['r'].replace('R', '')
                        course_distance = today_data['course_distance'].replace('m', '')
                        horse_cnt = today_data_horse[0]
                        number = today_data_horse[1][h]
                        handicap = today_data_horse[3][h]
                        weight = today_data_horse[2][h]
                        today = dt.strptime(today_data['date'], '%Y-%m-%d')
                        birth_day = dt.strptime(today_data_horse[4][h], '%Y-%m-%d')
                        birthDate = today - birth_day
                        place = today_data['place']

                    else:
                        # 過去データ
                        #print(race_data['horses'][h]['latest_races'][i-1]['horses'])
                        course_status = race_data['horses'][h]['latest_races'][i-1]['course_status']
                        course_type = race_data['horses'][h]['latest_races'][i-1]['course_type']

                        r = race_data['horses'][h]['latest_races'][i-1]['r'].replace('R', '')
                        course_distance = race_data['horses'][h]['latest_races'][i-1]['course_distance'].replace('m', '')
                        horse_cnt = len(race_data['horses'][h]['latest_races'][i-1]['horses'])
                        for n in range(horse_cnt): # 馬番
                            if race_data['horses'][h]['latest_races'][i-1]['horses'][n]['horse_id'] == horse_id:
                                number = race_data['horses'][h]['latest_races'][i-1]['horses'][n]['number']
                                num = n # 何番目の枠にデータがあるのか
                        handicap = race_data['horses'][h]['latest_races'][i-1]['horses'][n]['handicap']
                        weight = race_data['horses'][h]['latest_races'][i-1]['horses'][n]['weight']
                        today = dt.strptime(race_data['horses'][h]['latest_races'][i-1]['horses'][n]['date'], '%Y-%m-%d')
                        birth_day = dt.strptime(today_data_horse[4][h], '%Y-%m-%d')
                        birthDate = today - birth_day
                        place = race_data['horses'][h]['latest_races'][i-1]['place']

                        result_rank = race_data['horses'][h]['latest_races'][i-1]['horses'][n]['res_rank']
                        #print(race_data['horses'][h]['latest_races'][i-1]['horses'][n]['date'])
                        #print(race_data['horses'][h]['latest_races'][i-1]['horses'][n]['res_time'])
                        try:
                            m_s_f = datetime.timedelta(seconds=race_data['horses'][h]['latest_races'][i-1]['horses'][n]['res_time'])
                        except:
                            continue
                        threeF = race_data['horses'][h]['latest_races'][i-1]['horses'][n]['res_tf_time']
                        try:
                            corner_order_1 =  race_data['horses'][h]['latest_races'][i-1]['horses'][n]['res_corner_indexes'][0]
                        except:
                            corner_order_1 = 0
                        try:
                            corner_order_2 =  race_data['horses'][h]['latest_races'][i-1]['horses'][n]['res_corner_indexes'][1]
                        except:
                            corner_order_2 = 0
                        try:
                            corner_order_3 =  race_data['horses'][h]['latest_races'][i-1]['horses'][n]['res_corner_indexes'][2]
                        except:
                            corner_order_3 = 0


                    # 馬場状態
                    df_.loc[i, 'soil_heavy'] = 1 if course_status == '/重' else 0
                    df_.loc[i, 'soil_s_heavy'] = 1 if course_status == '稍重' else 0
                    df_.loc[i, 'soil_good'] = 1 if course_status == '/良' else 0
                    df_.loc[i, 'soil_bad'] = 1 if course_status == '不良' else 0

                    df_.loc[i, 'money'] = 0
                    df_.loc[i, 'horse_cnt'] = float(horse_cnt) / 16
                    df_.loc[i, 'len'] = (inZeroOne(float(course_distance) -800) /3000)
                    df_.loc[i, 'popularity'] = 0
                    df_.loc[i, 'borden_weight'] = inZeroOne((float(handicap) - 50) /10)
                    df_.loc[i, 'diff_accident'] = 0
                    df_.loc[i, 'birth_days'] = inZeroOne((birthDate.days - 700) / 1000)
                    # 当日データはNoneがある
                    try:
                        df_.loc[i, 'weight'] = inZeroOne((float(weight) - 300) / 300)
                    except:
                        df_.loc[i, 'weight'] = 0

                    # 　競馬場
                    df_.loc[i, 'place_Urawa'] = 1 if place == "浦和" else 0
                    df_.loc[i, 'place_Funabashi'] = 1 if place == "船橋" else 0
                    df_.loc[i, 'place_Ooi'] = 1 if place == "大井" else 0
                    df_.loc[i, 'place_Kawasaki'] = 1 if place == "川崎" else 0
                    if sum([df_.loc[i, 'place_Urawa'], df_.loc[i, 'place_Funabashi'], df_.loc[i, 'place_Ooi'],
                            df_.loc[i, 'place_Kawasaki']]) == 0:
                        df_.loc[i, 'place_other'] = 1
                    else:
                        df_.loc[i, 'place_other'] = 0

                    if i == 0:
                        # 当日データ
                        df_.loc[i, 'result_rank'] = 0
                        df_.loc[i, 'sec'] = 0
                        df_.loc[i, 'threeF'] = 0

                    else:
                        df_.loc[i, 'result_rank'] = float(result_rank) / 16
                        # 上3F（3ハロン）
                        try:
                            df_.loc[i, 'threeF'] = inZeroOne((float(threeF) - 30) / 30)
                        except ValueError:
                            df_.loc[i, 'threeF'] = 0
                        # タイム(秒)
                        try:
                            time = datetime.datetime.strptime(str(m_s_f), '%H:%M:%S.%f')
                            df_.loc[i, 'sec'] = inZeroOne((float(time.minute * 60 + time.second + time.microsecond / 1000000) - 40) / 250)
                        except:
                            time = dt.strptime(str(m_s_f), '%H:%M:%S')
                            df_.loc[i, 'sec'] = inZeroOne((float(time.minute * 60 + time.second + time.microsecond / 1000000) - 40) / 250)


                except: # エラーなら全部0
                    traceback.print_exc()
                    dropList.append(i)
                    df_.loc[i] = 0

            for i in dropList:
                df_.drop(i, axis=0, inplace=True)

            while len(df_) < 10:
                df_.loc[len(df_) + len(dropList)] = 0

            df_=df_.replace([np.inf, -np.inf], np.nan)
            df_  = missing_value_check(df_)
            pd.set_option('display.max_columns', 100)
            #print(df_.head(15))


            race_horse.append(df_[:10].values)
        else:
            race_horse.append(np.zeros((10, 20)))
    return race_horse


# 予測
def model_save_predict(X):
    X = X
    model = tf.keras.models.load_model("model/model_ver_m4.h5", compile=False)
    score = list(model.predict(X)[0])
    #np.save("PredictData/predict.npy",ys) # 予測した結果の保存
    pd.options.display.float_format = '{:.8f}'.format # 指数表記から少数表記に
    result = pd.DataFrame([], columns=['score', 'number'])
    result['score'] = score
    result['number'] = list(range(1,19))
    result = result.sort_values(by='score', ascending=False)
    score_list = result.to_dict(orient='records')
    pprint.pprint(score_list)
    return score_list

# モデル呼び出し
def model_load_predict():
    pre = np.load("PredictData/predict.npy")
    print(pre)

def missing_value_check(df):
    miss_num = df.isnull().values.sum()
    if miss_num != 0:
        print('missing_value：{}'.format(miss_num))
        #print(df)
        df = df.fillna(0)
    return df

def inZeroOne(num):
    if num > 1:
        return 1
    elif num < 0:
        return 0
    else:
        return num


def main():
    result = predict(JSON_RACE)
    print(result)


if __name__ == '__main__':
    main()
