import json
import pprint
import numpy as np
import pandas as pd
from collections import OrderedDict
from datetime import datetime as dt

JSON_RACE = './sample.json'

def predict(race):
    with open(race, encoding="utf-8_sig") as f:
        race_json = json.load(f)
    today_data, today_data_horse = today_race(race_json)
    print(today_data)
    print(today_data_horse)
    latest_races(race_json, today_data, today_data_horse)
    #pprint.pprint(race_json['course_distance'], width=60)


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
    i = 0
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
    #list [cnt, number, weight, handicap, birthday]
    today_data_horse.append(horse_cnt)
    today_data_horse.append(number)
    today_data_horse.append(weight)
    today_data_horse.append(handicap)
    today_data_horse.append(birthday)
    #today_data_horse.append(jockey)
    #today_data_horse.append(trainer)
    return today_data, today_data_horse

def latest_races(race_json, today_data, today_data_horse):
    race_data = race_json.copy()
    i = 0
    #print(len(race_data['horses'][i]['latest_races']))
    race_data['horses'][i]['latest_races'].sort(key=lambda x: x['date'], reverse=True) # 日付降順
    pprint.pprint(race_data['horses'][i]['latest_races'][0]['date'],  width=60)

    df_ = pd.DataFrame(np.zeros((1, 30)), columns=["course_type", "R", "len", "soil_condition", "horse_cnt", "horse_number", "result_rank",
                                                                             "weight", "borden_weight", "birth_days", "sec", "threeF", "corner_order_1", "corner_order_2", "corner_order_3",
                                                                             "racecourse_urawa", "racecourse_funabashi", "racecourse_kawasaki", "racecourse_tokyo", "racecourse_nigata",
                                                                             "racecourse_tyukei", "racecourse_ooi", "racecourse_hakodate", "racecourse_hanshin", "racecourse_nakayama",
                                                                             "racecourse_kyoto", "racecourse_ogura", "racecourse_sapporo", "racecourse_fukushima", "racecourse_morioka"])
    print(type(today_data_horse[4][i]))
    if i == 0:
        if today_data['course_status'] == '良':
            df_.loc[i, 'soil_condition'] = float(0)
        elif today_data['course_status'] == '稍':
            df_.loc[i, 'soil_condition'] = float(0.25)
        elif today_data['course_status'] == '重':
            df_.loc[i, 'soil_condition'] = float(0.75)
        elif today_data['course_status'] == '不':
            df_.loc[i, 'soil_condition'] = float(1)
        else:
            df_.loc[i, 'soil_condition'] = float(0.5)

        if today_data['course_type'] == 'ダ':
            df_.loc[i, 'course_type'] = float(0)
        elif today_data['course_type'] == '芝':
            df_.loc[i, 'course_type'] = float(1)
        else:
            df_.loc[i, 'course_type'] = float(0.5)

        df_.loc[i, 'R'] = float(str(today_data['r'].replace('R', ''))) / 12
        df_.loc[i, 'len'] = inZeroOne((float(str(today_data['course_distance']).replace('m', '')) - 800) / 3000)
        df_.loc[i, 'horse_cnt'] = today_data_horse[0] / 18
        df_.loc[i, 'horse_number'] = float(today_data_horse[1][i]) / 18
        df_.loc[i, 'borden_weight'] = inZeroOne((float(today_data_horse[3][i]) - 40) / 30)
        df_.loc[i, 'weight'] = inZeroOne((float(today_data_horse[2][i]) - 300) / 300)
        today = dt.strptime(today_data['date'], '%Y-%m-%d')
        birth_day = dt.strptime(today_data_horse[4][i], '%Y-%m-%d')
        birthDate = today - birth_day
        df_.loc[i, 'birth_days'] = inZeroOne((birthDate.days - 700) / 1000)

        df_.loc[i, 'racecourse_urawa'] = 1 if today_data['place'] == "浦和" else 0
        df_.loc[i, 'racecourse_funabashi'] = 1 if today_data['place'] == "船橋" else 0
        df_.loc[i, 'racecourse_kawasaki'] = 1 if today_data['place'] == "川崎" else 0
        df_.loc[i, 'racecourse_tokyo'] = 1 if today_data['place'] == "東京" else 0
        df_.loc[i, 'racecourse_nigata'] = 1 if today_data['place'] == "新潟" else 0
        df_.loc[i, 'racecourse_tyukei'] = 1 if today_data['place'] == "中京" else 0 # ここまで左回り
        df_.loc[i, 'racecourse_ooi'] = 1 if today_data['place'] == "大井" else 0
        df_.loc[i, 'racecourse_hakodate'] = 1 if today_data['place'] == "函館" else 0
        df_.loc[i, 'racecourse_hanshin'] = 1 if today_data['place'] == "阪神" else 0
        df_.loc[i, 'racecourse_nakayama'] = 1 if today_data['place'] == "中山" else 0
        df_.loc[i, 'racecourse_kyoto'] = 1 if today_data['place'] == "京都" else 0
        df_.loc[i, 'racecourse_ogura'] = 1 if today_data['place'] == "小倉" else 0
        df_.loc[i, 'racecourse_sapporo'] = 1 if today_data['place'] == "札幌" else 0
        df_.loc[i, 'racecourse_fukushima'] = 1 if today_data['place'] == "福島" else 0
        df_.loc[i, 'racecourse_morioka'] = 1 if today_data['place'] == "盛岡" else 0 # 左周り

        df_.loc[i, 'result_rank'] = 0
        df_.loc[i, 'sec'] = 0
        df_.loc[i, 'threeF'] = 0
        df_.loc[i, 'corner_order_1'] = 0
        df_.loc[i, 'corner_order_2'] = 0
        df_.loc[i, 'corner_order_3'] = 0


    pd.set_option('display.max_columns', 100)
    print(df_.head(1))

def inZeroOne(num):
    if num > 1:
        return 1
    elif num < 0:
        return 0
    else:
        return num


def main():
    predict(JSON_RACE)


if __name__ == '__main__':
    main()
