import glob
import datetime
import time
import datetime
import numpy as np
import pandas as pd
import os
import re
import traceback
from concurrent import futures

import sys
sys.path.append('../')
import settings
SAVE_FILE_PATH = settings.SAVE_FILE_PATH2 # 本番時は数字を取る



def read_csv(race, date):
    print(os.path.basename(race))
    horses = glob.glob(race + "/*.csv")
    horses = sorted(horses, key=lambda x: int(re.findall("\d+", os.path.basename(x))[0]))
    race_horse = []
    rankings = np.zeros(18)
    for i in range(18):
        if len(horses) > i:

            birth = [int(x) for x in re.findall("\d+", horses[i])[-3:]]
            df = pd.read_csv(horses[i], encoding="cp932")
            df, ranking = make_race_data(df, date, birth, 10)
            print('ranking:{}'.format(ranking))

            if ranking != 0:  # 欠場等でないなら
                if rankings[ranking - 1] == 0:
                    rankings[ranking - 1] = int(re.findall("\d+", os.path.basename(horses[i]))[0])
                else:
                    rankings[ranking] = int(re.findall("\d+", os.path.basename(horses[i]))[0])

            race_horse.append(df[:10].values)
        else:
            race_horse.append(np.zeros((10, 16)))

    return race_horse, rankings



def make_npy():
    races = glob.glob(SAVE_FILE_PATH + '*')
    for i in range(len(races)):
        year, month, day, roundNumber, length, roadState, top = os.path.basename(races[i]).split("-")
        #  下級レースの除外
        if int(roundNumber) <= settings.EXCLUDE_LOWER_RACE:
            continue
        race_horse, rankings = read_csv(race=races[i], date=[year, month, day])


"""
    X = [future.result()[0] for future in future_list]
    Y = [future.result()[1] for future in future_list]

    X = np.array(X)
    Y = np.array(Y)
    X = X.astype("float")
"""

def inZeroOne(num):
    if num > 1:
        return 1
    elif num < 0:
        return 0
    else:
        return num

def make_race_data(df, date, birth, l=10):
    df_ = pd.DataFrame(np.zeros((1, 16)), columns=["racecourse", "course_type", "R", "len", "soil_condition", "horse_cnt", "horse_number", "result_rank",
                                                                             "weight", "borden_weight", "birth_days", "sec", "threeF", "corner_order_1", "corner_order_2", "corner_order_3"])
    weightLog = 0
    dropList = []
    check = False
    ranking = 0
    for idx, row in df.iterrows():
        check = True
        if str(row['着順']) == "nan" or str(row['タイム']) == "nan":
            dropList.append(idx)
            df_.loc[idx] = 0
            continue

        try:
            # 馬場状態
            if row['馬場状態'] == '良':
                df_.loc[idx, 'soil_condition'] = float(0.25)
            elif row['馬場状態'] == '重':
                df_.loc[idx, 'soil_condition'] = float(0.5)
            elif row['馬場状態'] == '稍':
                df_.loc[idx, 'soil_condition'] = float(0.75)
            elif row['馬場状態'] == '不':
                df_.loc[idx, 'soil_condition'] = float(1)
            else:
                df_.loc[idx, 'soil_condition'] = 0

            # コース種別
            if row['コース種別'] == 'ダ':
                df_.loc[idx, 'course_type'] = float(0.5)
            elif row['コース種別'] == '芝':
                df_.loc[idx, 'course_type'] = float(1)
            else:
                df_.loc[idx, 'course_type'] = 0

            df_.loc[idx, 'R'] = float(str(row['R'].replace('R', ''))) / 12
            df_.loc[idx, 'len'] = inZeroOne((float(str(row['距離']).replace('m', '')) - 800) / 3000)
            df_.loc[idx, 'horse_cnt'] = float(0.5) # 値を取得
            df_.loc[idx, 'horse_number'] = float(str(row['馬番'])) / 18
            df_.loc[idx, 'result_rank'] = float(row['着順']) / 18
            df_.loc[idx, 'borden_weight'] = inZeroOne((float(row['斤量']) - 50) / 10)
            if row['体重'] == "計不":
                df_.loc[idx, 'weight'] = weightLog
            else:
                df_.loc[idx, 'weight'] = inZeroOne((float(row['体重']) - 300) / 300)
                weightLog = inZeroOne((float(row['体重']) - 300) / 300)

            # タイム(秒)
            m_s_f = datetime.timedelta(seconds=row['タイム'])
            print(m_s_f)
            try:
                time = datetime.datetime.strptime(str(m_s_f), '%H:%M:%S.%f')
                df_.loc[idx, 'sec'] = inZeroOne(
                    (float(time.minute * 60 + time.second + time.microsecond / 1000000) - 40) / 250)
            except:
                time = datetime.datetime.strptime(str(m_s_f), '%H:%M:%S')
                df_.loc[idx, 'sec'] = inZeroOne(
                    (float(time.minute * 60 + time.second + time.microsecond / 1000000) - 40) / 250)
            print('time;{}'.format(time))


            # 上3F（3ハロン）
            try:
                df_.loc[idx, 'threeF'] = inZeroOne((float(row['3Fタイム']) - 30) / 30)
            except ValueError:
                df_.loc[idx, 'threeF'] = 0

            # コーナー通過順
            try:
                df_.loc[idx, 'corner_order_1'] = float(row['コーナー通過順'][1:-1].split(',')[0]) / 18
            except:
                df_.loc[idx, 'corner_order_1'] = 0
            try:
                df_.loc[idx, 'corner_order_2'] = float(row['コーナー通過順'][1:-1].split(',')[1]) / 18
            except:
                df_.loc[idx, 'corner_order_2'] = 0
            try:
                df_.loc[idx, 'corner_order_3'] = float(row['コーナー通過順'][1:-1].split(',')[2]) / 18
            except:
                df_.loc[idx, 'corner_order_3'] = 0

            # 　競馬場
            if row['競馬場'] == "浦和":
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            elif row['競馬場'] == "船橋":
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            elif row['競馬場'] == "川崎":
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            elif row['競馬場'] == "東京":
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            elif row['競馬場'] == "中京": # 左回り
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            elif row['競馬場'] == "大井":
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            elif row['競馬場'] == "新潟":
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            elif row['競馬場'] == "函館":
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            elif row['競馬場'] == "阪神":
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            elif row['競馬場'] == "中山":
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            elif row['競馬場'] == "京都":
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            elif row['競馬場'] == "小倉":
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            elif row['競馬場'] == "札幌":
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            elif row['競馬場'] == "福島":
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            elif row['競馬場'] == "盛岡": # 左周り
                df_.loc[idx, 'racecourse'] = float(1 / 15)
            else:
                df_.loc[idx, 'racecourse'] = 0

            # レース日
            print(str(row['年月日']))
            raceDay = [int(x) for x in str(row['年月日']).split("-")]
            date = [int(x) for x in date]
            if raceDay[0] < 50:
                raceDay[0] += 2000
            elif raceDay[0] < 1900:
                raceDay[0] += 1900

            birthDate = datetime.date(raceDay[0], raceDay[1], raceDay[2]) - datetime.date(birth[0], birth[1], birth[2])
            df_.loc[idx, 'birth_days'] = inZeroOne((birthDate.days - 700) / 1000)

            # 当日の場合不明なもの
            if raceDay == date:
                ranking = row['着順']
                df_.loc[idx, 'result_rank'] = 0
                df_.loc[idx, 'sec'] = 0
                df_.loc[idx, 'weight'] = 0
                df_.loc[idx, 'threeF'] = 0
                df_.loc[idx, 'corner_order_1'] = 0
                df_.loc[idx, 'corner_order_2'] = 0
                df_.loc[idx, 'corner_order_3'] = 0

        except:  # エラーなら全部0
            traceback.print_exc()
            dropList.append(idx)
            df_.loc[idx] = 0

    for i in dropList:
        df_.drop(i, axis=0, inplace=True)
    if not check:
        df_.drop(0, axis=0, inplace=True)

    while len(df_) < l:
        df_.loc[len(df_) + len(dropList)] = 0

    print(df_.head(10))
    return df_, ranking



def main():
    make_npy()



if __name__ == '__main__':
    main()
