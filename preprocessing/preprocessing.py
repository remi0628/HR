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

            race_horse.append(df[:10].values)
        else:
            race_horse.append(np.zeros((10, 20)))

    return race_horse, rankings



def make_npy():
    races = glob.glob(SAVE_FILE_PATH + '*')
    future_list = []
    with futures.ProcessPoolExecutor(max_workers=None) as executor:
        for i in range(len(races)):
            year, month, day, roundNumber, length, roadState, top = os.path.basename(races[i]).split("-")
            #  下級レースの除外
            if int(roundNumber) <= settings.EXCLUDE_LOWER_RACE:
                continue
            future = executor.submit(fn=read_csv, race=races[i], date=[year, month, day])
"""
            future_list.append(future)
        _ = futures.as_completed(fs=future_list)

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

            df_.loc[idx, 'R'] = float(str(row['R'].replace('R', '')))
            df_.loc[idx, 'len'] = inZeroOne((float(re.findall("\d+", str(row['距離']))) - 800) / 3000)
            df_.loc[idx, 'horse_cnt'] = float(0.5)
            df_.loc[idx, 'horse_number'] = float(str(row['馬番']))
            df_.loc[idx, 'result_rank'] = float(row['着順'])
            df_.loc[idx, 'borden_weight'] = inZeroOne((float(row['斤量']) - 50) / 10)
            if row['体重'] == "計不":
                df_.loc[idx, 'weight'] = weightLog
            else:
                df_.loc[idx, 'weight'] = inZeroOne((float(row['体重']) - 300) / 300)
                weightLog = inZeroOne((float(row['体重']) - 300) / 300)

            # タイム(秒)
            try:
                time = datetime.datetime.strptime(str(row['タイム']), '%M:%S.%f')
                df_.loc[idx, 'sec'] = inZeroOne(
                    (float(time.minute * 60 + time.second + time.microsecond / 1000000) - 40) / 250)
            except:
                time = datetime.datetime.strptime(str(row['タイム']), '%S.%f')
                df_.loc[idx, 'sec'] = inZeroOne((float(time.second + time.microsecond / 1000000) - 40) / 250)

            # 上3F（3ハロン）
            try:
                df_.loc[idx, 'threeF'] = inZeroOne((float(row['3Fタイム']) - 30) / 30)
            except ValueError:
                df_.loc[idx, 'threeF'] = 0

            # 　競馬場
            if row['競馬場'] == "浦和":
                df_.loc[idx, 'racecourse'] = float(0.25)
            elif row['競馬場'] == "船橋":
                df_.loc[idx, 'racecourse'] = float(0.5)
            elif row['競馬場'] == "大井":
                df_.loc[idx, 'racecourse'] = float(0.75)
            elif row['競馬場'] == "川崎":
                df_.loc[idx, 'racecourse'] = float(1)
            else:
                df_.loc[idx, 'racecourse'] = 0

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

    return df_, ranking



def main():
    make_npy()



if __name__ == '__main__':
    main()
