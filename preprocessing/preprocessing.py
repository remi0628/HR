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
            read_csv_2(race=races[i], date=[year, month, day])
            future = executor.submit(fn=read_csv, race=races[i], date=[year, month, day])
            future_list.append(future)
        _ = futures.as_completed(fs=future_list)

    X = [future.result()[0] for future in future_list]
    Y = [future.result()[1] for future in future_list]

    X = np.array(X)
    Y = np.array(Y)
    X = X.astype("float")

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

def main():
    make_npy()



if __name__ == '__main__':
    main()
