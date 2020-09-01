import glob
import shutil
import datetime
import time
import numpy as np
import pandas as pd
import os
import re
import traceback

import sys
sys.path.append('../')
import settings
SAVE_FILE_PATH = settings.SAVE_FILE_PATH2

log_path = '../log/'
del_flag, num = 0, 0

# データが入っていない馬のレースフォルダを探して削除する 削除したフォルダは[no_data_folder.txt]に記載
def delete_file():
    global del_flag, num
    non_data_num = 0
    races = glob.glob(SAVE_FILE_PATH + '*')
    for i in range(len(races)):
        del_flag, non_data_num = read_file(races[i])
        if del_flag == 1: # データがない馬が存在するレースフォルダは削除
            num += 1
            path = SAVE_FILE_PATH + os.path.basename(races[i])
            print('データ無しファイル：{}件'.format(non_data_num))
            print('{} : Folder Exclusion.'.format(path))
            shutil.rmtree(path)

# フォルダ内を読み込みデータがあるか確認
def read_file(race):
    flag, num = 0, 0
    print(os.path.basename(race))
    horses = glob.glob(race + "/*.csv")
    horses = sorted(horses, key=lambda x: int(re.findall("\d+", os.path.basename(x))[0]))
    for i in range(len(horses)):
        df = pd.read_csv(horses[i], encoding="cp932")
        if len(df.loc[:,['年月日']]) == 0:
            flag = 1
            num += 1
    if flag == 1:
        log_output('no_data_folder.txt', os.path.basename(race + ':' + str(num) + '件'))
    return flag, num

# 初期化
def file_init():
    if(os.path.exists(log_path + 'no_data_folder.txt')):
        os.remove(log_path + 'no_data_folder.txt')
# error_log
def log_output(file_name, id=None):
    global log_path
    file = log_path + file_name
    if not (os.path.exists(file)):
        with open(file, mode='w') as f:
            f.write(str(id) + '\n' )
    with open(file, mode='a') as f:
        f.write(str(id) + '\n' )




def operation_check():
    print('-------------------------------------------------')
    print('消去したフォルダ数：{}件'.format(num))
    print('-------------------------------------------------')


def main():
    now = time.time()
    file_init()
    delete_file()
    operation_check()
    print("不要レースデータ処理時間 ：{:.2f}秒".format(time.time() - now) )



if __name__ == '__main__':
    main()
