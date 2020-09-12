import json
from collections import OrderedDict
import pprint

JSON_RACE = './sample.json'

def predict(race):
    with open(race, encoding="utf-8_sig") as f:
        race_json = json.load(f)
    today_data, today_data_horse = today_race(race_json)
    print(today_data)
    print(today_data_horse)
    latest_races(race_json)
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
    number, weight, handicap, jockey, trainer = [], [], [], [], []
    #pprint.pprint(race_data['horses'][i]['weight'],  width=60) #p
    horse_cnt = len(race_data['horses'])
    for i in range(horse_cnt):
        number.append(i + 1)
        weight.append(race_data['horses'][i]['weight'])
        handicap.append(race_data['horses'][i]['handicap'])
        #jockey.append(race_data['horses'][i]['jockey'])
        #trainer.append(race_data['horses'][i]['horse']['trainer'])
    #list
    today_data_horse.append(horse_cnt)
    today_data_horse.append(number)
    today_data_horse.append(weight)
    today_data_horse.append(handicap)
    #today_data_horse.append(jockey)
    #today_data_horse.append(trainer)
    return today_data, today_data_horse

def latest_races(race_json):
    race_data = race_json.copy()
    i = 0
    #print(len(race_data['horses'][i]['latest_races']))
    race_data['horses'][i]['latest_races'].sort(key=lambda x: x['date'], reverse=True) # 日付降順
    pprint.pprint(race_data['horses'][i]['latest_races'][0]['date'],  width=60)


def main():
    predict(JSON_RACE)


if __name__ == '__main__':
    main()
