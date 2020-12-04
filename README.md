# Horse Racing AI
* 地方競馬含め全競馬場データを扱って学習させたAI 　正答率：約22%  

## Data
* 今回はデータの量が多く欠損値も見受けられたので学習に使用できなさそうなデータは省くよう設定
<img width="400" alt="" src="https://user-images.githubusercontent.com/16487150/101145391-0f363000-365d-11eb-8294-0e9ed9a99124.jpg">

## Preprocessing
* カラムは30個に設定
<img width="1000" alt="" src="https://user-images.githubusercontent.com/16487150/101145156-bbc3e200-365c-11eb-82e6-edbed8ed7daf.jpg">

## json file
* 予想させたい当日のレースデータ(json)  
前処理を行いDataFrameに落とし込みます。
<img width="200" alt="" src="https://user-images.githubusercontent.com/16487150/101144778-2e808d80-365c-11eb-8d49-79d5741730dd.jpg">
<img width="400" alt="" src="https://user-images.githubusercontent.com/16487150/101144977-74d5ec80-365c-11eb-8003-5ab9383ec8ad.jpg">

## Results
* 結果は以下の画像の様に出力  
上から1位になる確率が高い馬番を値と一緒に表示しています。  
![HR1](https://user-images.githubusercontent.com/16487150/101144676-07c25700-365c-11eb-9e2e-c236f1818076.jpg)
