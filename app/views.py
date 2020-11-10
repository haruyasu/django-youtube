from django.views.generic import View
from django.shortcuts import render
from apiclient.discovery import build
from datetime import datetime, timedelta, date
from django.conf import settings
from django.utils.timezone import localtime
import pandas as pd
import math


class IndexView(View):
    def get(self, request, *args, **kwargs):

        # 今から24時間前の時刻-utc時刻
        fromtime = (datetime.utcnow()-timedelta(hours=168)).strftime('%Y-%m-%dT%H:%M:%SZ')
        # videourlの設定
        videourl = 'https://www.youtube.com/watch?v='
        embedurl = 'https://www.youtube.com/embed/'
        # データを入れる空のリストを作成
        data = []
        youtube = build('youtube', 'v3', developerKey=settings.YOUTUBE_API_KEY)
        keyword = 'ヒカキン'

        # youtube.search
        result = youtube.search().list(
            part='snippet',
            # 検索したい文字列を指定
            q=keyword,
            # 1回の試行における最大の取得数
            maxResults=1,
            # 視聴回数が多い順に取得
            order='viewCount',
            # いつから情報を検索するか？
            publishedAfter=fromtime,
            # 動画タイプ
            type='video',
            # 地域コード
            regionCode='JP',
        ).execute()

        for i in result['items']:
            print(i['snippet']['publishedAt'])
            published_at = datetime.strptime(i['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d')
            print(published_at)
            data.append([i['id']['videoId'], published_at, i['snippet']['title'], i['snippet']['channelTitle'], keyword])

        videoid_list = []
        for i in data:
            videoid_list.append(i[0])

        videoid_list = sorted(set(videoid_list), key=videoid_list.index)

        # 50のセットの数(次のデータ取得で最大50ずつしかデータが取れないため、50のセットの数を数えている)
        _set_50 = math.ceil(len(data) / 50)

        _id_list = []
        for i in range(_set_50):
            _id_list.append(','.join(videoid_list[i*50:(i*50+50)]))

        # 再生回数データを取得
        viewcount_list = []
        for videoid in _id_list:
            # youtube.videos
            viewcount = youtube.videos().list(
                part='statistics',
                maxResults=50,
                id=videoid
            ).execute()

            for i in viewcount['items']:
                viewcount_list.append([i['id'], i['statistics']['viewCount']])

        # データフレームの作成
        youtube_data = pd.DataFrame(data, columns=['videoid', 'publishtime', 'title', 'channeltitle', 'keyword'])
        # 重複の削除 subsetで重複を判定する列を指定,inplace=Trueでデータフレームを新しくするかを指定
        youtube_data.drop_duplicates(subset='videoid', inplace=True)
        # 動画のURL
        # youtube_data['url'] = videourl + youtube_data['videoid']
        youtube_data['url'] = embedurl + youtube_data['videoid']
        # 調査した日
        youtube_data['search_day'] = date.today().strftime('%Y-%m-%d')
        df_viewcount = pd.DataFrame(viewcount_list, columns=['videoid', 'viewcount'])

        # 2つのデータフレームのマージ
        youtube_data = pd.merge(df_viewcount, youtube_data, on='videoid', how='left')
        # viewcountの列のデータを条件検索のためにint型にする(元データも変更)
        youtube_data['viewcount'] = youtube_data['viewcount'].astype(int)
        # データフレームの条件を満たす行だけを抽出
        youtube_data = youtube_data.query('viewcount>=10000')

        youtube_data = youtube_data[['search_day', 'publishtime', 'keyword', 'title', 'channeltitle', 'url', 'viewcount']]
        youtube_data['viewcount'] = youtube_data['viewcount'].astype(str)

        # search_response = youtube.search().list(
        #     part='snippet',
        #     q='ボードゲーム',
        #     order='viewCount',
        #     type='video',
        # ).execute()

        # print(search_response)

        return render(request, 'app/index.html', {
            'youtube_data': youtube_data
        })
