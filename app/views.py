from django.views.generic import View
from django.shortcuts import render
from apiclient.discovery import build
from datetime import datetime, timedelta, date
from django.conf import settings
import pandas as pd
import math


class IndexView(View):
    def get(self, request, *args, **kwargs):

        # 今から24時間前の時刻-utc時刻
        fromtime = (datetime.utcnow()-timedelta(days=90)).strftime('%Y-%m-%dT%H:%M:%SZ')
        # videourlの設定
        videourl = 'https://www.youtube.com/watch?v='
        embedurl = 'https://www.youtube.com/embed/'
        # データを入れる空のリストを作成
        data = []
        youtube = build('youtube', 'v3', developerKey=settings.YOUTUBE_API_KEY)
        keyword = '中田敦彦'

        # youtube.search
        result = youtube.search().list(
            part='snippet',
            # 検索したい文字列を指定
            q=keyword,
            # 1回の試行における最大の取得数
            maxResults=12,
            # 視聴回数が多い順に取得
            order='viewCount',
            # いつから情報を検索するか？
            publishedAfter=fromtime,
            # 動画タイプ
            type='video',
            # 地域コード
            regionCode='JP',
        ).execute()

        for item in result['items']:
            published_at = datetime.strptime(item['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d')
            data.append([
                item['id']['videoId'], # 動画ID
                item['snippet']['channelId'], # チャンネルID
                published_at, # 動画公開日時
                item['snippet']['title'], # 動画タイトル
                item['snippet']['channelTitle'], # チャンネル名
                keyword # 検索キーワード
            ])

        videoid_list = {}
        for item in data:
            videoid_list[item[0]] = item[1]

        # videoid_list = sorted(set(videoid_list), key=videoid_list.index)

        # # 50のセットの数(次のデータ取得で最大50ずつしかデータが取れないため、50のセットの数を数えている)
        # _set_50 = math.ceil(len(data) / 50)

        # _id_list = []
        # for i in range(_set_50):
        #     _id_list.append(','.join(videoid_list[i*50:(i*50+50)]))


        # チャンネルデータを取得
        channel_list = []
        for videoid, channelid in videoid_list.items():
            # youtube.channels
            result = youtube.channels().list(
                part='snippet',
                id=channelid,
            ).execute()

            for i in result['items']:
                channel_list.append([
                    videoid, # 動画ID
                    i['snippet']['thumbnails']['default']['url'], # プロフィール画像
                ])

        # 再生回数データを取得
        viewcount_list = []
        for videoid, channelid in videoid_list.items():
            # youtube.videos
            viewcount = youtube.videos().list(
                part='statistics',
                maxResults=50,
                id=videoid
            ).execute()

            for i in viewcount['items']:
                viewcount_list.append([
                    i['id'], # 動画ID
                    i['statistics']['viewCount'], # 視聴回数
                    i['statistics']['likeCount'], # 高評価数
                    i['statistics']['dislikeCount'], # 低評価数
                    i['statistics']['commentCount'], # コメント数
                ])

        # # コメントデータを取得
        # comment_list = []

        # for videoid, channelid in videoid_list.items():
        #     # youtube.videos
        #     comments = youtube.commentThreads().list(
        #         part='snippet',
        #         maxResults=3,
        #         textFormat='plainText',
        #         order='time',
        #         videoId=videoid
        #     ).execute()

        #     for i in comments['items']:
        #         comment_list.append([
        #             i['snippet']['videoId'], # 動画ID
        #             i['snippet']['topLevelComment']['snippet']['textDisplay'], # コメント内容
        #             i['snippet']['topLevelComment']['snippet']['authorDisplayName'], # ユーザー名
        #             i['snippet']['topLevelComment']['snippet']['likeCount'], # 高評価数
        #             i['snippet']['topLevelComment']['snippet']['publishedAt'], # 書き込まれた日時
        #         ])

        # データフレームの作成
        youtube_data = pd.DataFrame(data, columns=['videoid', 'channelId', 'publishtime', 'title', 'channeltitle', 'keyword'])
        # 重複の削除 subsetで重複を判定する列を指定,inplace=Trueでデータフレームを新しくするかを指定
        youtube_data.drop_duplicates(subset='videoid', inplace=True)
        # 動画のURL
        # youtube_data['url'] = videourl + youtube_data['videoid']
        youtube_data['url'] = embedurl + youtube_data['videoid']
        # 調査した日
        youtube_data['search_day'] = date.today().strftime('%Y-%m-%d')

        df_channel = pd.DataFrame(channel_list, columns=['videoid', 'profileImg'])

        df_viewcount = pd.DataFrame(viewcount_list, columns=['videoid', 'viewcount', 'likeCount', 'dislikeCount', 'commentCount'])

        # df_comment = pd.DataFrame(comment_list, columns=['videoid', 'textDisplay', 'authorDisplayName', 'commentLikeCount', 'commentPublishedAt'])
        # df_comment = df_comment.groupby('videoid').agg(
        #     {
        #         'textDisplay': list,
        #         'authorDisplayName': list,
        #         'commentLikeCount': list,
        #         'commentPublishedAt': list
        #     }
        # )

        # 2つのデータフレームのマージ
        youtube_data = pd.merge(df_channel, youtube_data, on='videoid', how='left')
        youtube_data = pd.merge(df_viewcount, youtube_data, on='videoid', how='left')
        # youtube_data = pd.merge(df_comment, youtube_data, on='videoid', how='left')

        # viewcountの列のデータを条件検索のためにint型にする(元データも変更)
        youtube_data['viewcount'] = youtube_data['viewcount'].astype(int)
        # データフレームの条件を満たす行だけを抽出
        youtube_data = youtube_data.query('viewcount>=100')

        youtube_data = youtube_data[[
            'search_day',
            'publishtime',
            'keyword',
            'title',
            'channeltitle',
            'url',
            'profileImg',
            'viewcount',
            'likeCount',
            'dislikeCount',
            'commentCount',
            # 'textDisplay',
            # 'authorDisplayName',
            # 'commentLikeCount',
            # 'commentPublishedAt',
        ]]

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
