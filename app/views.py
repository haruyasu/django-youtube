from django.views.generic import View
from django.shortcuts import render, redirect
from apiclient.discovery import build
from datetime import datetime, timedelta, date
from django.conf import settings
from .forms import YoutubeForm
import pandas as pd

YOUTUBE_API = build('youtube', 'v3', developerKey=settings.YOUTUBE_API_KEY)


# キーワード動画検索
def search_video(keyword, items_count, order, search_start, search_end):
    # youtube.search
    result = YOUTUBE_API.search().list(
        part='snippet',
        # 検索したい文字列を指定
        q=keyword,
        # 1回の試行における最大の取得数
        maxResults=items_count,
        # 視聴回数が多い順に取得
        order=order,
        # 検索開始日
        publishedAfter=search_start.strftime('%Y-%m-%dT%H:%M:%SZ'),
        # 検索終了日
        publishedBefore=search_end.strftime('%Y-%m-%dT%H:%M:%SZ'),
        # 動画タイプ
        type='video',
        # 地域コード
        regionCode='JP',
    ).execute()

    # 検索データを取得
    search_list = []
    for item in result['items']:
        published_at = datetime.strptime(item['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d')
        search_list.append([
            item['id']['videoId'], # 動画ID
            item['snippet']['channelId'], # チャンネルID
            published_at, # 動画公開日時
            item['snippet']['title'], # 動画タイトル
            item['snippet']['channelTitle'], # チャンネル名
        ])
    return search_list


# チャンネルデータ取得
def get_channel(videoid_list):
    channel_list = []
    for videoid, channelid in videoid_list.items():
        # youtube.channels
        result = YOUTUBE_API.channels().list(
            part='snippet',
            id=channelid,
        ).execute()

        for item in result['items']:
            channel_list.append([
                videoid, # 動画ID
                item['snippet']['thumbnails']['default']['url'], # プロフィール画像
            ])
    return channel_list


# 動画データ取得
def get_video(videoid_list):
    count_list = []
    for videoid, channelid in videoid_list.items():
        # youtube.videos
        result = YOUTUBE_API.videos().list(
            part='statistics',
            maxResults=50,
            id=videoid
        ).execute()

        for item in result['items']:
            try:
                likeCount = item['statistics']['likeCount']
                dislikeCount = item['statistics']['dislikeCount']
                commentCount = item['statistics']['commentCount']
            except KeyError: # 高評価数、低評価数、コメント数が公開されてない場合
                likeCount = '-'
                dislikeCount = '-'
                commentCount = '-'

            count_list.append([
                item['id'], # 動画ID
                item['statistics']['viewCount'], # 視聴回数
                likeCount, # 高評価数
                dislikeCount, # 低評価数
                commentCount, # コメント数
            ])
    return count_list


# 動画データをデータフレーム化する
def make_df(search_list, channel_list, count_list, viewcount):
    # データフレームの作成
    youtube_data = pd.DataFrame(search_list, columns=[
        'videoid',
        'channelId',
        'publishtime',
        'title',
        'channeltitle'
    ])

    # 重複の削除 subsetで重複を判定する列を指定,inplace=Trueでデータフレームを新しくするかを指定
    youtube_data.drop_duplicates(subset='videoid', inplace=True)

    # 埋め込み動画のURL
    youtube_data['url'] = 'https://www.youtube.com/embed/' + youtube_data['videoid']

    # データフレームの作成
    df_channel = pd.DataFrame(channel_list, columns=[
        'videoid',
        'profileImg'
    ])
    df_viewcount = pd.DataFrame(count_list, columns=[
        'videoid',
        'viewcount',
        'likeCount',
        'dislikeCount',
        'commentCount'
    ])

    # 2つのデータフレームのマージ
    youtube_data = pd.merge(df_channel, youtube_data, on='videoid', how='left')
    youtube_data = pd.merge(df_viewcount, youtube_data, on='videoid', how='left')

    # viewcountの列のデータを条件検索のためにint型にする(元データも変更)
    youtube_data['viewcount'] = youtube_data['viewcount'].astype(int)

    # データフレームの条件を満たす行だけを抽出
    youtube_data = youtube_data.query('viewcount>=' + str(viewcount))

    youtube_data = youtube_data[[
        'publishtime',
        'title',
        'channeltitle',
        'url',
        'profileImg',
        'viewcount',
        'likeCount',
        'dislikeCount',
        'commentCount',
    ]]

    youtube_data['viewcount'] = youtube_data['viewcount'].astype(str)

    return youtube_data


# キーワード動画検索
def search_rivalvideo(channelid_list, search_start, search_end):
    # ライバルのチャンネルを検索した動画を入れるリスト
    rivalvideo_list = []

    for channelid in channelid_list:
        # youtube.search
        result = YOUTUBE_API.search().list(
            part='snippet',
            # ライバルのチャンネルIDを指定
            channelId=channelid,
            # 1回の試行における最大の取得数
            maxResults=2,
            # 視聴回数が多い順に取得
            order='viewCount',
            # 検索開始日
            publishedAfter=search_start.strftime('%Y-%m-%dT%H:%M:%SZ'),
            # 検索終了日
            publishedBefore=search_end.strftime('%Y-%m-%dT%H:%M:%SZ'),
            # 動画タイプ
            type='video',
            # 地域コード
            regionCode='JP',
        ).execute()

        for item in result['items']:
            published_at = datetime.strptime(item['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d')
            rivalvideo_list.append([
                item['id']['videoId'], # 動画ID
                item['snippet']['channelId'], # チャンネルID
                item['snippet']['channelTitle'], # チャンネル名
                item['snippet']['title'], # 動画タイトル
                published_at, # 動画公開日時
            ])

    return rivalvideo_list


def search_relatedvideo(rivalvideo_list, myChannelId):
    related_list = []
    for rivalvideo in rivalvideo_list:
        result = YOUTUBE_API.search().list(
            part='snippet',
            # ライバルの動画IDを指定
            relatedToVideoId=rivalvideo[0],
            # 1回の試行における最大の取得数
            maxResults=4,
            # 視聴回数が多い順に取得
            order='viewCount',
            # 動画タイプ
            type='video',
            # 地域コード
            regionCode='JP',
        ).execute()

        for item in result['items']:
            if item['snippet']['channelId'] == myChannelId:
                published_at = datetime.strptime(item['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d')
                related_list.append([
                    str(result['items'].index(item)) + '番目',
                    item['id']['videoId'], # 動画ID
                    item['snippet']['channelId'], # チャンネルID
                    item['snippet']['channelTitle'], # チャンネル名
                    item['snippet']['title'], # 動画タイトル
                    published_at, # 動画公開日時
                    rivalvideo[0], # ライバル動画ID
                    rivalvideo[1], # ライバルチャンネルID
                    rivalvideo[2], # ライバルチャンネル名
                    rivalvideo[3], # ライバル動画タイトル
                    rivalvideo[4], # ライバル動画公開日時
                ])
    return related_list


class IndexView(View):
    def get(self, request, *args, **kwargs):
        # 検索フォーム
        form = YoutubeForm(
            request.POST or None,
            # フォームに初期値を設定
            initial={
                'items_count': 12, # 検索数
                'viewcount': 1000, # 検索数
                'order': 'viewCount', # 並び順
                'search_start': datetime.today() - timedelta(days=30), # 1ヶ月前
                'search_end': datetime.today(), # 本日
            }
        )

        return render(request, 'app/index.html', {
            'form': form
        })

    def post(self, request, *args, **kwargs):
        # キーワード検索
        form = YoutubeForm(request.POST or None)

        # フォームのバリデーション
        if form.is_valid():
            # フォームからデータを取得
            keyword = form.cleaned_data['keyword']
            items_count = form.cleaned_data['items_count']
            viewcount = form.cleaned_data['viewcount']
            order = form.cleaned_data['order']
            search_start = form.cleaned_data['search_start']
            search_end = form.cleaned_data['search_end']

            # 動画検索
            search_list = search_video(keyword, items_count, order, search_start, search_end)

            # 動画IDリスト作成
            videoid_list = {}
            for item in search_list:
                # key：動画ID
                # value：チャンネルID
                videoid_list[item[0]] = item[1]

            # チャンネルデータ取得
            channel_list = get_channel(videoid_list)

            # 動画データ取得
            count_list = get_video(videoid_list)

            # 動画データをデータフレーム化する
            youtube_data = make_df(search_list, channel_list, count_list, viewcount)

            return render(request, 'app/keyword.html', {
                'youtube_data': youtube_data,
                'keyword': keyword
            })
        else:
            return redirect('index')


class RelatedView(View):
    def get(self, request, *args, **kwargs):
        channelid_list = ['UCgQgMOBZOJ1ZDtCZ4hwP1uQ']
        myChannelId = 'UCaminwG9MTO4sLYeC3s6udA'

        search_start = datetime.today() - timedelta(days=30)
        search_end = datetime.today()

        # ライバル動画を検索
        rivalvideo_list = search_rivalvideo(channelid_list, search_start, search_end)

        # 関連動画を検索
        related_list = search_relatedvideo(rivalvideo_list, myChannelId)

        # 動画IDリスト作成
        videoid_list = {}
        id_list = []
        for item in related_list:
            # key：動画ID
            # value：チャンネルID
            videoid_list[item[1]] = item[2]
            id_list.append(item[1])

        # チャンネルデータ取得
        channel_list = get_channel(videoid_list)

        # 動画データ取得
        count_list = get_video(videoid_list)

        # ライバル動画IDリスト作成
        rivalvideoid_list = {}
        for item in related_list:
            # key：ライバル動画ID
            # value：ライバルチャンネルID
            rivalvideoid_list[item[6]] = item[7]

        # ライバルチャンネルデータ取得
        rivalchannel_list = get_channel(rivalvideoid_list)

        # ライバル動画データ取得
        rivalcount_list = get_video(rivalvideoid_list)

        # データフレームの作成
        youtube_data = pd.DataFrame(related_list, columns=[
            'ranking', # ランキング
            'videoid', # 動画ID
            'channelid', # チャンネルID
            'channeltitle', # チャンネル名
            'title', # 動画タイトル
            'publishtime', # 動画公開日
            'rivalvideoid', # ライバル動画ID
            'rivalchannelid', # ライバルチャンネルID
            'rivalchanneltitle', # ライバルチャンネル名
            'rivaltitle', # ライバル動画タイトル
            'rivalpublishtime', # ライバル動画公開日時
        ])

        # 重複の削除 subsetで重複を判定する列を指定,inplace=Trueでデータフレームを新しくするかを指定
        youtube_data.drop_duplicates(subset='videoid', inplace=True)

        # 動画のURL
        youtube_data['url'] = 'https://www.youtube.com/embed/' + youtube_data['videoid']
        youtube_data['rivalurl'] = 'https://www.youtube.com/embed/' + youtube_data['rivalvideoid']

        # データフレームの作成
        df_channel = pd.DataFrame(channel_list, columns=[
            'videoid',
            'profileImg'
        ])
        df_viewcount = pd.DataFrame(count_list, columns=[
            'videoid',
            'viewcount',
            'likeCount',
            'dislikeCount',
            'commentCount'
        ])
        # df_rivalchannel = pd.DataFrame(rivalchannel_list, columns=[
        #     'videoid',
        #     'rivalprofileImg'
        # ])
        # df_rivalviewcount = pd.DataFrame(rivalcount_list, columns=[
        #     'videoid',
        #     'rivalviewcount',
        #     'rivallikeCount',
        #     'rivaldislikeCount',
        #     'rivalcommentCount'
        # ])

        # 2つのデータフレームのマージ
        youtube_data = pd.merge(df_channel, youtube_data, on='videoid', how='left')
        youtube_data = pd.merge(df_viewcount, youtube_data, on='videoid', how='left')
        # youtube_data = pd.merge(df_rivalchannel, youtube_data, on='videoid', how='left')
        # youtube_data = pd.merge(df_rivalviewcount, youtube_data, on='videoid', how='left')

        # データフレーム抽出
        youtube_data = youtube_data[[
            'ranking', # ランキング
            'url', # 動画URL
            'profileImg', # プロフィール画像
            'title', # 動画タイトル
            'channeltitle', # チャンネル名
            'viewcount', # 再生回数
            'publishtime', # 動画公開日
            'likeCount', # 高評価数
            'dislikeCount', # 低評価数
            'commentCount', # コメント数
            'rivalurl',# ライバル動画URL
            # 'rivalprofileImg', # ライバルプロフィール画像
            'rivaltitle', # ライバル動画タイトル
            'rivalchanneltitle', # ライバルチャンネル名
            # 'rivalviewcount', # ライバル再生回数
            'rivalpublishtime', # ライバル動画公開日
            # 'rivallikeCount', # ライバル高評価数
            # 'rivaldislikeCount', # ライバル低評価数
            # 'rivalcommentCount', # ライバルコメント数
        ]]

        return render(request, 'app/related.html', {
            'youtube_data': youtube_data
        })
