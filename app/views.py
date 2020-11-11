from django.views.generic import View
from django.shortcuts import render, redirect
from apiclient.discovery import build
from datetime import datetime, timedelta
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
def get_videos(videoid_list):
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
        'videoid', 'channelId', 'publishtime', 'title', 'channeltitle'
    ])

    # 重複の削除 subsetで重複を判定する列を指定,inplace=Trueでデータフレームを新しくするかを指定
    youtube_data.drop_duplicates(subset='videoid', inplace=True)

    # 埋め込み動画のURL
    youtube_data['url'] = 'https://www.youtube.com/embed/' + youtube_data['videoid']

    # データフレームの作成
    df_channel = pd.DataFrame(channel_list, columns=[
        'videoid', 'profileImg'
    ])
    df_viewcount = pd.DataFrame(count_list, columns=[
        'videoid', 'viewcount', 'likeCount', 'dislikeCount', 'commentCount'
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
            count_list = get_videos(videoid_list)

            # 動画データをデータフレーム化する
            youtube_data = make_df(search_list, channel_list, count_list, viewcount)

            return render(request, 'app/youtube.html', {
                'youtube_data': youtube_data,
                'keyword': keyword
            })
        else:
            return redirect('index')
