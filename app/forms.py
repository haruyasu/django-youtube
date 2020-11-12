from django import forms

ORDER_CHOICES = {
    'viewCount': '再生回数の多い順',
    'date': '作成日の新しい順',
    'rating': '評価の高い順',
    'relevance': '関連性が高い順',
}

class KeywordForm(forms.Form):
    keyword = forms.CharField(max_length=100, label='キーワード')
    items_count = forms.IntegerField(label='検索数')
    viewcount = forms.IntegerField(label='再生回数')
    order = forms.ChoiceField(label='並び順', widget=forms.Select, choices=list(ORDER_CHOICES.items()))
    search_start = forms.DateField(widget=forms.DateInput(attrs={"type":'date'}), label='検索開始日')
    search_end = forms.DateField(widget=forms.DateInput(attrs={"type":'date'}), label='検索終了日')

class RelatedForm(forms.Form):
    my_channel_id = forms.CharField(max_length=100, label='自分のチャンネルID')
    rival_channel_id = forms.CharField(max_length=1000, label='相手のチャンネルID')
    rival_items_count = forms.IntegerField(label='相手の検索数')
    rival_order = forms.ChoiceField(label='相手の並び順', widget=forms.Select, choices=list(ORDER_CHOICES.items()))
    rival_search_start = forms.DateField(widget=forms.DateInput(attrs={"type":'date'}), label='相手の検索開始日')
    rival_search_end = forms.DateField(widget=forms.DateInput(attrs={"type":'date'}), label='相手の検索終了日')
    related_items_count = forms.IntegerField(label='関連の検索数')
