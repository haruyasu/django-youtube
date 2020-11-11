from django import forms

ORDER_CHOICES = {
    'viewCount': '再生回数の多い順',
    'date': '作成日の新しい順',
    'rating': '評価の高い順',
    'relevance': '関連性が高い順',
}

class YoutubeForm(forms.Form):
    keyword = forms.CharField(max_length=30, label='キーワード')
    items_count = forms.IntegerField(label='検索数')
    viewcount = forms.IntegerField(label='再生回数')
    order = forms.ChoiceField(label='並び順', widget=forms.Select, choices=list(ORDER_CHOICES.items()))
    search_start = forms.DateField(widget=forms.DateInput(attrs={"type":'date'}), label='検索開始日')
    search_end = forms.DateField(widget=forms.DateInput(attrs={"type":'date'}), label='検索終了日')
