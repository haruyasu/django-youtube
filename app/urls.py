from django.urls import path
from app import views

urlpatterns = [
    path('', views.IndexView.as_view(), name='index'),
    path('related/', views.RelatedView.as_view(), name='related'),
]
