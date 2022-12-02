from django.urls import path
from . import views


urlpatterns = [
    path('date_opt/', views.date_opt, name='date_opt'),
    path('subway_opt/', views.subway_opt, name='subway_opt'),
    path('place_opt/', views.place_opt, name='place_opt'),
]