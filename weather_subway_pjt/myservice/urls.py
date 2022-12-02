from django.urls import path
from . import views



urlpatterns = [
    path('my_place/', views.my_place, name='my_place'),
    path('my_place_impl/', views.save_my_place, name='my_place'),
    path('del/my_place/', views.del_my_place, name='my_place'),
]