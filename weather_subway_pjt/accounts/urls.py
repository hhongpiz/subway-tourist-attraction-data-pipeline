from django.core.checks.templates import check_for_template_tags_with_the_same_name
from django.urls import path

# 장고가 제공하는 계정과 관련된 view import
from django.contrib.auth import views as auth_views
from . import views

app_name = 'accounts'

urlpatterns = [
    path('log_in/',  auth_views.LoginView.as_view(template_name='accounts/log_in.html'), name='log_in'),
    path('sign_up/', views.sign_up, name='sign_up'),
    path('log_out/', auth_views.LogoutView.as_view(), name='log_out'),
]