from django.contrib import auth
from django.contrib.auth import authenticate, login, logout
from django.shortcuts import render, redirect
from accounts.forms import UserForm


def sign_up(request):

    if request.method == "POST":
        form = UserForm(request.POST)
        
        if form.is_valid():  # 유효성 검사
            form.save()
            username = form.cleaned_data.get('username')
            raw_password = form.cleaned_data.get('password1')
            user = authenticate(username=username, password=raw_password)  # 사용자 인증
            login(request, user)  # 로그인
            return redirect('/options/date_opt')  # 성공하면 날씨 선택 페이지로 이동

    form = UserForm()  
    return render(request, 'accounts/sign_up.html', {'form': form})  # 다시 회원가입 페이지 로드
