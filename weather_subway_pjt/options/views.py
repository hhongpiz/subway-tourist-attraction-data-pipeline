#options->views.py
from django.shortcuts import render
from django.http import HttpResponse
from options import module_test
from django.contrib.auth.decorators import login_required
from options.models import Tourism
# Create your views here.
@login_required(login_url='accounts:log_in')
def date_opt(request):
	return render(request, 'options/date_opt.html')

@login_required(login_url='accounts:log_in')
def subway_opt(request):
	global context, result, gu,y, m, d, time
	day = request.GET['day']
	time = request.GET['time']
	gu = request.GET['gu']
	# 1. get 방식으로 넘어온 date, time, gu 값 가지고 오기(변수로) # 게시판 데이터, 카페 장고
	weather, result, date = module_test.weather_import(day = day, time = time, gu = gu) # view에서 호출
	result.reset_index(inplace=True)
	# 1. 위경도를 db에서 가져오는 코드
	# 2. result에 넣는 코드
	# 3. 위경도 정보를 subway_opt로 넘기는 코드
	# 4. subway_opt측에서 위경도 정보를 좌표 입력란에 넣는 코드
	result.columns = ['ranking', 'station_name', 'result', 'lat', 'lng']
	result = result.to_dict('ranking')
	y = date.values[0][:4]
	m = date.values[0][5:7]
	d = date.values[0][8:10]
	pm10 = int(weather.PM10.iloc[0])
	pm25 = int(weather.PM25.iloc[0])

	if pm10 == 15:
		pm10 = '/static/img/PM10_good.jpg'
	elif pm10 == 55:
		pm10 = '/static/img/PM10_normal.jpg'
	elif pm10 == 115:
		pm10 = '/static/img/PM10_bad.jpg'
	else:
		pm10 = '/static/img/PM10_verybad.jpg'

	if pm25 == 7.5:
		pm25 = '/static/img/PM25_good.jpg'
	elif pm25 == 25:
		pm25 = '/static/img/PM25_normal.jpg'
	elif pm25 == 55:
		pm25 = '/static/img/PM25_bad.jpg'
	else:
		pm25 = '/static/img/PM25_verybad.jpg'

	context = {'tmp': weather.ONDO.iloc[0], 'humn': weather.HUMN.iloc[0], 'sky': weather.SKY.iloc[0],
			   'rain': weather.RAIN.iloc[0],'snow': weather.SNOW.iloc[0], 'windd': weather.WINDD.iloc[0],
			   'winds': weather.WINDS.iloc[0], 'pm10': pm10, 'pm25': pm25}
	# 미세먼지는 넘길때 이미지 경로를 설정해서 넘기기

	return render(request,  'options/subway_opt.html', {'weather': context, 'result': result, 'gu': gu, 'year': y, 'month': m, 'day': d, 'time': time})
	# return render(request, 'sharesRes/restaurantUpdate.html', content)
	# return render(request, 'options/subway_opt.html') # render할때 context를 붙여서 보내기!

@login_required(login_url='accounts:log_in')
def place_opt(request):
	station = request.GET['station']
	pop=request.GET['pop']
	res={'station':station,'pop':pop}
	tour = Tourism.objects.filter(station_name=station).values('station_name', 't_name', 'address','cate','lat', 'lon')
	for i in tour:
		if i['cate'] == '관광지':
			i.update(cate='tour')
		else:
			i.update(cate='res')

	for j in tour:
		j['t_name']=j['t_name'].replace('/','')

	return render(request, 'options/place_opt.html',{'tourist':tour,'station_pop':res,'weather': context, 'result': result, 'gu': gu, 'year': y, 'month': m, 'day': d, 'time': time})