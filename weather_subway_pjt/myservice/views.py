from django.shortcuts import render, redirect
from options.models import Tourism
from .models import MyPlace
from django.contrib.auth.decorators import login_required

# Create your views here.
@login_required(login_url='accounts:log_in')
def my_place(request):
    list_mp = None
    list_st = None
    id = request.user.id

    if request.method == 'GET':
        list_mp=MyPlace.objects.filter(user_id=id).distinct().values('station_name','t_name', 'mp_idx', 'cate')
        list_st=MyPlace.objects.filter(user_id=id).distinct().values('station_name','gu')

    else: 
        return redirect('/')

    return render(request, 'myservice/my_place.html',{'mp':list_mp,'st':list_st})


# Create your views here.
@login_required(login_url='accounts:log_in')
def save_my_place(request):


    id = request.user.id

    if request.method == 'POST':
        station = request.POST['station']
        gu = request.POST['gu']
        location = request.POST.getlist('location')
        location_cate = request.POST.getlist('location_cate')
        id = request.user.id
        list_t=MyPlace.objects.filter(user_id=id).values('t_name')
        # # print(list_t)
        l=[]
        for i in range(len(list_t)):
            l.append(list(list_t[i].values())[0])


        for i, name in enumerate(location):
            if len(location) != 0:
                if name not in l:
                    #for i, name in enumerate(location):
                   MyPlace(user_id=id, gu=gu, station_name=station, t_name=name, cate=location_cate[i]).save()
            else:
                MyPlace(user_id=id, gu=gu, station_name=station, t_name=name, cate=location_cate[i]).save()
    else:
        return redirect('/')

    return redirect('/myservice/my_place/')

@login_required(login_url='accounts:log_in')
def del_my_place(request):

    if request.method == 'GET':
        return redirect('/')

    id = request.user.id
    del_places = request.POST.getlist('del-loc')

    MyPlace.objects.filter(user_id=id, mp_idx__in=del_places).delete()
    return redirect('/myservice/my_place/')



