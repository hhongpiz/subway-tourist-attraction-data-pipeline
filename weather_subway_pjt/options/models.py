from django.db import models

# Create your models here.
class Tourism(models.Model):
    t_idx = models.BigAutoField(primary_key=True)
    station_name = models.CharField(max_length=100)
    t_name = models.CharField(max_length=100)
    cate = models.CharField(max_length=10)
    address = models.CharField(max_length=100)
    lat = models.FloatField()
    lon = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'tourism'
