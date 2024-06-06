from django.db import models

# Create your models here.
class Musician(models.Model):

    date = models.CharField(max_length=50)
    uid = models.CharField(max_length=50)
    granularity = models.CharField(max_length=100)
    growth = models.CharField(max_length=100)
    kpi = models.CharField(max_length=100)