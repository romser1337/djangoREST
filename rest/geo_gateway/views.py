from rest_framework.response import Response
from rest_framework.decorators import api_view

from . import posgre_to_pd as ptp
import pandas as pd
import geopandas
import json


# Create your views here.

@api_view(['GET'])
def getData(request):
    conn = ptp.connect()
    query = """select geojson from geo_test.coverage_analytics ca where uid = '1'"""
    cursor = conn.cursor()
    cursor.execute(query)
    tuples_list = cursor.fetchall()
    response = json.dumps(tuples_list[0][0])
    return Response(json.loads(response))


@api_view(['GET'])
def getCells(request):
    conn = ptp.connect()
    query = """select geojson from geo_test.cells c where uid = '3'"""
    cursor = conn.cursor()
    cursor.execute(query)
    tuples_list = cursor.fetchall()
    response = json.dumps(tuples_list[0][0])
    return Response(json.loads(response))


@api_view(['GET'])
def dismantleSIte(request):
    # some code
    # geojson =  function(siteid)
    return Response(json.loads())
