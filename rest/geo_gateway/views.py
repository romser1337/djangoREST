from rest_framework.response import Response
from rest_framework.decorators import api_view

from . import posgre_to_pd as ptp
import pandas as pd
import geopandas
import json


# Create your views here.

@api_view(['GET'])
def getData(request):
    QueryDict = request.query_params
    conn = ptp.connect()

    query2 = """select geojson from geo_test.coverage_analytics ca where 1=1 uid = '1'"""
    cursor = conn.cursor()
    cursor.execute(query2)
    tuples_list = cursor.fetchall()
    response = json.dumps(tuples_list[0][0])
    return Response(json.loads(response))

