from rest_framework.response import Response
from rest_framework.decorators import api_view
from geo.Geoserver import Geoserver
import time
import os
from . import posgre_to_pd as ptp
import json

# Create your views here.

@api_view(['GET'])
def get_coverage(request):
    conn = ptp.connect()
    query = """select geojson from geo_test.coverage_analytics ca where uid = '1'"""
    cursor = conn.cursor()
    cursor.execute(query)
    tuples_list = cursor.fetchall()
    response = json.dumps(tuples_list[0][0])
    return Response(json.loads(response))


@api_view(['GET'])
def get_cells(request):
    conn = ptp.connect()
    query = """select geojson from geo_test.cells c where uid = '3'"""
    cursor = conn.cursor()
    cursor.execute(query)
    tuples_list = cursor.fetchall()
    response = json.dumps(tuples_list[0][0])
    return Response(json.loads(response))


@api_view(['GET'])
def dismantle_site(request):
    cells = request.GET.getlist('cells[]', [])
    sites = request.GET.getlist('sites[]', [])
    # creating layer name
    current_timestamp = time.time()
    layer_name = sites[0] + str(current_timestamp)

    # hardcoded predefined workspace
    workspace = 'dismantle'

    # open tiff file
    # dev
    # file_data = open(r'/rest/geo_gateway/static/raster_test_output_rgba.tif')
    # prod
    file_data = open(r'/rest/rest/geo_gateway/static/raster_test_output_rgba.tif')
    file = file_data.name

    # publishing geolayer
    geo = Geoserver('http://geoserver:8080/geoserver', username='admin', password='geoserver')
    geo.create_coveragestore(layer_name=layer_name,
                             path=file,
                             workspace='dismantle')
    file_data.close()
    # response data
    return Response({'workspace': workspace, 'layer': layer_name})
