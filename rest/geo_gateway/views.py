from rest_framework.response import Response
from rest_framework.decorators import api_view
from geo.Geoserver import Geoserver
import time
import os
from . import posgre_to_pd as ptp
import json

### iPrism modules import
from modules.db_handler import DBHandler
from modules.vis_geomaps import pix_data_site_switch_off, raster_transform_django_test, get_visualization_params
from modules.db_fetcher_geo import generate_raster
from modules.dt_geosimulator import dt_geosimulator

### Create folders in the container:
#   /modules
#   /data
#   /data/temp_rasters



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
    #cells = request.GET.getlist('cells[]', [])
    sites = request.GET.getlist('sites[]', [])
    # AP: sites should be a list object with Site_IDs inside. Each site should be in the following format: Site_135, Site_136, etc.
    
    # creating layer name
    current_timestamp = time.time()
    layer_name = sites[0] + str(current_timestamp)

    # hardcoded predefined workspace
    workspace = 'dismantle'

    ##### iPrism code section START

    # check is sites input is not empty and that it is a list
    if not sites:
        print("Sites list is empty")
    elif not isinstance(sites, list):
        print("Sites list is not a list")

    db = DBHandler(password="smacap", dbname='geospatial')
    dt_geo = dt_geosimulator(password="smacap", dbname='geospatial')


    # Hardcoded user GUI selection:
    selection_tab3 = "QoE"

    year = 0
    traffic_scenario = 1.3
    optim_scenario = 0

    if selection_tab3 == "QoE":
        kpi = 'geo_user_tput_dl'

    rsrp_min, rsrp_max = -130, -50
    cqi_min, cqi_max = 5, 13

    folium_params_tab3 = get_visualization_params(selection_tab3)
    if folium_params_tab3 != "Invalid Selection":
        vmin = folium_params_tab3['vmin']
        vmax = folium_params_tab3['vmax']
        cmap = folium_params_tab3['cmap']
        legend_title = folium_params_tab3['legend_title']
        nodata_val = folium_params_tab3['nodata']
    else:
        print("Error in get_visualization_params")


    cov_data=dt_geo.pix_data_site_switch_off(rsrp_min, rsrp_max, cqi_min, cqi_max, traffic_scenario, optim_scenario, year, kpi, sites)
    raster_cov_filter=db.generate_raster(cov_data)
    
    memfile_rgb, memfile_rgba, output_rgb_file, output_rgba_file, legend_dict_tab3, bounds_tab3 = raster_transform_django_test(raster_cov_filter, vmin, vmax, cmap, 6, 0)
    # Description of the output:
    # memfile_rgb - in-memory file object
    # memfile_rgba - in-memory file object
    # output_rgb_file - file path to the RGB raster
    # output_rgba_file - file path to the RGBA raster
    # legend_dict_tab3 - legend dictionary
    # bounds_tab3 - bounds list

    # We go with 4 channel memfile raster as the default output scenario:
    file_data = memfile_rgba

    ### iPrism code section END

    # open tiff file
    # dev
    # file_data = open(r'/rest/geo_gateway/static/raster_test_output_rgba.tif')
    # prod
    # file_data = open(r'/rest/rest/geo_gateway/static/raster_test_output_rgba.tif')
    file = file_data.name

    # publishing geolayer
    geo = Geoserver('http://geoserver:8080/geoserver', username='admin', password='geoserver')
    geo.create_coveragestore(layer_name=layer_name,
                             path=file,
                             workspace='dismantle')
    file_data.close()
    # response data
    return Response({'workspace': workspace, 'layer': layer_name})
