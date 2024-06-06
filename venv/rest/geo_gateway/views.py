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
    query = """select 
 longitude
, latitude
,k."RSRP" rsrp
from geo_test.kirill k limit 100 """
    df = ptp.sql_to_dataframe(conn, query)
    gdf = geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
    )
    response = json.loads(gdf.to_json())
    for feature in response["features"]:
        feature["properties"]["fill"] = ptp.geos_color(feature["properties"]["rsrp"], "rsrp")

    return Response(response)

