import psycopg2
import pandas as pd
import sys
import math

def connect():
    conn = None
    try:
        conn = psycopg2.connect(
                   host='localhost',
                   database='postgres',
                   user='django',
                   password='django2024')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    return conn

def sql_to_dataframe(conn, query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print('Error: %s' % error)
        cursor.close()
        return 1
    # The execute returns a list of tuples:
    tuples_list = cursor.fetchall()
    headers = []
    for col in cursor.description:
        headers.append(col[0])

    cursor.close()
    # Now we need to transform the list into a pandas DataFrame:
    df = pd.DataFrame(tuples_list, columns=headers,)
    return df


def geos_color(value, kpi):
    val = float(value)
    kpi = kpi.upper()
    if kpi == 'RSRP':
        if val > -85:
            return 'lime'
        elif val <= -85 and val > -95:
            return 'aqua'
        elif val <= -95 and val > -105:
            return 'blue'
        elif val <= -105 and val > -115:
            return 'yellow'
        elif val <= -115:
            return 'red'
    elif kpi == 'RSRQ':
        if val > -6:
            return 'lime'
        elif val <= -6 and val > -10:
            return 'aqua'
        elif val <= -10 and val > -14:
            return 'blue'
        elif val <= -14 and val > -18:
            return 'yellow'
        elif val <= -18:
            return 'red'
    elif kpi == 'CQI':
        if val > 12:
            return 'lime'
        elif val <= 12 and val > 9:
            return 'aqua'
        elif val <= 9 and val > 6:
            return 'blue'
        elif val <= 6 and val > 3:
            return 'yellow'
        elif val <= 3:
            return 'red'
    elif kpi == 'CINR':
        if val > 15:
            return 'lime'
        elif val <= 15 and val > 10:
            return 'blue'
        elif val <= 10 and val > 5:
            return 'aqua'
        elif val <= 5 and val > 0:
            return 'yellow'
        elif val <= 0 and val > -5:
            return 'red'
        elif val <= -5:
            return 'black'

def LatLongSpherToMerc(coordinates):
    lat = coordinates[0]
    lon = coordinates[1]
    if float(lat) > 89.5:
        lat = 89.5
    if float(lat) < -89.5:
        lat = -89.5

    rLat = math.radians(float(lat))
    rLong = math.radians(float(lon))

    a = 6378137.0
    x = a * rLong
    y = a * math.log(math.tan(math.pi / 4 + rLat / 2))
    return (x, y)