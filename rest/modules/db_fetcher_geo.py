#Release history
#01.03.2024: site_data_pix function added (copy of sector_data_pix)


from modules.db_handler import DBHandler

import pandas as pd
import math
import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.features import rasterize
import glob
import os
import time


class db_fetcher(DBHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)  # Initializing the base class

    def sector_data_pix(self, sector_id, rsrp_min, rsrp_max, cqi_min, cqi_max, scenario_optim, scenario_traffic, year, kpi):
        """Method to fetch data from pixel_sector table based on the given parameters."""

        # If sector_id is a list or tuple with more than one value, format it appropriately
        # Else, just enclose the single value in parenthesis
        if isinstance(sector_id, (list, tuple)) and len(sector_id) > 1:
            sector_id_str = str(tuple(sector_id))
        else:
            sector_id_str = f"('{sector_id[0]}')"

        # Construct the SQL query
        query = f"""
        SELECT latitude_50, longitude_50, {kpi}
        FROM pixel_sector
        WHERE 
            sector_ID IN {sector_id_str} AND 
            scenario_optim = {scenario_optim} AND
            ABS (scenario_traffic - {scenario_traffic}) < 0.0001 AND
            year = {year} AND
            geo_rsrp >= {rsrp_min} AND
            geo_rsrp <= {rsrp_max} AND
            geo_cqi >= {cqi_min} AND
            geo_cqi <= {cqi_max}
        """
        # Execute the query and fetch data
        data = self.execute(query)

        # Specify the column names and embed the results in a DataFrame and return
        df = pd.DataFrame(data, columns=['latitude_50', 'longitude_50', 'kpi'])

        return df

    def coverage_data_pix(self, rsrp_min, rsrp_max, cqi_min, cqi_max, scenario_traffic, scenario_optim, year, kpi):
        """Method to fetch data from pixel_agg table based on the given parameters."""

        # Construct the SQL query
        query = f"""
        SELECT latitude_50, longitude_50, geo_rsrp, geo_cqi, {kpi}
        FROM pixel_agg
        WHERE 
            scenario_optim = {scenario_optim} AND
            ABS (scenario_traffic - {scenario_traffic}) < 0.0001 AND
            year = {year} AND
            geo_rsrp >= {rsrp_min} AND
            geo_rsrp <= {rsrp_max} AND
            geo_cqi >= {cqi_min} AND
            geo_cqi <= {cqi_max}
        """
        # Execute the query and fetch data
        data = self.execute(query)

        # Specify the column names and embed the results in a DataFrame and return
        df = pd.DataFrame(data, columns=['latitude_50', 'longitude_50', 'geo_rsrp', 'geo_cqi', 'kpi'])

        return df

    def sector_data_pix_ext(self, sector_id, rsrp_min, rsrp_max, cqi_min, cqi_max, scenario_optim, scenario_traffic, year, kpi):
        """Extended Method to fetch data from pixel_sector table based on the given parameters.
            includes RSRP, Traffic and CQI fields always (used in sector troubleshooting dashboard)"""

        if isinstance(sector_id, (list, tuple)) and len(sector_id) > 1:
            sector_id_str = str(tuple(sector_id))
        else:
            sector_id_str = f"('{sector_id[0]}')"

        # Construct the SQL query
        query = f"""
        SELECT sector_ID, latitude_50, longitude_50, {kpi}, geo_rsrp, geo_cqi, geo_served_demand, geo_user_tput_dl, count_samples
        FROM pixel_sector
        WHERE 
            sector_ID IN {sector_id_str} AND 
            scenario_optim = {scenario_optim} AND
            ABS (scenario_traffic - {scenario_traffic}) < 0.0001 AND
            year = {year} AND
            geo_rsrp >= {rsrp_min} AND
            geo_rsrp <= {rsrp_max} AND
            geo_cqi >= {cqi_min} AND
            geo_cqi <= {cqi_max}
        """
        # Execute the query and fetch data
        data = self.execute(query)

        # Specify the column names and embed the results in a DataFrame and return
        df = pd.DataFrame(data, columns=['sector_id', 'latitude_50', 'longitude_50', 'kpi', 'geo_rsrp', 'geo_cqi', 'geo_served_demand', 'geo_user_tput_dl', 'count_samples'])

        return df


    def site_data_pix_ext(self, site_id, rsrp_min, rsrp_max, cqi_min, cqi_max, scenario_optim, scenario_traffic, year, kpi):
        """Same as sector_data_pix but works with sites as the input. Note: [Works with 1 site only so far]. it doesnt change sector granularity of the output.
            it just select all sectors aligned with site_id inputs. Extended Method to fetch data from pixel_sector table based on the given parameters.
            includes RSRP, Traffic and CQI fields always (used in sector troubleshooting dashboard)"""

        # Construct the SQL query
        query = f"""
        SELECT sector_ID, latitude_50, longitude_50, {kpi}, geo_rsrp, geo_cqi, geo_served_demand, geo_user_tput_dl, count_samples
        FROM pixel_sector
        WHERE 
            site_id = '{site_id}' AND 
            scenario_traffic = {scenario_traffic} AND
            scenario_optim = {scenario_optim} AND           
            year = {year} AND
            geo_rsrp >= {rsrp_min} AND
            geo_rsrp <= {rsrp_max} AND
            geo_cqi >= {cqi_min} AND
            geo_cqi <= {cqi_max}
        """
        # Execute the query and fetch data
        data = self.execute(query)

        # Specify the column names and embed the results in a DataFrame and return
        df = pd.DataFrame(data, columns=['sector_id', 'latitude_50', 'longitude_50', 'kpi', 'geo_rsrp', 'geo_cqi', 'geo_served_demand', 'geo_user_tput_dl', 'count_samples'])

        return df


    def compet_csp_data_pix(self, csp_name, band_category, roads, population_min, population_max, rsrp_min, rsrp_max, kpi):
        """Method to fetch competitive data from csp_details_pop table based on the given parameters."""

        # Construct the SQL query
        if roads == 0:
            query = f"""
            SELECT Latitude, Longitude, QOS_RSRP, population, {kpi}
            FROM csp_details_pop
            WHERE 
                Connection_ServiceProviderBrandName = '{csp_name}' AND
                band_category = '{band_category}' AND
                population >= {population_min} AND
                population <= {population_max} AND
                QOS_RSRP >= {rsrp_min} AND
                QOS_RSRP <= {rsrp_max}
            """
        else:
            query = f"""
            SELECT Latitude, Longitude, QOS_RSRP, population, {kpi}
            FROM csp_details_pop
            WHERE 
                Connection_ServiceProviderBrandName = '{csp_name}' AND
                band_category = '{band_category}' AND
                roads_proximity = {roads} AND
                population >= {population_min} AND
                population <= {population_max} AND
                QOS_RSRP >= {rsrp_min} AND
                QOS_RSRP <= {rsrp_max}
            """
        # Execute the query and fetch data
        data = self.execute(query)

        # Specify the column names and embed the results in a DataFrame and return
        df = pd.DataFrame(data, columns=['Latitude', 'Longitude', 'QOS_RSRP', 'population', 'kpi'])

        return df

    def compet_cat_data_pix(self, csp_name, band_category, cat_min, cat_max, roads, population_min, population_max, kpi):
        """Method to fetch competitive summary data from compet_cat_pop table based on the given parameters."""

        # Construct the SQL query
        if roads == 0:
            query = f"""
            SELECT Latitude, Longitude, population, {kpi}
            FROM compet_cat_pop
            WHERE
                target_csp = '{csp_name}' AND
                band_category = '{band_category}' AND
                population >= {population_min} AND
                population <= {population_max} AND
                {kpi} >= {cat_min} AND
                {kpi} <= {cat_max} AND
                {kpi} > 0
            """
        else:
            query = f"""
            SELECT Latitude, Longitude, population, {kpi}
            FROM compet_cat_pop
            WHERE 
                target_csp = '{csp_name}' AND
                band_category = '{band_category}' AND
                roads_proximity = {roads} AND
                population >= {population_min} AND
                population <= {population_max} AND
                {kpi} >= {cat_min} AND
                {kpi} <= {cat_max} AND
                {kpi} > 0
            """
        # Execute the query and fetch data
        #print(query)
        data = self.execute(query)

        # Specify the column names and embed the results in a DataFrame and return
        df = pd.DataFrame(data, columns=['Latitude', 'Longitude', 'population', 'kpi'])

        return df

    def bestserver_data_pix(self, scenario_traffic, scenario_optim):
        """Method to fetch data from pixel_agg table based on the given parameters."""

        # Construct the SQL query
        query = f"""
        SELECT latitude_50, longitude_50, best_server
        FROM pixel_agg
        WHERE 
            scenario_optim = {scenario_optim} AND
            ABS (scenario_traffic - {scenario_traffic}) < 0.0001 AND
            year = 2
        """
        # Execute the query and fetch data
        data = self.execute(query)

        # Specify the column names and embed the results in a DataFrame and return
        df = pd.DataFrame(data, columns=['latitude_50', 'longitude_50', 'best_server'])

        return df

    def generate_raster(self, sector_df):
        # Convert df_pixel to a GeoDataFrame
        # [Visualization lib]
        geometry = gpd.points_from_xy(sector_df.longitude_50, sector_df.latitude_50)
        gdf = gpd.GeoDataFrame(sector_df, geometry=geometry, crs="EPSG:4326")
        gdf = gdf[gdf['kpi'].notna()]
        gdf['kpi'] = gdf['kpi'].apply(lambda x: round(x, 2))
        xmin, ymin, xmax, ymax = gdf.geometry.total_bounds
        res_lat = 56.0 / 111000 #raster size (TO FIX in the future, because it depends a lot on where the project is happening
        cols = int((xmax - xmin) / res_lat)
        rows = int((ymax - ymin) / res_lat)

        shapes = [(geom, value) for geom, value in zip(gdf.geometry, gdf['kpi'])]
        transform = from_origin(xmin, ymax, res_lat, res_lat)
        out_array = rasterize(shapes=shapes, transform=transform, out_shape=(rows, cols), default_value=0)

        ### clearing recent files from the folder before adding new one

        ts=time.time()
        # Filter files older than 3 hours
        files_sector = glob.glob('data/temp_rasters/sector_raster*.tif') + glob.glob('temp_rasters/sector_raster*.xml')
        files_sector = [file for file in files_sector if os.path.getmtime(file) < time.time() - 6 * 3600]
        for file in files_sector:
            os.remove(file)
        print('Generate_raster function:\n Found next files for clearing', files_sector)
        te=time.time()
        td=te-ts
        print('Files above have been cleared and took', td, 'seconds')


        timestamp=time.time()
        raster_filename = f'data/temp_rasters/sector_raster_switchoff_{timestamp}.tif'

        with rasterio.open(raster_filename, 'w', driver='GTiff', height=rows, width=cols, count=1, dtype='float32',
                           crs=gdf.crs, transform=transform, compress='deflate', tiled=True, predictor=2) as dst:
            dst.write(out_array, 1)

        #print('Raster is ready', raster_filename)
        return raster_filename



    def generate_raster_django_test(self, sector_df):
        # Temp copy of generate_raster function used in django test
        # It returnes both raster_as_a_file and raster_as_a_path
        # Convert df_pixel to a GeoDataFrame
        # [Visualization lib]

        geometry = gpd.points_from_xy(sector_df.longitude_50, sector_df.latitude_50)
        gdf = gpd.GeoDataFrame(sector_df, geometry=geometry, crs="EPSG:4326")
        gdf = gdf[gdf['kpi'].notna()]
        gdf['kpi'] = gdf['kpi'].apply(lambda x: round(x, 2))
        xmin, ymin, xmax, ymax = gdf.geometry.total_bounds
        res_lat = 56.0 / 111000 #raster size (TO FIX in the future, because it depends a lot on where the project is happening
        cols = int((xmax - xmin) / res_lat)
        rows = int((ymax - ymin) / res_lat)

        shapes = [(geom, value) for geom, value in zip(gdf.geometry, gdf['kpi'])]
        transform = from_origin(xmin, ymax, res_lat, res_lat)
        out_array = rasterize(shapes=shapes, transform=transform, out_shape=(rows, cols), default_value=0)

        ### clearing recent files from the folder before adding new one

        ts=time.time()
        # Filter files older than 3 hours
        files_sector = glob.glob('data/temp_rasters/sector_raster*.tif') + glob.glob('temp_rasters/sector_raster*.xml')
        files_sector = [file for file in files_sector if os.path.getmtime(file) < time.time() - 6 * 3600]
        for file in files_sector:
            os.remove(file)
        print('Generate_raster function:\n Found next files for clearing', files_sector)
        te=time.time()
        td=te-ts
        print('Files above have been cleared and took', td, 'seconds')


        timestamp=time.time()
        raster_filename = f'data/temp_rasters/sector_raster_switchoff_{timestamp}.tif'

        # Saving out_array as a raster file in memory:
        


        # Saving raster to the file in local folder
        with rasterio.open(raster_filename, 'w', driver='GTiff', height=rows, width=cols, count=1, dtype='float32',
                           crs=gdf.crs, transform=transform, compress='deflate', tiled=True, predictor=2) as dst:
            dst.write(out_array, 1)

        # Preparing raster file object to be returned as the output of the function


        #print('Raster is ready', raster_filename)
        return raster_filename



    def generate_raster_compet(self, sector_df):
        #this method creates a raster for competitive data visualization (300m)
        # Convert df_pixel to a GeoDataFrame
        # [Geospatial]
        geometry = gpd.points_from_xy(sector_df.Longitude, sector_df.Latitude)
        gdf = gpd.GeoDataFrame(sector_df, geometry=geometry, crs="EPSG:4326")
        gdf = gdf[gdf['kpi'].notna()]
        gdf['kpi'] = gdf['kpi'].apply(lambda x: round(x, 2))
        xmin, ymin, xmax, ymax = gdf.geometry.total_bounds
        res_lat = 315.0 / 111000 #raster size (TO FIX in the future, because it depends a lot on where the project is happening
        cols = int((xmax - xmin) / res_lat)
        rows = int((ymax - ymin) / res_lat)

        shapes = [(geom, value) for geom, value in zip(gdf.geometry, gdf['kpi'])]
        transform = from_origin(xmin, ymax, res_lat, res_lat)
        out_array = rasterize(shapes=shapes, transform=transform, out_shape=(rows, cols), default_value=0)

        ### clearing recent files from the folder before adding new one
        files_compet = glob.glob('data/temp_rasters/compet_raster*.tif') + glob.glob('temp_rasters/compet_raster*.xml')
        files_compet = [file for file in files_compet if os.path.getmtime(file) < time.time() - 6 * 3600]
        for file in files_compet:
            os.remove(file)
        print('Generate_raster_compet: Found next files for clearing', files_compet)
        print('Files above have been cleared')
    

        timestamp=time.time()
        raster_filename = f'data/temp_rasters/compet_raster{timestamp}.tif'
        
        with rasterio.open(raster_filename, 'w', driver='GTiff', height=rows, width=cols, count=1, dtype='float32',
                           crs=gdf.crs, transform=transform, compress='deflate', tiled=True, predictor=2) as dst:
            dst.write(out_array, 1)

        #print('Raster is ready', raster_filename)
        return raster_filename
    
    def get_center_density(self, df):
        # Calculates center density points removing the outliers
        # [Visualization, Used]

        # Picking up latitude and longitude based on the column names
        latitude = df['latitude_50'].astype(float)
        longitude = df['longitude_50'].astype(float)

        # Extracting the bounding box coordinates
        xmin_i = longitude[longitude <= longitude.quantile(0.02)].max()
        xmax_i = longitude[longitude > longitude.quantile(0.98)].min()
        ymin_i = latitude[latitude <= latitude.quantile(0.02)].max()
        ymax_i = latitude[latitude > latitude.quantile(0.98)].min()
        x_center = (xmax_i+xmin_i)/2
        y_center = (ymax_i+ymin_i)/2

        return [y_center, x_center]
    
