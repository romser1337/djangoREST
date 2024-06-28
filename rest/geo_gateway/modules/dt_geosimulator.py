from modules.db_handler import DBHandler

import pandas as pd
import time as time
import numpy as np


class dt_geosimulator(DBHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)  # Initializing the base class
   
    def pix_data_site_switch_off(self, rsrp_min, rsrp_max, cqi_min, cqi_max, scenario_traffic, scenario_optim, year, kpi, sites_sw_off):
        #Method to fetch data from pixel_agg, pixel_sector table and simulate site switch-offs."""

        if sites_sw_off is None or not sites_sw_off:
            agg_query = f"""
                SELECT index, latitude_50, longitude_50, geo_rsrp, geo_cqi, {kpi}
                FROM pixel_agg
                WHERE 
                    scenario_traffic = {scenario_traffic} AND
                    scenario_optim = {scenario_optim} AND
                    year = {year} AND
                    geo_rsrp >= {rsrp_min} AND
                    geo_rsrp <= {rsrp_max} AND
                    geo_cqi >= {cqi_min} AND
                    geo_cqi <= {cqi_max}
                """
                
            ts=time.time()
            data_agg=self.execute(agg_query)
            df = pd.DataFrame(data_agg, columns=['index', 'latitude_50', 'longitude_50', 'geo_rsrp', 'geo_cqi', 'kpi'])
            return df

        # Fetch detailed sector info
        sector_query = f"""
            SELECT ps.index, 
                ps.Site_ID, 
                ps.geo_user_tput_dl, 
                ps.geo_churn_prob, 
                ps.geo_cap_demand, 
                ps.geo_served_demand, 
                ps.geo_latent_demand, 
                ps.geo_rsrp, 
                ps.geo_cqi, 
                ps.COUNT_SAMPLES
            FROM pixel_sector ps
            INNER JOIN (
            SELECT DISTINCT index 
            FROM pixel_sector 
            WHERE 
                Site_ID IN ({','.join(["'" + str(site) + "'" for site in sites_sw_off])}) AND
                scenario_traffic = {scenario_traffic} AND
                scenario_optim = {scenario_optim} AND
                year = {year}
            ) as affected_pixels ON ps.index = affected_pixels.index
            WHERE
                scenario_traffic = {scenario_traffic} AND
                scenario_optim = {scenario_optim} AND
                year = {year}
            """
        
        sector_query2 = f"""
        SELECT ps.index, 
                ps.Site_ID, 
                ps.geo_user_tput_dl, 
                ps.geo_churn_prob, 
                ps.geo_cap_demand, 
                ps.geo_served_demand, 
                ps.geo_latent_demand, 
                ps.geo_rsrp, 
                ps.geo_cqi, 
                ps.COUNT_SAMPLES
        FROM pixel_sector ps
        WHERE 
            ps.Site_ID IN ({','.join(["'" + str(site) + "'" for site in sites_sw_off])}) AND
            ps.scenario_optim = {scenario_optim} AND
            ps.scenario_traffic = {scenario_traffic} AND
            ps.year = {year}  
        """

        print('Fetching Switchoff Sectors data')
        #slice of pixel_sector for Site_ID from switch off list
        #ts=time.time()
        #sector_data = self.execute(sector_query2)
        #te=time.time()-ts
        #detailed_sector_df = pd.DataFrame(sector_data, columns=['index', 'Site_ID', 'geo_user_tput_dl', 'geo_churn_prob', 'geo_cap_demand', 'geo_served_demand', 'geo_latent_demand', 'geo_rsrp', 'geo_cqi', 'COUNT_SAMPLES'])


        #print('detailed_sector_df', detailed_sector_df)
        #print('Done\n')
        #print('Time SQL', te)


        # Two part data fetching:
        ### TEst part of the code with SQL queries split to 2 parts:

        ## Part1
        # Query to fetch distinct indices affected by the sites to be switched off
        affected_indices_query = f"""
            SELECT DISTINCT index
            FROM pixel_sector
            WHERE 
                Site_ID IN ({','.join(["'" + str(site) + "'" for site in sites_sw_off])}) AND
                scenario_traffic = {scenario_traffic} AND
                scenario_optim = {scenario_optim} AND
                year = {year}      
        """

        # Execute the query and fetch data
        ts = time.time()
        affected_indices_data = self.execute(affected_indices_query)
        te = time.time() - ts
        print(f"Part1 Query execution time: {te} seconds")

        # Convert the data to a DataFrame
        affected_indices_df = pd.DataFrame(affected_indices_data, columns=['index'])

        # Extract the list of affected indices
        affected_indices = affected_indices_df['index'].tolist()

        ## Part2
        # Construct the query to fetch full data using the affected indices
        full_data_query = f"""
            SELECT ps.index, 
                ps.Site_ID, 
                ps.geo_user_tput_dl, 
                ps.geo_churn_prob, 
                ps.geo_cap_demand, 
                ps.geo_served_demand, 
                ps.geo_latent_demand, 
                ps.geo_rsrp, 
                ps.geo_cqi, 
                ps.COUNT_SAMPLES
            FROM pixel_sector ps
            WHERE 
                scenario_traffic = {scenario_traffic} AND
                scenario_optim = {scenario_optim} AND
                year = {year} AND
                ps.index IN ({','.join(["'" + idx + "'" for idx in affected_indices])})
        """

        # Measure execution time
        ts = time.time()
        sector_data = self.execute(full_data_query)
        te = time.time() - ts
        print(f"Part2 Query execution time: {te} seconds")

        # Convert the data to a DataFrame
        detailed_sector_df = pd.DataFrame(sector_data, columns=['index', 'Site_ID', 'geo_user_tput_dl', 'geo_churn_prob', 'geo_cap_demand', 'geo_served_demand', 'geo_latent_demand', 'geo_rsrp', 'geo_cqi', 'COUNT_SAMPLES'])

        ### END OF TEST




        print('Simulating Switch off')
        # Create unique pixels list and initialize sw_off_aggr DataFrame
        sw_off_pixels = detailed_sector_df['index'].unique()
        sw_off_aggr = pd.DataFrame(sw_off_pixels, columns=['index'])
        sw_off_aggr['coverage_loss'] = 0
        #sw_off_aggr['new_RSRP'] = 0 #it is inserted during join later
        #sw_off_aggr['new_CQI'] = 0 #it is inserted during join later
        sw_off_aggr['offload_coef'] = 0


        # Simulate site switch-offs and calculate new KPIs
        # Separate data into switch-off and remain categories
        switch_off_data = detailed_sector_df[detailed_sector_df['Site_ID'].isin(sites_sw_off)]
        remain_data = detailed_sector_df[~detailed_sector_df['Site_ID'].isin(sites_sw_off)]

        # Calculate aggregates for switch-off and remain data
        agg_switch_off = switch_off_data.groupby('index')['geo_served_demand'].sum().fillna(0).rename('traffic_sw_off')
        agg_remain = remain_data.groupby('index')['geo_served_demand'].sum().fillna(0).rename('traffic_remain')


        # Calculate weighted averages for RSRP and CQI
        weighted_rsrp = (remain_data['geo_rsrp'] * remain_data['COUNT_SAMPLES']).groupby(remain_data['index']).sum() / remain_data.groupby('index')['COUNT_SAMPLES'].sum()
        weighted_cqi = (remain_data['geo_cqi'] * remain_data['COUNT_SAMPLES']).groupby(remain_data['index']).sum() / remain_data.groupby('index')['COUNT_SAMPLES'].sum()

        # Combine aggregates into sw_off_aggr DataFrame
        sw_off_aggr = sw_off_aggr.set_index('index')
        sw_off_aggr = sw_off_aggr.join([agg_switch_off, agg_remain, weighted_rsrp.rename('new_RSRP'), weighted_cqi.rename('new_CQI')])
        sw_off_aggr['traffic_remain'].fillna(0, inplace=True)    

        sw_off_aggr['traffic_sw_off'] = sw_off_aggr['traffic_sw_off'].astype(float)
        sw_off_aggr['traffic_remain'] = sw_off_aggr['traffic_remain'].astype(float)

        # Calculate offload_coef and coverage_loss
        # Calculate 'offload_coef' only where 'traffic_remain' is greater than 0
        #print('sw_off_aggr.dtypes\n', sw_off_aggr.dtypes)
        sw_off_aggr.to_csv('temp_code/sw_off_aggr.csv')

        sw_off_aggr['offload_coef'] = np.where(
            sw_off_aggr['traffic_remain'] > 0, 
            (sw_off_aggr['traffic_sw_off'].fillna(0) + sw_off_aggr['traffic_remain']) / sw_off_aggr['traffic_remain'], 
            0
        )

        sw_off_aggr['coverage_loss'] = 0
        sw_off_aggr.loc[(sw_off_aggr['traffic_remain'] == 0) | sw_off_aggr['traffic_remain'].isna(), 'coverage_loss'] = 1
        sw_off_aggr['offload_coef'].fillna(0, inplace=True)  # Handle division by zero

        #print('sw_off_aggr', sw_off_aggr)
        #sw_off_aggr.to_csv('temp_code/sw_off_aggr.csv', index=False)
        print('Done\n')

        print('Fetch aggregated data from pixel_agg')
        # Fetch aggregated data from pixel_agg
        agg_query = f"""
        SELECT index, latitude_50, longitude_50, geo_rsrp, geo_cqi, {kpi}
        FROM pixel_agg
        WHERE 
            scenario_traffic = {scenario_traffic} AND
            scenario_optim = {scenario_optim} AND
            year = {year} AND
            geo_rsrp >= {rsrp_min} AND
            geo_rsrp <= {rsrp_max} AND
            geo_cqi >= {cqi_min} AND
            geo_cqi <= {cqi_max}
        """

        
        ts=time.time()
        data_agg=self.execute(agg_query)
        df = pd.DataFrame(data_agg, columns=['index', 'latitude_50', 'longitude_50', 'geo_rsrp', 'geo_cqi', 'kpi'])
        print('Fetched df:', df)
        te=time.time()-ts
        #print('Fetched df:', df)
        print('Done, SQL time is:,', te)

        #print('---> sw_off_aggr:', sw_off_aggr)
        # Adjust data in df for switched-off pixels
        print('Adjusting data in df for switched-off pixels')
        df = df.merge(sw_off_aggr, on='index', how='left')

        #print('df datatypes:\n', df.dtypes)

        #df.to_csv('temp_code/df_before.csv')
        # Logic for adjusting the KPIs
        df['kpi']=df['kpi'].astype(float)
        df.loc[df['coverage_loss'] == 1, 'kpi'] = None
        df.loc[df['coverage_loss'] == 1, 'coverage_loss'] = 1
        if kpi == 'geo_user_tput_dl':
            df.loc[df['coverage_loss'] == 0, 'kpi'] /= df['offload_coef']
        if kpi == 'geo_churn_prob':
            df.loc[df['coverage_loss'] == 0, 'kpi'] *= df['offload_coef']
        if kpi == 'geo_served_demand':
            df.loc[df['coverage_loss'] == 0, 'kpi'] *= df['offload_coef']
        if kpi == 'geo_latent_demand':
            df.loc[df['coverage_loss'] == 0, 'kpi'] *= df['offload_coef']
        if kpi == 'geo_revenue_potential':
            df.loc[df['coverage_loss'] == 0, 'kpi'] *= df['offload_coef']
        if kpi == 'geo_rsrp':
            df.loc[df['coverage_loss'] == 0, 'kpi'] = df['new_RSRP']
        if kpi == 'geo_cqi':
            df.loc[df['coverage_loss'] == 0, 'kpi'] = df['new_CQI']

        #df.to_csv('temp_code/df_after.csv')

        #print('---> output df:', df['kpi'])

        return df

    


