import numpy as np
import pandas as pd
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import rasterio
import branca.colormap as bm
from io import BytesIO



def create_legend_dict(cmap_str, vmin, vmax, values_num=6):
    #[Visualization, Used]
    """
    Create a legend dictionary based on given colormap and min-max range.

    Parameters:
    - cmap_str: String name of the colormap
    - vmin: Minimum value of the range
    - vmax: Maximum value of the range

    Returns:
    - A legend dictionary with labels and respective hex color codes
    """

    # Initialize colormap
    cmap = cm.get_cmap(cmap_str)

    # Create a normalization object
    norm = mcolors.Normalize(vmin=vmin, vmax=vmax)

    # Define the range and intervals
    values = np.linspace(vmin, vmax, values_num)

    # Create legend dictionary
    legend_dict = {}
    for val in values:
        color = mcolors.rgb2hex(cmap(norm(val)))
        legend_dict[f"{val:.2f}"] = color.lstrip("#")
    #print(legend_dict)
    return legend_dict

def get_visualization_params(selection_tab):
    #[Visualization/Folium class]
    #Function taking selection tab value as the input and returning vmin, vmax, cmap, legend_title for visualization
    params_dict = {
        'QoE': {
            'nodata': 0.0,
            'vmin': 0.0,
            'vmax': 13,
            'cmap': 'RdYlGn',
            'legend_title': 'User DL Tput (Mbps)'
        },
        'Churn': {
            'nodata': 0.01,
            'vmin': 0.01,
            'vmax': 0.5,
            'cmap': 'Reds',
            'legend_title': 'Churn Probability (%)'
        },
        'Served Demand': {
            'nodata': 0,
            'vmin': 0,
            'vmax': 2,
            'cmap': 'Greens',
            'legend_title': 'Served Traffic (GB per 50x50m)'
        },
        'Latent Demand': {
            'nodata': 0,
            'vmin': 0,
            'vmax': 0.2,
            'cmap': 'Reds',
            'legend_title': 'Latent Demand (GB per 50x50m)'
        },
        'Revenue Potential': {
            'nodata': 0,
            'vmin': 50,
            'vmax': 2000,
            'cmap': 'Reds',
            'legend_title': 'Revenue Potential (USD per 50x50m)'
        },
        'Capacity Management': {
            'nodata': 0,
            'vmin': -0.3,
            'vmax': 0.3,
            'cmap': 'coolwarm',
            'legend_title': 'Capacity BW\n Demand (Mhz)'
        },
        'Coverage Signal Level': {
            'nodata': 0,
            'vmin': -115,
            'vmax': -60,
            'cmap': 'RdYlGn',
            'legend_title': 'RSRP (dBm)'
        },
        'Coverage Quality': {
            'nodata': 0,
            'vmin': 5,
            'vmax': 13,
            'cmap': 'RdYlGn',
            'legend_title': 'CQI'
        },
        'ROI': {
            'nodata': 0,
            'vmin': 0,
            'vmax': 5,
            'cmap': 'RdYlGn',
            'legend_title': 'ROI'
        },
        'NPV': {
            'nodata': 0,
            'vmin': -100000,
            'vmax': 100000,
            'cmap': 'RdYlGn',
            'legend_title': 'Net Present Value (USD)'
        },
        'TTC': {
            'nodata': 0,
            'vmin': 0,
            'vmax': 12,
            'cmap': 'RdYlGn',
            'legend_title': 'Time To Congestions (Months)'
        }
    }

    return params_dict.get(selection_tab, "Invalid Selection")

def get_visualization_params_kpi(kpi):
    #Similar function to the get_visualization_params but works with kpi input instead of selection tab input. This function is used in AI Chat
    #Function taking selection tab value as the input and returning vmin, vmax, cmap, legend_title for visualization
    params_dict = {
        'geo_user_tput_dl': {
            'nodata': 0.0,
            'vmin': 0.0,
            'vmax': 13,
            'cmap': 'RdYlGn',
            'legend_title': 'User DL Tput (Mbps)'
        },
        'geo_churn_prob': {
            'nodata': 0.01,
            'vmin': 0.01,
            'vmax': 0.5,
            'cmap': 'Reds',
            'legend_title': 'Churn Probability (%)'
        },
        'geo_served_demand': {
            'nodata': 0,
            'vmin': 0,
            'vmax': 2,
            'cmap': 'Greens',
            'legend_title': 'Served Traffic (GB per 50x50m)'
        },
        'geo_latent_demand': {
            'nodata': 0,
            'vmin': 0,
            'vmax': 0.2,
            'cmap': 'Reds',
            'legend_title': 'Latent Demand (GB per 50x50m)'
        },
        'geo_revenue_potential': {
            'nodata': 0,
            'vmin': 50,
            'vmax': 2000,
            'cmap': 'Reds',
            'legend_title': 'Revenue Potential (USD per 50x50m)'
        },
        'geo_cap_demand': {
            'nodata': 0,
            'vmin': -0.3,
            'vmax': 0.3,
            'cmap': 'coolwarm',
            'legend_title': 'Capacity BW\n Demand (Mhz)'
        },
        'geo_rsrp': {
            'nodata': 0,
            'vmin': -115,
            'vmax': -60,
            'cmap': 'RdYlGn',
            'legend_title': 'RSRP (dBm)'
        },
        'geo_cqi': {
            'nodata': 0,
            'vmin': 5,
            'vmax': 13,
            'cmap': 'RdYlGn',
            'legend_title': 'CQI'
        },
        'roi': {
            'nodata': 0,
            'vmin': 0,
            'vmax': 5,
            'cmap': 'RdYlGn',
            'legend_title': 'ROI'
        },
        'npv': {
            'nodata': 0,
            'vmin': -100000,
            'vmax': 100000,
            'cmap': 'RdYlGn',
            'legend_title': 'Net Present Value (USD)'
        },
        'ttc': {
            'nodata': 0,
            'vmin': 0,
            'vmax': 12,
            'cmap': 'RdYlGn',
            'legend_title': 'Time To Congestions (Months)'
        }
    }

    return params_dict.get(kpi, "Invalid Selection")



def raster_transform_django_test(raster_file, vmin, vmax, cmap_name, n_legend_entries, no_data):
    # This is temp copy of raster_transform function used in django test
    # It returns colorized raster as a file and its path
    # [Visualization/Folium]
    # Function returning colorized raster for the folium map engine
    # It returns colored_raster_as_a_file, colored raster_as_a_path, legend, and bounds

    # Read the raster data
    with rasterio.open(raster_file) as src:
        raster_data = src.read(1)  # Read the first band
        
        # Get bounds from the raster file
        bounds = src.bounds
        ymin, xmin, ymax, xmax = bounds.bottom, bounds.left, bounds.top, bounds.right

        ### TEMP: 3 and 4 channel rasters
        transform = src.transform  # Get the affine transform (pixel to CRS)
        crs = src.crs  # Get the coordinate reference system
        ### END TEMP

    # Ensure the raster data is a numpy array with a numeric type
    raster_data = np.array(raster_data, dtype=float)  # Convert to float for safe operations

    # Replace no_data values with NaN before clipping
    raster_data[raster_data == no_data] = np.nan

    # Clipping raster values above and below the max and min
    raster_data = np.clip(raster_data, vmin, vmax)
    #print_raster_value_distribution(raster_data, 15)

    # Normalize the raster
    normalized_raster = (raster_data - vmin) / (vmax - vmin)

    # Increase the number of colors in the colormap
    #n_colors = 256  # Increase this number for higher resolution
    #cmap = cm.get_cmap(cmap_name, n_colors)
    #colored_raster = cmap(normalized_raster)

    # Apply colormap
    cmap = cm.get_cmap(cmap_name)
    colored_raster = cmap(normalized_raster)

    # Handle NaN values by setting them to transparent
    nan_mask = np.isnan(normalized_raster)
    colored_raster[nan_mask] = [0, 0, 0, 0]  # RGBA for transparent

    ### TEMP Save colored rasters (3 and 4 channels)
    def save_colored_raster_disk(colored_raster, output_file, crs, transform, bands):
        # Determine the data type and format
        height, width = colored_raster.shape[:2]
        dtype = rasterio.uint8
        with rasterio.open(
                output_file, 'w', driver='GTiff',
                height=height, width=width,
                count=bands, dtype=dtype,
                crs=crs, transform=transform) as dst:

            for i in range(bands):
                # Write each band; note that rasterio expects bands in the order 1, 2, 3,...
                dst.write((colored_raster[:, :, i] * 255).astype(dtype), i + 1)

    # Save colored raster to an in-memory file
    def save_colored_raster_to_memory(colored_raster, crs, transform, bands):
        height, width = colored_raster.shape[:2]
        dtype = rasterio.uint8
        memfile = BytesIO()
        with rasterio.open(
                memfile, 'w', driver='GTiff',
                height=height, width=width,
                count=bands, dtype=dtype,
                crs=crs, transform=transform) as dst:

            for i in range(bands):
                dst.write((colored_raster[:, :, i] * 255).astype(dtype), i + 1)
        
        memfile.seek(0)  # Rewind the file pointer to the beginning
        return memfile

    # save to memory option
    # Save colored raster to an in-memory file
    def save_colored_raster_to_memory(colored_raster, crs, transform, bands):
        height, width = colored_raster.shape[:2]
        dtype = rasterio.uint8
        memfile = BytesIO()
        with rasterio.open(
                memfile, 'w', driver='GTiff',encoding="utf-8",name='dismantle.tif',
                height=height, width=width,
                count=bands, dtype=dtype,
                crs=crs, transform=transform) as dst:

            for i in range(bands):
                dst.write((colored_raster[:, :, i] * 255).astype(dtype), i + 1)
        
        memfile.seek(0)  # Rewind the file pointer to the beginning
        return memfile


    # Save the raster to memory
    memfile_rgb = save_colored_raster_to_memory(colored_raster, crs, transform, 3)
    memfile_rgba = save_colored_raster_to_memory(colored_raster, crs, transform, 4)

    
    # disk save option
    # print('debug raster_transform: saving 3 and 4 channel rasters')
    # output_rgb_file = 'data/temp_rasters/raster_test_output_rgb.tif'
    # output_rgba_file = 'data/temp_rasters/raster_test_output_rgba.tif'
    # save_colored_raster(colored_raster, output_rgb_file, crs, transform, 3)
    # save_colored_raster(colored_raster, output_rgba_file, crs, transform, 4)
    # print('debug raster_transform: saving complete')
    #

    ### END TEMP section

    # Create legend
    legend = {}
    for i in range(n_legend_entries):
        value = vmin + (vmax - vmin) * i / (n_legend_entries - 1)
        color = cmap(i / (n_legend_entries - 1))
        legend[f"{value:.2f}"] = color

    return memfile_rgb, memfile_rgba, legend, [[ymin, xmin], [ymax, xmax]]




def raster_transform(raster_file, vmin, vmax, cmap_name, n_legend_entries, no_data):
    # [Visualization/Folium]
    # Function returning colorized raster for the folium map engine
    # It returns colored_raster_as_a_file, colored raster_as_a_path, legend, and bounds

    # Read the raster data
    with rasterio.open(raster_file) as src:
        raster_data = src.read(1)  # Read the first band
        
        # Get bounds from the raster file
        bounds = src.bounds
        ymin, xmin, ymax, xmax = bounds.bottom, bounds.left, bounds.top, bounds.right

        ### TEMP: 3 and 4 channel rasters
        transform = src.transform  # Get the affine transform (pixel to CRS)
        crs = src.crs  # Get the coordinate reference system
        ### END TEMP

    # Ensure the raster data is a numpy array with a numeric type
    raster_data = np.array(raster_data, dtype=float)  # Convert to float for safe operations

    # Replace no_data values with NaN before clipping
    raster_data[raster_data == no_data] = np.nan

    # Clipping raster values above and below the max and min
    raster_data = np.clip(raster_data, vmin, vmax)
    #print_raster_value_distribution(raster_data, 15)

    # Normalize the raster
    normalized_raster = (raster_data - vmin) / (vmax - vmin)

    # Increase the number of colors in the colormap
    #n_colors = 256  # Increase this number for higher resolution
    #cmap = cm.get_cmap(cmap_name, n_colors)
    #colored_raster = cmap(normalized_raster)

    # Apply colormap
    cmap = cm.get_cmap(cmap_name)
    colored_raster = cmap(normalized_raster)

    # Handle NaN values by setting them to transparent
    nan_mask = np.isnan(normalized_raster)
    colored_raster[nan_mask] = [0, 0, 0, 0]  # RGBA for transparent

    ### TEMP Save colored rasters (3 and 4 channels)
    def save_colored_raster(colored_raster, output_file, crs, transform, bands):
        # Determine the data type and format
        height, width = colored_raster.shape[:2]
        dtype = rasterio.uint8
        with rasterio.open(
                output_file, 'w', driver='GTiff',
                height=height, width=width,
                count=bands, dtype=dtype,
                crs=crs, transform=transform) as dst:

            for i in range(bands):
                # Write each band; note that rasterio expects bands in the order 1, 2, 3,...
                dst.write((colored_raster[:, :, i] * 255).astype(dtype), i + 1)

    #print('debug raster_transform: saving 3 and 4 channel rasters')
    #output_rgb_file = 'data/temp_rasters/raster_test_output_rgb.tif'
    #output_rgba_file = 'data/temp_rasters/raster_test_output_rgba.tif'
    #save_colored_raster(colored_raster, output_rgb_file, crs, transform, 3)
    #save_colored_raster(colored_raster, output_rgba_file, crs, transform, 4)
    #print('debug raster_transform: saving complete')

    ### END TEMP section

    # Create legend
    legend = {}
    for i in range(n_legend_entries):
        value = vmin + (vmax - vmin) * i / (n_legend_entries - 1)
        color = cmap(i / (n_legend_entries - 1))
        legend[f"{value:.2f}"] = color

    return colored_raster, legend, [[ymin, xmin], [ymax, xmax]]


def get_folium_legend(cmap_name, vmin, vmax):
    #THis function returns legend for folium (based on provided cmap_name, vmin, vmax)
    #Notice: coolwarm colormap is created manually, its not using any public colormap
    #[Visualization/Folium class]

    # Approximate colors for 'coolwarm' colormap
    coolwarm_colors = ['#3b4cc0', '#6b8bd4', '#b2c8df', '#f0f0f0', '#fbb6ac', '#e67f83', '#b40426']

    capacity_params = get_visualization_params('Capacity Management')

    coolwarm_colormap = bm.LinearColormap(
        colors=coolwarm_colors,
        vmin = capacity_params['vmin'],
        vmax = capacity_params['vmax']
        )

    colormap_dict = {
        'RdYlGn': bm.linear.RdYlGn_09,
        'Reds': bm.linear.Reds_09,
        'Greens': bm.linear.Greens_09,
        'coolwarm': coolwarm_colormap,  # Approximation
        # More mappings can be added as needed
        }

    colormap = colormap_dict.get(cmap_name, bm.linear.RdYlGn_09) #RdYlGn_09 is default if something is wrong
    return colormap.scale(vmin, vmax)



###NOT USED FUNCTIONS:   

def get_bbox_coordinates(df, zoom):
    # Assuming the first column is latitude and the second is longitude
    # [Vizualization. Not used after switching to folium]
    latitude = df.iloc[:, 0]
    longitude = df.iloc[:, 1]

    # Extracting the bounding box coordinates
    xmin_i = longitude.min()
    xmax_i = longitude.max()
    ymin_i = latitude.min()
    ymax_i = latitude.max()

    scaling_factor = zoom
    xmin = (xmax_i + xmin_i) / 2 - scaling_factor / 100 * (((xmax_i + xmin_i) / 2) - xmin_i)
    xmax = (xmax_i + xmin_i) / 2 + scaling_factor / 100 * ((xmax_i - (xmax_i + xmin_i) / 2))
    ymin = (ymax_i + ymin_i) / 2 - scaling_factor / 100 * (((ymax_i + ymin_i) / 2) - ymin_i)
    ymax = (ymax_i + ymin_i) / 2 + scaling_factor / 100 * ((ymax_i - (ymax_i + ymin_i) / 2))

    return [xmin, ymin, xmax, ymax]

def get_bbox_ave_coordinates(df, zoom):
    # Picking up latitude and longitude based on the column names
    # [Vizualization. Not used after switching to folium]
    latitude = df['latitude_50'].astype(float)
    longitude = df['longitude_50'].astype(float)

    # Extracting the bounding box coordinates
    xmin_i = longitude[longitude <= longitude.quantile(0.2)].mean()
    xmax_i = longitude[longitude > longitude.quantile(0.8)].mean()
    ymin_i = latitude[latitude <= latitude.quantile(0.2)].mean()
    ymax_i = latitude[latitude > latitude.quantile(0.8)].mean()
    xave_i = longitude.mean()
    yave_i = latitude.mean()

    scaling_factor = zoom
    xmin = xave_i - scaling_factor / 100 * (xave_i - xmin_i)
    xmax = xave_i + scaling_factor / 100 * (xmax_i - xave_i)
    ymin = yave_i - scaling_factor / 100 * (yave_i - ymin_i)
    ymax = yave_i + scaling_factor / 100 * (ymax_i - yave_i)

    return [xmin, ymin, xmax, ymax]



def get_bbox_ave_coordinates_compet(df, zoom):
    # Picking up latitude and longitude based on the column names
    # [Visualization, Not Used, keep]

    latitude = df['Latitude']
    longitude = df['Longitude']

    # Extracting the bounding box coordinates
    xmin_i = longitude[longitude <= longitude.quantile(0.2)].mean()
    xmax_i = longitude[longitude > longitude.quantile(0.8)].mean()
    ymin_i = latitude[latitude <= latitude.quantile(0.2)].mean()
    ymax_i = latitude[latitude > latitude.quantile(0.8)].mean()
    xave_i = longitude.mean()
    yave_i = latitude.mean()

    scaling_factor = zoom
    xmin = xave_i - scaling_factor / 100 * (xave_i - xmin_i)
    xmax = xave_i + scaling_factor / 100 * (xmax_i - xave_i)
    ymin = yave_i - scaling_factor / 100 * (yave_i - ymin_i)
    ymax = yave_i + scaling_factor / 100 * (ymax_i - yave_i)

    return [xmin, ymin, xmax, ymax]


def print_raster_value_distribution(raster_data, num_bins):
    ##Temp function when I was troubleshooting poor colors in cmap.
    # [Visualization/others NOT USED]
    # Prints distribution of the raster values
    """
    Prints the distribution of raster values based on specified number of bins.

    :param raster_data: numpy array containing raster data.
    :param num_bins: Number of bins for the distribution.
    """

    # Check data type
    print(type(raster_data))
    print(raster_data.dtype)

    # Find min and max values in the raster data
    vmin = np.nanmin(raster_data)
    vmax = np.nanmax(raster_data)
    print(vmin)
    print(vmax)

    # Define bins based on the vmin, vmax, and num_bins
    bins = np.linspace(vmin, vmax, num_bins + 1)

    # Calculate histogram
    hist, bin_edges = np.histogram(raster_data, bins)

    # Print the distribution
    print("Raster values distribution:")
    for i in range(len(hist)):
        print(f"Range {bin_edges[i]:.2f} to {bin_edges[i+1]:.2f}: {hist[i]}")

