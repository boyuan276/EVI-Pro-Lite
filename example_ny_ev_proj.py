# example_ny_ev_proj.py
# Example of EV charging demand projection for New York State.
# This is a multi-threaded version of example_ny_ev_proj.ipynb.

# %% Import packages

import os
import sys
import numpy as np
import pandas as pd
from EVIProLite_LoadPlotting import (temp_run, loadPlotting)
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging


def county_run(temp_csv, scenario_csv, api_key):
    """
    Run the EVI-Pro Lite model for a county.

    Parameters
    ----------
    temp_csv : pandas.DataFrame
        Hourly temperature data.
    scenario_csv : pandas.DataFrame
        Scenario data.
    api_key : str
        NREL API key.

    Returns
    -------
    final_result : pandas.DataFrame
        15 min EV charging demand.
    """
    
    temp_csv['date'] = temp_csv['date'].dt.date
    # Saturday and Sunday are 5 and 6, Monday is 0. <5 is weekday
    temp_csv['weekday'] = temp_csv['date'].apply(lambda x: x.weekday())#<5)
    temp_csv['temp_c'] = temp_csv['temperature']
    temp_csv.drop('temperature',axis = 1,inplace=True)

    # Run the model
    final_result = temp_run(scenario_csv,temp_csv,api_key)
    
    return final_result


if __name__ == '__main__':
    # %% Set up logging

    start = time.time()
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # %% Set up directories

    cwd = os.getcwd()
    input_dir = os.path.join(cwd, 'InputData')
    output_dir = os.path.join(cwd, 'OutputData')
    fig_dir = os.path.join(cwd, 'Figures')
    if not os.path.exists(input_dir):
        logging.critical('Input directory does not exist: {}'.format(input_dir))
        sys.exit()

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info('Created directory: {}'.format(output_dir))

    if not os.path.exists(fig_dir):
        os.makedirs(fig_dir)
        logging.info('Created directory: {}'.format(fig_dir))

    logging.info('Input directory: {}'.format(input_dir))
    logging.info('Output directory: {}'.format(output_dir))
    logging.info('Figure directory: {}'.format(fig_dir))

    # %% Set up API key

    api_key_file = 'nrel_api_key.txt'

    if os.path.isfile(api_key_file):
        # Read API key from file
        with open(api_key_file, 'r') as f:
            api_key = f.read()
            logging.info('API key found in file.')
    else:
        logging.warning('API key not found in file. Please enter your API key below.')
        api_key = input('Enter API key: ')

    # Check if API key is valid
    if len(api_key) != 40:
        logging.warning('API key is not valid. Use the demo key instead.')
        api_key = 'DEMO_KEY'

    # %% Read vehicle count data

    vehicle_by_county_proj = pd.read_csv(os.path.join(input_dir, 'vehicle_by_county_proj.csv'),
                                        index_col=0)

    # Use year 2035 as an example
    year = 2035
    vehicle_by_county_proj_year = vehicle_by_county_proj[str(year)]
    logging.info('Vehicle count data for year {}:'.format(year))

    # Rank counties in alphabetical order
    vehicle_by_county_proj_year = vehicle_by_county_proj_year.sort_index()

    # %% Read temperature data

    # Hourly air temperature data in 2018 from NREL ResStock
    temp_df = pd.read_csv(os.path.join(input_dir, 'resstock_amy2018_temp.csv'), 
                        index_col=0, parse_dates=True)

    # Parse county names and rename columns
    county_names = [county.split(',')[1] for county in temp_df.columns]
    county_names = [county.strip() for county in county_names]
    temp_df.columns = county_names
    
    # Calculate daily average temperature
    temp_df_daily = temp_df.resample('D').mean()

    # Only run for the first 10 days
    temp_df_daily = temp_df_daily.iloc[:10]

    # %% Run the model

    # Loop through the first three counties
    # NOTE: This is for demonstration purpose only.
    #       The full run will take a long time.
    #       NREL throttles the API calls to 1 call per second.
    #       A user can make at most 1,000 calls per hour.
    temp_csv_list = list()
    scenario_csv_list = list()

    for county in county_names:

            # Get daily average temperature for the county
            temp_csv = pd.DataFrame(temp_df_daily[county]).reset_index()
            temp_csv.columns = ['date', 'temperature']
            temp_csv_list.append(temp_csv)

            # Get scenario data for the county
            fleet_size = vehicle_by_county_proj_year[county]
            temp_c = temp_csv['temperature'].mean()

            # Example scenario: two PHEV types
            # User can change the scenario data here
            scenario_dict = {
                'fleet_size': [fleet_size]*2,
                'mean_dvmt': [35]*2,
                'temp_c': [temp_c]*2,
                'pev_type': ['PHEV20', 'PHEV50'],
                'pev_dist': ['EQUAL']*2,
                'class_dist': ['Equal']*2,
                'home_access_dist': ['HA75']*2,
                'home_power_dist': ['MostL1']*2,
                'work_power_dist': ['MostL2']*2,
                'pref_dist': ['Home60']*2,
                'res_charging': ['min_delay']*2,
                'work_charging': ['min_delay']*2,
            }
            scenario_csv = pd.DataFrame(scenario_dict)
            # Save scenario data to CSV
            scenario_csv.to_csv(os.path.join(output_dir, f'{county}_scenarios.csv'.replace(' ','_')))
            scenario_csv_list.append(scenario_csv)

    # Set up a thread pool with 10 threads
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(county_run, temp_csv, scenario_csv, api_key) 
                   for temp_csv, scenario_csv in zip(temp_csv_list, scenario_csv_list)]
        
        for i, future in enumerate(as_completed(futures)):
            final_result = future.result()

            # Plotting and Save CSVs with data
            county = county_names[i]
            scenario_csv = scenario_csv_list[i]
            for scenario,row in scenario_csv.iterrows():
                # Plot charging demand for the first week
                fig_name = os.path.join(fig_dir, f'{county}_scen{str(scenario)}_temp_gridLoad.png'.replace(' ','_'))
                loadPlotting(final_result, scenario, fig_name)

                # Save charging demand to CSV
                filename = os.path.join(output_dir, f'{county}_scen{str(scenario)}_temp_gridLoad.csv'.replace(' ','_'))
                final_result[scenario].to_csv(filename)

            logging.info('Finished running for county: {}'.format(county))
            
    end = time.time()
    logging.info('#############################################')
    logging.info('Finished running the model.')
    logging.info(f'Time elapsed: {end-start:.2f} seconds')
    logging.info('#############################################')