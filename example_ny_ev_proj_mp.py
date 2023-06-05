# example_ny_ev_proj.py
# Example of EV charging demand projection for New York State.
# This is a multithreading version of example_ny_ev_proj.ipynb.

# %% Import packages

import os
import sys
import numpy as np
import pandas as pd
from EVIProLite_LoadPlotting import (temp_run, loadPlotting)
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging


def county_run(temp_csv, scenario_csv, api_key, county):
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
    county : str
        County name.

    Returns
    -------
    final_result : pandas.DataFrame
        15 min EV charging demand.
    """

    # Handle temperature data
    temp_csv['date'] = pd.to_datetime(temp_csv['date']).dt.date
    # Saturday and Sunday are 5 and 6, Monday is 0. <5 is weekday
    temp_csv['weekday'] = temp_csv['date'].apply(lambda x: x.weekday())  # <5)
    temp_csv['temp_c'] = temp_csv['temperature']
    temp_csv.drop('temperature', axis=1, inplace=True)

    # Handle small fleet size
    scaling_factor = np.ones(len(scenario_csv))
    for scenario, row in scenario_csv.iterrows():
        if row['fleet_size'] < 30000:
            scenario_csv.loc[scenario, 'fleet_size'] = 10000
            scaling_factor[scenario] = row['fleet_size'] / 10000
            logging.warning(f"Scenario {scenario}: Fleet size of {county} is too small: {row['fleet_size']}.")
            logging.warning(f"Set fleet size to 10000 and scale the results by {scaling_factor[scenario]}.")

    # Run the model
    logging.debug(f'Running with API key: {api_key}')
    final_result = temp_run(scenario_csv, temp_csv, api_key, county=county)

    # Scale the results
    for scenario, row in scenario_csv.iterrows():
        if scaling_factor[scenario] != 1:
            final_result[scenario][['home_l1', 'home_l2', 'work_l1', 'work_l2', 'public_l2', 'public_l3']] = \
                final_result[scenario][['home_l1', 'home_l2', 'work_l1', 'work_l2', 'public_l2', 'public_l3']] * \
                scaling_factor[scenario]

    return final_result


if __name__ == '__main__':
    # %% Set up logging

    start = time.time()
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.captureWarnings(True)

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
    api_key_list = list()

    if os.path.isfile(api_key_file):
        # Read API key from file
        with open(api_key_file, 'r') as f:
            # Read each line
            for line in f:
                # Check if API key is valid
                if line.strip() and len(line.strip()) == 40:
                    api_key_list.append(line.strip())
                else:
                    logging.warning(f'Invalid API key: {line.strip()}')
        n_api_key = len(api_key_list)
        if n_api_key > 0:
            logging.info(f'{n_api_key} API keys found in file.')
        else:
            logging.warning('No valid API key found in file. Please enter your API key below.')
            api_key = input('Enter API key: ')
            # Check if API key is valid
            if len(api_key) != 40:
                logging.critical('Invalid API key. Please check your API key.')
                sys.exit()
            else:
                api_key_list.append(api_key)
                n_api_key = 1
    else:
        logging.warning('API key not found in file. Please enter your API key below.')
        api_key = input('Enter API key: ')
        # Check if API key is valid
        if len(api_key) != 40:
            logging.critical('Invalid API key. Please check your API key.')
            sys.exit()
        else:
            api_key_list.append(api_key)
            n_api_key = 1

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

    # Loop through the months
    # NOTE: This is only for reducing computational cost.
    for m in range(12, 13):
        temp_df_daily_m = temp_df_daily[temp_df_daily.index.month == m]

        # %% Run the model

        # Loop through the first three counties
        # NOTE: NREL throttles the API calls to 1 call per second.
        #       A user can make at most 1,000 calls per hour.
        temp_csv_list = list()
        scenario_csv_list = list()

        for county in county_names:
            # Get daily average temperature for the county
            temp_csv = pd.DataFrame(temp_df_daily_m[county]).reset_index()
            temp_csv.columns = ['date', 'temperature']
            # temp_csv['date'] = pd.to_datetime(temp_csv['date'])
            temp_csv_list.append(temp_csv)

            # Get scenario data for the county
            fleet_size = vehicle_by_county_proj_year[county]
            temp_c = temp_csv['temperature'].mean()

            # Example scenario: two PHEV types
            # User can change the scenario data here
            scenario_dict = {
                'fleet_size': [fleet_size] * 2,
                'mean_dvmt': [35] * 2,
                'temp_c': [temp_c] * 2,
                'pev_type': ['PHEV50'] * 2,
                'pev_dist': ['EQUAL'] * 2,
                'class_dist': ['Equal'] * 2,
                'home_access_dist': ['HA75'] * 2,
                'home_power_dist': ['MostL1'] * 2,
                'work_power_dist': ['MostL2'] * 2,
                'pref_dist': ['Home60'] * 2,
                'res_charging': ['min_delay', 'max_delay'],
                'work_charging': ['min_delay'] * 2,
            }
            scenario_csv = pd.DataFrame(scenario_dict)
            # Save scenario data to CSV
            scenario_csv.to_csv(os.path.join(output_dir, f'{county}_month{m}_scenarios.csv'.replace(' ', '_')))
            scenario_csv_list.append(scenario_csv)

        # Set up a thread pool with 10 threads
        with ThreadPoolExecutor(max_workers=10) as executor:

            futures = list()
            for i, (temp_csv, scenario_csv) in enumerate(zip(temp_csv_list, scenario_csv_list)):
                # Switch to the next API key for every call
                api_key_index = i % n_api_key
                api_key = api_key_list[api_key_index]

                # Submit the task to the executor with the assigned API key
                futures.append(executor.submit(county_run, temp_csv, scenario_csv, api_key, county_names[i]))

            for i, future in enumerate(as_completed(futures)):
                final_result = future.result()

                # Plotting and Save CSVs with data
                county = county_names[i]
                scenario_csv = scenario_csv_list[i]

                for scenario, row in scenario_csv.iterrows():
                    # Plot charging demand for the first week
                    fig_name = os.path.join(fig_dir,
                                            f'{county}_month{m}_scen{str(scenario)}_temp_gridLoad.png'.replace(' ',
                                                                                                               '_'))
                    loadPlotting(final_result, scenario, fig_name)

                    # Save charging demand to CSV
                    filename = os.path.join(output_dir,
                                            f'{county}_month{m}_scen{str(scenario)}_temp_gridLoad.csv'.replace(' ',
                                                                                                               '_'))
                    final_result[scenario].to_csv(filename)

                logging.info(f'Finished running for county: {county}')
        logging.info(f'Finished running the model for month {m}.')

    end = time.time()
    logging.info('#############################################')
    logging.info('Finished running the model.')
    logging.info(f'Time elapsed: {end - start:.2f} seconds')
    logging.info('#############################################')
