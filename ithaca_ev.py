# %% Import packages

import os
import numpy as np
import pandas as pd
import EVIProLite_LoadPlotting

# %% Set input paths

scen_file = 'InputData/Scenarios_test.csv'
temp_file = 'InputData/ShortTemps_test.csv'
api_key_file = 'nrel_api_key.txt'

if os.path.isfile(api_key_file):
    # Read API key from file
    with open(api_key_file, 'r') as f:
        api_key = f.read()
        print('API key found in file.')
else:
    print('API key not found in file. Please enter your API key below.')
    api_key = input('Enter API key: ')

# Check if API key is valid
if len(api_key) != 40:
    Warning('API key is not valid. Use the demo key instead.')
    api_key = 'DEMO_KEY'

# %% Construct scenario data frame

"""
TODO: Loop through all counties in NY
TODO: Get population in each county and calculate vehicle population in future years
TODO: Calculate vehicle miles traveled in future years
TODO: Get future year vehicle count from NYSERDA

"""




# %% Run EVIProLite_LoadPlotting

# TODO: reload the run function to read data from dataframes instead of csv files

if __name__ == '__main__':
    EVIProLite_LoadPlotting.run(scenario_path=scen_file,
                                temp_path=temp_file,
                                api_key=api_key)