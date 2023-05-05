# NYS_population_project.py

# %% Import packages
import os
import sys
import time
import requests
import logging
import concurrent.futures
import threading

thread_local = threading.local()

# %% Downloader functions

def get_session():
    """Create a thread-local session object"""
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

def download_site(url):

    session = get_session()
    with session.get(url) as response:
        # Write the result to a file
        with open(os.path.join(data_dir, f"{url.split('=')[-1]}.xlsx"), 'wb') as f:
            f.write(response.content)

def download_all_sites(sites):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_site, sites)


if __name__ == "__main__":

    # %% Set up logging
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.debug('Start of program')

    # %% Set working directory
    cwd = os.getcwd()
    data_dir = os.path.join(cwd, 'Population_Data')
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    logging.debug('Set working directory')

    # %% Download data
    # Download data from Cornell Program on Applied Demographics
    # https://pad.human.cornell.edu/counties/expprojdata.cfm?

    # Set base url
    base_url = 'https://pad.human.cornell.edu/counties/expprojdata.cfm?'

    # Create a list of urls to download
    urls = [f'{base_url}+county={id}' for id in range(1, 125, 2)]

    start_time = time.time()
    download_all_sites(urls)
    duration = time.time() - start_time
    print(f"Downloaded {len(urls)} in {duration} seconds")

