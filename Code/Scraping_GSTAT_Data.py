import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, unquote
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import datetime
import logging

"""
We configure logging using basicConfig() to set the logging level to INFO. 
This means that only messages with severity level INFO and higher will be logged.
"""
logging.basicConfig(level=logging.INFO)

def download_gstat_xlsx_file(save_directory, archive_directory, start_year):
    """
    Download an Excel file (.xlsx) from the GSTAT Quarterly Statistics page in current working directory if doesn't exist in Archive directory

    """
    # Ensure the archive directory exists
    if not os.path.exists(archive_directory):
        os.makedirs(archive_directory)
        logging.info(f"Created archive directory: {archive_directory}")
    
    try:
        # URL of the SAMA Monthly Statistics page "https://www.stats.gov.sa/ar/1250"
        headers = {'user-agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}

        # Get the current year
        current_year = datetime.now().year

        file_pattern = 'https://www.stats.gov.sa/sites/default/files/ITR%20Q{quarter}{year}A.xlsx'

        # Initialize a list to store the generated URLs
        generated_urls = ['https://www.stats.gov.sa/sites/default/files/International%20Trade%2C%20Third%20Quarter%202021Ar.xlsx',]

        # Loop through the years from start_year to the current year
        for year in range(start_year, current_year + 1):
            if year == start_year:
                # Format the URL with the year and quarter
                url = file_pattern.format(year=year, quarter=4)
                generated_urls.append(url)
            else:
                # Loop through the quarters (1 to 4)
                for quarter in range(1, 5):
                    # Format the URL with the year and quarter
                    url = file_pattern.format(year=year, quarter=quarter)
                    generated_urls.append(url)

        for link in generated_urls:
                archive_found = False # change to true if file exist in Archive directory

                file_name = link.split('/')[-1]
                # Decode URL-encoded file name if necessary
                file_name = unquote(file_name)

                # Define the local file path and archive file path
            
                local_file_path = os.path.join(save_directory, file_name)
                archive_file_path = os.path.join(archive_directory, file_name) 

                # Check if file already exists in archive
                if os.path.exists(archive_file_path):
                    logging.info(f"File already exists in archive. Skipping download.")
                    archive_found = True

                elif archive_found==False: 
                    try:
                        # Download the file inside the current working directory
                        response = requests.get(link, headers=headers)

                        if response.status_code == 200:
                            
                            with open(local_file_path, 'wb') as file:
                                file.write(response.content)

                            logging.info(f"File {file_name} downloaded successfully in: {local_file_path}")
                    except Exception as d:
                        logging.error(f"Failed with downloading {file_name}")

    except Exception as e:
        logging.error(e)    

#save in current working directory
save_directory = os.getcwd()
archive_directory = os.path.join(save_directory, 'Archive') # Join the current working directory with the subdirectory 'Archive'

start_year = 2021
# Call the function:
downloaded_file_name = download_gstat_xlsx_file(save_directory, archive_directory, start_year)
if downloaded_file_name:
    print(f"Downloaded file name: {downloaded_file_name}")





