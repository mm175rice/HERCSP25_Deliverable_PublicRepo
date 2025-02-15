"""
Comprehensive Texas Academic Performance Report (TAPR) Data Scraper
This scraper allows users to select the level (Campus, District, Region, State) and type of data they would like to download from the TAPR data download on the TEA website. If the level is "D" for District, district type data will also be downloaded in addition to the TAPR data unless the user has indicated they do not want the data (set dist_type = False). 

If the files already exist, the scraper will not download new files. 

The scraper creates separate folders for each year of data and names the files with the appropriate year. 

"""
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import os
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import streamlit as st


#District Type Scraper (Helper Function)
def district_type_scraper(year):
    """
    Scrapes the Texas Education Agency (TEA) website for district type data of a given school year.

    Args:
        year (int): The ending year of the school year (e.g., 2024 for the 2023-24 school year).

    Returns:
        pd.DataFrame: A DataFrame containing data from the specified school year's district type Excel file, 
                      or None if no file is found.

    Raises:
        requests.exceptions.RequestException: If there's an issue fetching the webpage.
    """
    school_year = f"{year-1}-{year-2000}"  # Convert to academic year format (e.g., "2023-24")
    url = f"https://tea.texas.gov/reports-and-data/school-data/district-type-data-search/district-type-{school_year}"

    try:
        # Attempt to get the webpage
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Warning: Could not access {url} (Status Code: {response.status_code})")
            return None  # Skip this year if the page is unavailable
        
        soup = BeautifulSoup(response.text, 'html.parser')

        # Look for Excel file links
        for link in soup.find_all("a", href=True):
            file_url = link.get('href')
            if file_url and re.search(r".xlsx$", file_url):
                full_url = f"https://tea.texas.gov{file_url}" if file_url.startswith("/") else file_url
                print(f"Found Excel file: {full_url}")
                return pd.read_excel(full_url, sheet_name=2)  # Read from the correct sheet
        
        print(f"Warning: No Excel file found on {url}")
        return None  # No file found, return None instead of breaking

    except requests.exceptions.RequestException as e:
        print(f"Error fetching district type data for {year}: {e}")
        return None
      
#Detects if files download within a given time limit, otherwise timesout (Helper Function)
def wait_for_downloads(variables, year, directory, timeout=200):
    """
    Waits for the expected data files to be downloaded within a specified timeout period.

    Args:
        variables (list of str): A list of variable names that determine expected file names.
        year (int): The year of the dataset, affecting file format (.dat for <2021, .csv for >=2021).
        directory (str): The directory where the files are expected to be downloaded.
        timeout (int, optional): Maximum time in seconds to wait for all files to be downloaded. Defaults to 200.

    Returns:
        bool: True if all expected files are downloaded within the timeout period, False otherwise.

    Raises:
        FileNotFoundError: If the specified directory does not exist.

    Notes:
        - The function checks for `.crdownload` files to ensure downloads are complete.
        - It waits in 5-second intervals before checking again.
        - If downloads complete within the timeout, a success message is printed.
    """
    start_time = time.time()  # Record the start time
    expected_files = []  # List to store expected file names based on year and variables

    # Determine expected file names based on the year
    for var in variables:
        if year < 2021:
            expected_files.append(f"DIST{var}.dat" if var != "REF" else "DREF.dat")
        else:
            expected_files.append(f"DIST{var}.csv" if var != "REF" else "DREF.csv")

    check = 1  # Variable to print waiting message only once
    while time.time() - start_time < timeout:  # Continue checking until timeout is reached
        downloaded_files = os.listdir(directory)  # Get the list of files in the directory

        # Check if all expected files are present and not still downloading (.crdownload files)
        if all(file in downloaded_files and not file.endswith(".crdownload") for file in expected_files):
            st.write(f"All downloads for {year} completed successfully.\n")
            return True  # Return True if all files are found and fully downloaded
        
        # Print waiting message only once at the start
        if check == 1:
            st.write("Waiting for all files to download...")
        check += 1

        time.sleep(5)  # Wait for 5 seconds before checking again

    return False  # Return False if the timeout is reached before all files are downloaded

#Renames files to include year in file name (Helper Function)
def file_renamer(directory, year, prefix, var, level):
    """
    Renames downloaded files in the specified directory based on naming conventions.

    Args:
        directory (str): The path to the directory containing the files.
        year (int): The year to be appended to the renamed files.
        prefix (str): The prefix used in some file names (e.g., 'DIST').
        var (str): The variable name (e.g., 'POP', 'ECON', 'REF').
        level (str): The level identifier (some files may use this instead of the prefix).

    Returns:
        None: The function renames files in place and does not return a value.

    Notes:
        - The function checks for `.csv` and `.dat` file extensions.
        - It looks for two possible naming patterns:
            1. `{prefix}{var}{ext}` (e.g., `DISTPOP.csv`)
            2. `{level}{var}{ext}` (e.g., `STATEPOP.dat`)
        - If a match is found, the file is renamed to:
            - `{level}{var}_{year}{ext}` for "REF" files.
            - `{prefix}{var}_{year}{ext}` for all other files.
        - The function **only renames the first matching file** and stops checking further.
    """
    for ext in ['.csv', '.dat']:  # Check both CSV and DAT file formats
        old_patterns = [
            f"{prefix}{var}{ext}",  # Pattern with prefix
            f"{level}{var}{ext}"     # Pattern with level (some files may not have prefix)
        ]

        for old_pattern in old_patterns:
            old_name = os.path.join(directory, old_pattern)  # Full path of the old file
            if os.path.exists(old_name):  # Check if file exists
                # Determine new name format
                if var == "REF":
                    new_name = os.path.join(directory, f"{level}{var}_{year}{ext}")
                else:
                    new_name = os.path.join(directory, f"{prefix}{var}_{year}{ext}")

                os.rename(old_name, new_name)  # Rename the file
                break  # Stop checking after renaming the first matching file



#Converts .dat files to .csv files automatically (Helper Function)
#Helper function: Converts .dat files to .csv files 
def convert_dat_to_csv(directory):
    """
    Converts all .dat files in the specified directory to .csv files.
    
    Parameters:
        directory (str): Path to the directory containing .dat files.
    """
    if not os.path.exists(directory):
        st.write(f"Directory '{directory}' does not exist.")
        return

    # Iterate through files in the directory
    for file_name in os.listdir(directory):
        if file_name.endswith(".dat"):
            dat_file_path = os.path.join(directory, file_name)
            csv_file_path = os.path.join(directory, file_name.replace(".dat", ".csv"))

            try:
                # Read the .dat file with automatic delimiter detection
                df = pd.read_csv(dat_file_path, delimiter=None, engine='python')

                # Save as .csv
                df.to_csv(csv_file_path, index=False)
                st.write(f"Converted: {file_name} -> {csv_file_path}")

            except Exception as e:
                st.write(f"Error converting {file_name}: {e}")



#### Scrape Data from TAPR Advanced Download (Master Function)
def tea_scraper(directory_path, years, variables, level, dist_type=True):
    """
    Scrape all HERC data for specified years, variables, and level of data.
    
    Parameters:
        directory (string): file path that you would like data to be downloaded to
        years (list): List of years to scrape data for (formatted YYYY)
        variables (list): List of variable codes to download (such as "GRAD")
        level (str): Administrative level to scrape. Options:
            'C' for Campus
            'D' for District
            'R' for Region
            'S' for State

    Returns: 
        Specified files stored in folders located in users current directory. 
    """
    ### Checking to see if directory is a valid path and continuing code if it is  ### 
    if not os.path.isdir(directory_path):
        print(f"Error: {directory_path} is not a valid directory.")
        return
    print(f"Processing directory: {directory_path}")
    directory_path_name = directory_path

    ### Evaluating if level is accurate or not and converting it to expanded level name ### 
    valid_levels = {
        'C': 'Campus',
        'D': 'District',
        'R': 'Region',
        'S': 'State'
    }
    
    if level not in valid_levels:
        raise ValueError(f"Invalid level. Must be one of: {', '.join(valid_levels.keys())}")
    
    file_prefix = {
        'C': 'CAMP',
        'D': 'DIST',
        'R': 'REGN',
        'S': 'STATE'
    }[level]

    ### Looping through years to get data ### 
    for year in years:
        #Construct URL for TAPR data download page 
        url = f"https://rptsvr1.tea.texas.gov/perfreport/tapr/{year}/download/DownloadData.html"

        #Create a directory for current years data 
        dir_name = f"raw_data{year}"
        full_dir_path = os.path.join(directory_path_name, dir_name)
        os.makedirs(full_dir_path, exist_ok=True)
        
        #Set up chrome webdriver options 
        chrome_options = webdriver.ChromeOptions()
        absolute_download_path = os.path.abspath(full_dir_path)
        
        #Configure download prefrences to automate file saving 
        prefs = {
            "download.default_directory": absolute_download_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_settings.popups": 0
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        try:
            #Initialive web scraper and navigate to the data download page 
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)

            # Check if the page loaded properly
            if "Page Not Found" in driver.page_source or "404" in driver.page_source:
                st.write(f"Year {year} does not exist. Skipping...")
                driver.quit()
                continue  # Skip this year and move to the next one
            #Select the desired level 
            level_select = driver.find_element(By.XPATH, f"//input[@type='radio' and @name='sumlev' and @value='{level}']")
            level_select.click()
        
        except WebDriverException as e:
            print(f"Failed to access {year}. Error: {e}")
            driver.quit()
            continue  # Skip this year

        unavailable = [] #Track unavilable files 
        st.write(f"Downloading {valid_levels[level]} Level TAPR Data for {year}...")
        
        # Loop through variables to download corresponding files 
        for var in variables:
            st.write(f"Checking for {file_prefix}{var} data...")
            #Define possible file patters to check if file is there 
            file_patterns = [
                f"{file_prefix}{var}_{year}.csv",
                f"{file_prefix}{var}_{year}.dat",
                f"{level}{var}_{year}.dat",
                f"{level}{var}_{year}.csv"
            ]
            
            # Skip downloading if the file already exists 
            if any(os.path.isfile(os.path.join(full_dir_path, file)) for file in file_patterns):
                st.write(f"{var}_{year} already exists")
                unavailable.append(var)
                continue
                
            try:
                #Select the dataset corresponding to the current variables 
                select_data = driver.find_element(By.XPATH, f"//input[@type='radio' and @name='setpick' and @value='{var}']")
                select_data.click()
                
                time.sleep(1)
                #Click continue button to initiate download 
                download = driver.find_element(By.XPATH, "//input[@type='submit' and @value='Continue']")
                download.click()
                st.write(f"Downloaded {level if var == 'REF' else file_prefix}{var} for {year}")
                
            except NoSuchElementException:
                st.write(f"{var} not found for {year}")
                unavailable.append(var)
                continue
        
        #Get the list of successfully downloaded variables 
        available_vars = set(variables) - set(unavailable)

        #Wait for all downlods to complet and rename files accordingly 
        if wait_for_downloads(variables=available_vars, year=year, directory=full_dir_path):
            for a_var in available_vars:
                file_renamer(directory=full_dir_path, year=year, prefix=file_prefix, var=a_var, level=level)  
        driver.quit()

        #If downloading district level data, get the district type dataset 
        if level == "D" and dist_type:
            st.write(f"Downloading District Type Data for {year}...")
            
            if os.path.isfile(os.path.join(full_dir_path, f"district_type{year}.csv")):
                st.write(f"District Type Data for {year} already exists")
                st.write("")
                continue

            df = district_type_scraper(year)

            if df is None:
                st.write(f"Failed to retrieve District Type Data for {year}. Skipping...")
                continue  # Skip this year

            df.to_csv(os.path.join(full_dir_path, f"district_type{year}.csv"), index=False)
            st.write(f"Downloaded District Type Data for {year}")
            st.write("")

        #Calling helper function to convert .dat files to .csv files 
        convert_dat_to_csv(full_dir_path)

    st.write("All Data Downloaded!")





### Creating a GUI Streamlit App 
# Mapping full names to their respective codes
LEVEL_MAPPING = {
    "Campus": "C",
    "District": "D",
    "Region": "R",
    "State": "S"
}

st.title("Texas Academic Performance Report (TAPR) Scraper")

st.markdown("""
### Welcome to the TAPR Scraper App! 🍵  
This app allows you to easily download multiple files from the TAPR advanced download website.  
https://rptsvr1.tea.texas.gov/perfreport/tapr/2023/download/DownloadData.html  
Enter the required parameters below and click **Run Scraper** to get the data.
""")

# User Input Fields (Place to write inputs)
directory_input = st.text_input("Enter directory you would like data to be downloaded to\n\nExample (Windows): C:\\Users\\YourName\\Downloads\n\nExample (Mac): /Users/YourName/Downloads")
#st.markdown(
#    "Enter years (comma-separated, 2018 and onwards)\n\n"
#    "**Note:** The year listed corresponds to the academic school year.\n\n"
#    "_For example, 2018 refers to the 2017-18 academic year._"
#)
years_input = st.text_input("Enter years (comma-seperated, 2018 and onwards)\n\nNOTE: The year listed corresponds to the academic school year.\n\nFor example, 2018 refers to the 2017-18 academic year")
variables_input = st.text_input("Enter variables (comma-separated, e.g., GRAD, STAAR1, PROF)")

# Show full names in the dropdown, but store the corresponding code
selected_level = st.selectbox("Select Level of Data", list(LEVEL_MAPPING.keys()))

# Convert string inputs into strs, lists (To fit in puthon function)
inputted_directory = os.path.normpath(directory_input)
inputted_years = [int(year.strip()) for year in years_input.split(",") if year.strip().isdigit()]
inputted_variables = [var.strip() for var in variables_input.split(",") if var.strip()]

# Convert selected level to its respective code
inputted_level = LEVEL_MAPPING[selected_level]

# Run scraper when button is clicked
if st.button("Run Scraper"):
    if not inputted_directory or not inputted_years or not inputted_variables:
        st.error("Please enter valid directory, years and variables.")
    else:
        result = tea_scraper(inputted_directory, inputted_years, inputted_variables, inputted_level)
        st.success(f"Scraping complete: {result}")




