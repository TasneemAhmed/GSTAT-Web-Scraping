# GSTAT ETL Documentation

## Overview

This Python script facilitates an EL (Extract, Transform, Load) process. It's designed to Read data from an Excel file which downloaded from website, transform it, and then load it into a destination database. The script handles various tasks such as establishing database connections, data extraction, data transformation, logging and load data.

## Usage

To use this script:

1. Install necessary Python libraries: `pandas`, `datetime`, `sqlalchemy`,`time` etc.
2. Configure database connections and settings in `ETL_Config`.
3. Run the script as a standalone Python application or in SSIS.

# Import Section

## Overview

This section of the script is dedicated to importing necessary libraries and custom modules required for the ETL process. Each import serves a specific purpose in the script, whether it's for data manipulation, database connection, or other utilities.

## Details of Imports

### Standard Libraries and Packages

- `pandas`: A powerful data manipulation and analysis tool, used extensively for data processing tasks in the script.

    ```python
    import pandas as pd
    ```

- `datetime`: Utilized for handling date and time information, crucial in time-sensitive data operations.

    ```python
    from datetime import datetime
    ```

- `time`: Provides time-related functions, useful for handling delays or time calculations.

    ```python
    import time
    ```

- `re`: Library in Python provides support for working with regular expressions, which are powerful tools for matching patterns in strings. Regular expressions are often used for tasks like searching, replacing, and splitting strings based on specific patterns.

    ```python
    import re
    ```

- `logging`: Used for logging information and errors, aiding in debugging and tracking the script's execution.

    ```python
    import logging
    ```

- `sqlalchemy`: A SQL toolkit and Object-Relational Mapping (ORM) library for Python, used for database interactions.

    ```python
    import sqlalchemy
    ```

- `SQLAlchemyError`: It is an exception class in the SQLAlchemy library that serves as a base class for all exceptions raised by SQLAlchemy during database operations. It is part of the SQLAlchemy's error handling mechanism and is designed to catch and handle errors related to database interactions.

    ```python
    from sqlalchemy.exc import SQLAlchemyError
    ```

- `OS`: library in Python provides a way to interact with the operating system in a platform-independent manner. like get current working directory

    ```python
    import os
    ```

- `shutil`: library in Python provides a collection of high-level operations on files and directories, making it easier to perform tasks like copying, moving, and removing files and directories. It builds on the lower-level operations provided by the os library, offering a more user-friendly interface for common file system tasks.

    ```python
    import shutil
    ```

- `glob`: ibrary in Python is used for finding files and directories that match a specified pattern

    ```python
    import glob
    ```

- `NVARCHAR`: in Python is used in conjunction with SQLAlchemy, a popular SQL toolkit and Object-Relational Mapping (ORM) library for Python, to work with Microsoft SQL Server databases.
    In SQL Server, NVARCHAR is often used when you need to store text data that may include characters from different languages, as it supports the Unicode standard.

    ```python
    from sqlalchemy.dialects.mssql import NVARCHAR
    ```

### Custom Modules

- `ETL_Config as c`: A custom module likely containing configuration settings for the ETL process.

    ```python
    import ETL_Config as c
    ```

- `ETL_com_functions as e`: Another custom module, presumably containing common functions used in the ETL operations.

    ```python
    import ETL_com_functions as e
    ```

# Global Variables

## Global Variables Initialization

### Overview

The script initializes several global variables used for database connections and configurations. These variables are set to `None` initially and are configured during the script's execution.

### Variables

- `Engine_DMDQ`: Intended for the database engine of the DM_Quality database.
- `Engine`: Database engine for the primary destination database.
- `SchemaName`: Name of the database schema in use.
- `database_name`: Name of the destination database.
- `num_src`: A variable potentially used to track the number of sources or source-related parameters.
- `start_time`: A variable potentially used to get the start time of ETL process


### Code Snippet

```python
Engine_DMDQ, Engine, SchemaName, database_name, num_src = None, None, None, None, None
```

# get_database_config Function

## Purpose

The `get_database_config` function is designed to retrieve specific database configuration settings from the ETL configuration module. This function is a key component in the script, allowing dynamic access to various database configurations based on a given key.

## Parameters

- `config_key` (str): A string key that identifies which database configuration to retrieve. This key corresponds to a specific set of configuration details in the ETL configuration module.

## Functionality

- The function accesses the `config` dictionary within the `ETL_Config` (aliased as `c`) module.
- It then retrieves the configuration details for the database associated with the provided `config_key`.
- The configuration details are expected to be stored under the `"servers"` key in the `config` dictionary of the `ETL_Config` module.

## Code Snippet

```python
def get_database_config(config_key):
    """Retrieve database configuration from ETL configuration module."""
    try:
        return c.config["servers"][config_key]
    except KeyError as error:
        logging.error(f"Configuration key {config_key} not found: {error}")
        raise
```

# `establish_connections(dest_config_key, dmdq_config_key)`

## Purpose
The `establish_connections` function establishes connections to two databases based on the provided configuration keys. It retrieves and sets the schema and database names as global variables.

## Parameters
- `dest_config_key` (str): The configuration key for the destination database.
- `dmdq_config_key` (str): The configuration key for the DM_Quality database.

## Returns
- `Engine_DMDQ`: The database engine connection object for the DM_Quality database.
- `Engine`: The database engine connection object for the destination database.
- `SchemaName` (str): The schema name retrieved from the destination database configuration.
- `database_name` (str): The database name retrieved from the destination database configuration.

## Raises
- `Exception`: If there is an error while establishing the database connections, the function logs the error and raises the exception.


## Code Snippet
```python
def establish_connections(dest_config_key, dmdq_config_key):
    """
    Establishes database connections based on provided configuration keys.
    """
    global Engine_DMDQ, Engine, SchemaName, database_name
    try:
        # Establish connections to the destination and DM_Quality databases
        Engine_DMDQ, Engine = e.connect_to_databases(dest_config_key, dmdq_config_key)
        # Retrieve schema and database name from configuration
        SchemaName = get_database_config(dest_config_key)["schema"]
        database_name = get_database_config(dest_config_key)["database"]
        return Engine_DMDQ, Engine, SchemaName, database_name
    except Exception as error:
        logging.error(f"Error establishing database connections: {error}")
        raise

```

# `move_file_to_archive(file_path)`

## purpose
The `move_file_to_archive` function moves files with names matching a specified pattern (e.g., `ITR Q12023A.xlsx`) from the current working directory to an `Archive` subdirectory. If no matching files are found, the function logs an informational message.

## Parameters
- `file_path` (str): The pattern for the file name(s) to search for within the current working directory.

## Returns
- `None`

## Raises
- `FileNotFoundError`: If the specified file does not exist.
- `PermissionError`: If there is an issue with file permissions.
- `Exception`: For any other exceptions that might occur during the file moving process.

## Logging
- Logs an informational message if no files matching the pattern are found.
- Logs an informational message when a file is successfully moved to the `Archive` directory.
- Logs an error message if a `FileNotFoundError`, `PermissionError`, or any other exception occurs.


## Code Snippet
```python
def move_file_to_archive(file_path):
    """
    Move files with names matching 'Monthly_Bulletin_*xlsx' to the 'Archive' directory.

    Parameters:
    file_path (str): The pattern file name we need to look for inside current working dir.

    Returns:
    None

    Raises:
    FileNotFoundError: If the specified file does not exist.
    PermissionError: If there is an issue with file permissions.
    Exception: For any other exceptions that might occur.
    """
    try:
        save_directory = os.getcwd()
        archive_directory = os.path.join(save_directory, 'Archive') # Join the current working directory with the subdirectory 'Archive'
        # Find all files matching the pattern
        pattern = os.path.join(save_directory, file_path)
        files = glob.glob(pattern)
        
        if not files:
            logging.info("No files matching the pattern were found.")
            return
        
        for file_path in files:
            shutil.move(file_path, archive_directory)
            logging.info(f"File moved to: {archive_directory}")

    except FileNotFoundError as e:
        logging.error(f"File not found: {file_path}. Exception: {e}")
    except PermissionError as e:
        logging.error(f"Permission error while moving file: {file_path}. Exception: {e}")
    except Exception as e:
        logging.error(f"An error occurred while moving the file: {file_path}. Exception: {e}")

```

# `read_departments_sheets(pattern)`

## purpose
The `read_departments_sheets` function reads specific sheets (`1.1` and `2.1`) from Excel files that match a given glob pattern. The function returns the data from these sheets as a list of tuples, where each tuple contains a sheet name and its corresponding data as a DataFrame.

## Parameters
- `pattern` (str): The glob pattern used to match Excel files (e.g., `*.xlsx`).

### Returns
- `list of tuples`: A list of tuples, where each tuple contains:
  - `sheet_name` (str): The name of the sheet.
  - `sheet_data` (DataFrame): The data from the sheet.

## Raises
- `FileNotFoundError`: If any of the specified files are not found.
- `Exception`: For any other errors that occur while reading the Excel files or processing the pattern.

## Logging
- Logs the start of reading department data.
- Logs a message when an Excel file has been successfully read.
- Logs an error if a file is not found or if any other error occurs during the reading process.
- Logs an error if an error occurs while processing the pattern.


## Code Snippet
```python

def read_departments_sheets(pattern):
    """
    Read sheets: 1.1, 2.1 in an Excel file and return them as a list of tuples.

    Parameters:
        pattern (str): Glob pattern to match the Excel files.

    Returns:
        list of tuples: sheets_data, a tuple where the first elements are sheet names and the second values are corresponding DataFrames.
    """
    global start_time
    sheets_data = []

    try:
        start_time = time.time()
        # 1. Find all files matching the pattern which is '*.xlsx'
        files = glob.glob(pattern)
        if not files:
            print("No files matching the pattern were found.")
            return sheets_data
        
        logging.info(f"Started reading Departments data")
        # 2. Read all sheets into a dictionary of DataFrames, key is sheet_name and value the sheet data
        for file in files:
            try:
                excel_data = pd.read_excel(file, sheet_name=['1.1', '2.1'])
                
                # 3. Convert dictionary to a list of tuples, first element of each tuple is sheet name and second sheet data
                for sheet_name in excel_data:
                    sheets_data.append((sheet_name, excel_data[sheet_name]))
                    
                logging.info(f"Finished reading {file}")
                    
            except FileNotFoundError:
                logging.error(f"File {file} not found.")
                continue
            except Exception as e:
                logging.error(f"An error occurred while reading Excel file {file}: {str(e)}")
                continue
                
        return sheets_data
    
    except Exception as e:
        logging.error(f"An error occurred while processing the pattern: {str(e)}")
```

# `read_countries_sheets(pattern)`

## purpose
The `read_countries_sheets` function reads specific sheets (`1.4` and `2.4`) from Excel files that match a given glob pattern. The function returns the data from these sheets as a list of tuples, where each tuple contains a sheet name and its corresponding data as a DataFrame.

## Parameters
- `pattern` (str): The glob pattern used to match Excel files (e.g., `*.xlsx`).

## Returns
- `list of tuples`: A list of tuples, where each tuple contains:
  - `sheet_name` (str): The name of the sheet.
  - `sheet_data` (DataFrame): The data from the sheet.

## Raises
- `FileNotFoundError`: If any of the specified files are not found.
- `Exception`: For any other errors that occur while reading the Excel files or processing the pattern.

## Logging
- Logs the start of reading countries data.
- Logs a message when an Excel file has been successfully read.
- Logs an error if a file is not found or if any other error occurs during the reading process.
- Logs an error if an error occurs while processing the pattern.


## Code Snippet

```python
def read_countries_sheets(pattern):
    """
    Read sheets: 1.4, 2.4 in an Excel file and return them as a list of tuples.

    Parameters:
        pattern (str): Glob pattern to match the Excel files.

    Returns:
        list of tuples: sheets_data, A tuple where first elements are sheet names and second values are corresponding DataFrames.
    """
    sheets_data = []

    try:
        # 1. Find all files matching the pattern
        files = glob.glob(pattern)
        if not files:
            print("No files matching the pattern were found.")
            return sheets_data

        logging.info("Started reading Countries data")
        
        # 2. Read all sheets into a dictionary of DataFrames
        for file in files:
            try:
                excel_data = pd.read_excel(file, sheet_name=['1.4', '2.4'])

                # 3. Convert dictionary to a list of tuples
                for sheet_name in excel_data:
                    sheets_data.append((sheet_name, excel_data[sheet_name]))
                
                logging.info(f"Finished reading {file}")

            except FileNotFoundError:
                logging.error(f"File {file} not found.")
                continue
            except Exception as e:
                logging.error(f"An error occurred while reading Excel file {file}: {str(e)}")
                continue

        return sheets_data
    except Exception as e:
        logging.error(f"An error occurred while processing the pattern: {str(e)}")
```
# `remove_digits(input_string)`

## purpose
The `remove_digits` function removes all digits from a given string. It uses a regular expression to identify and remove any numeric characters from the input string.

## Parameters
- `input_string` (str): The string from which digits will be removed.

## Returns
- `str`: The input string with all digits removed.

## Example
For example, calling `remove_digits('1.الربع الأول')` will return `'الربع الأول'`.

### Note
- The regular expression `r'\d'` is used to match any digit in the input string. The dot (`.`) before `\d` in the provided function is incorrect and should be removed to match digits correctly.

## Code Snippet

```python
def remove_digits(input_string):
    return re.sub(r'.\d', '', input_string)

```
# `extract_year(column_name)`

## purpose
The `extract_year` function extracts a 4-digit year from a given string. If a year is found in the string, it returns the year. If no year is found or if the input is not a string, it returns the original input.

## Parameters
- `column_name` (str or any type): The input from which a 4-digit year will be extracted. If `column_name` is not a string, it is returned unchanged.

## Returns
- `str` or `column_name` (same type as input): If a 4-digit year is found in the string, it is returned as a string. Otherwise, the original input is returned.

## Example
- `extract_year('2023*')` returns `'2023'`.

## Notes
- The regular expression `r'\d{4}'` is used to find a sequence of exactly four digits, which typically represents a year.
- If `column_name` is not a string, the function will return it unchanged.

## Code Snippet

```python
def extract_year(column_name):
    if isinstance(column_name, str):
        match = re.search(r'\d{4}', column_name)
        return match.group(0) if match else column_name
    else:
        return column_name

```
# `transform_by_departments_data(sheets_data)`

## Purpose
The `transform_by_departments_data` function processes and transforms data from multiple sheets of Excel files. It performs various operations such as renaming columns, extracting specific values, and organizing the data into a dictionary. This function handles errors by logging them and continues processing other sheets.

## Parameters
- `sheets_data` (dict): A dictionary where keys are sheet names and values are DataFrames containing the sheet data.

## Returns
- `dict`: A dictionary where keys are sheet names and values are lists of transformed DataFrames.

## Transformation Steps
1. **Column Renaming**: Renames specific columns using a predefined dictionary.
2. **Row Identification**: Identifies rows based on the values `'وصف القسم'` (Section Description) and `'الإجمالي'` (Total).
3. **Row Selection**: Selects data between the identified rows.
4. **Drop Empty Columns**: Drops columns with all empty values.
5. **Extract Year and Quarter**:
   - Extracts the current year and quarter from the last column.
   - Extracts values and year/quarter information from the last three columns.
6. **Add New Columns**: Adds new columns for year, quarter, and previous year/quarter values.
7. **Drop Unnamed Columns**: Drops columns with 'Unnamed' in their names.
8. **Filter Rows**: Filters the DataFrame to include only rows from the fourth row onwards.
9. **Organize Data**: Adds the transformed DataFrame to a dictionary, ensuring that if a sheet name is already present, the DataFrame is appended to the existing list.

## Logging
- Logs the start of the transformation process.
- Logs a message when a sheet is successfully transformed.
- Logs an error if an issue occurs during the transformation of a sheet.
- Logs an error if an issue occurs while processing the data.


## Notes
- **`mapping_quarters`**: A dictionary used to map Arabic quarter names to English abbreviations (`Q1`, `Q2`, `Q3`, `Q4`).
- **Error Handling**: If an error occurs during the processing of a sheet, the function logs the error and continues with the next sheet.

## Code Snippet

```python
def transform_by_departments_data(sheets_data):
    """
    Transforms data from multiple sheets by extracting specific columns and values, renaming columns, and organizing the data into a dictionary.

    Args:
        sheets_data (dict): A dictionary where keys are sheet names and values are DataFrames containing the sheet data.

    Returns:
        dict: A dictionary where keys are sheet names and values are lists of transformed DataFrames.

    The function performs the following steps:
    1. Renames specific columns using a predefined dictionary.
    2. Identifies the rows where the column 'وصف القسم' (Section Description) and 'الإجمالي' (Total) are located.
    3. Selects data between these identified rows.
    4. Drops columns with all empty values.
    5. Extracts the current year and quarter from the last column.
    6. Extracts values and year/quarter information from the last three columns.
    7. Adds new columns to the DataFrame for year, quarter, and previous year/quarter values.
    8. Drops columns with 'Unnamed' in their names.
    9. Filters the DataFrame to include only rows from the fourth row onwards.
    10. Adds the transformed DataFrame to a dictionary.

    The dictionary returned has the following structure:
    {
        'sheet_name1': [df1, df2],
        'sheet_name2': [df1, df2],
        ...
    }

    If an error occurs during the transformation of a sheet, the function logs the error and continues with the next sheet.
    """
    departments_transformed_data = {}
    # Dictionary to rename specific columns
    rename_dict = {'الفهرس':'Section_number', 'Unnamed: 0': 'Section_number', 'Unnamed: 1':'Section_description'}

    try:
        logging.info("Transforming By Departments data...")
        for sheet_name, df in sheets_data:
            try:
                 # Get the index of the row where any column contains the value 'وصف القسم' & 'الإجمالي
                start_index = df[df.apply(lambda row: row.astype(str).str.contains('وصف القسم').any(), axis=1)].index[0]
                end_index = df[df.apply(lambda row: row.astype(str).str.contains('الإجمالي').any(), axis=1)].index[0]-1
       
                # Select rows start and end  from these indexes
                df = df.loc[start_index:end_index].reset_index(drop=True)
                #drop columns which has all empty values
                df.dropna(axis=1, how='all', inplace=True)
                #rename columns
                df.rename(columns= rename_dict, inplace=True)
        
                """From the last column"""
                 # Select the second row of the last column
                current_year = extract_year(df.iloc[1, -1])
                # select the first row of the last column
                current_Q_ar = remove_digits(df.iloc[0, -1])
        
                """From the third column from the last"""
                #select only the third columns from the last
                Current_Quarter_Of_Pevious_Year_Value = df.iloc[:,-3]
                #select the first row of the third column from the last
                Current_Quarter_Of_Pevious_Year_Quarter = df.iloc[0, -3]
                #select the second row of the third column from the last
                Current_Quarter_Of_Pevious_Year_Year = extract_year(df.iloc[1, -3])

                """From the second column from the last"""
                #get only the second column from the last
                Previous_Value = df.iloc[:,-2]
                #the first row from the second column
                Previous_Quarter = df.iloc[0, -2]
                #the second row from the second column
                Previous_Year = extract_year(df.iloc[1, -2])
        
                """Get the last column"""
                Current_Value = df.iloc[:,-1]

                df['Year'] = current_year
                year = df['Year'].iloc[0] #get the first value from 'Year' column
                df['Quarter'] = current_Q_ar
                df['Quarter'] = df['Quarter'].map(mapping_quarters)
                quarter = df['Quarter'].iloc[0] #get the first value from 'Quarter' column
        
                df['Current_Quarter_Of_Pevious_Year_Value'] = Current_Quarter_Of_Pevious_Year_Value
                df['Current_Quarter_Of_Pevious_Year_Quarter'] = Current_Quarter_Of_Pevious_Year_Quarter
                df['Current_Quarter_Of_Pevious_Year_Year'] = Current_Quarter_Of_Pevious_Year_Year

                df['Previous_Value'] = Previous_Value
                df['Previous_Quarter'] = Previous_Quarter
                df['Previous_Year'] = Previous_Year

                df['Current_Value'] = Current_Value
                df['Current_Quarter'] = current_Q_ar
                df['Current_Year'] = current_year
        
                #drop columns which has 'Unnamed' in its name
                unneeded_columns = [col for col in df.columns if 'Unnamed' in col]
                df.drop(unneeded_columns, axis=1, inplace=True)
                df = df.iloc[3:] #filter dataframe to have from the fourth row to the end

                """
                    Add the transformed DataFrame to the dictionary
                        if sheet name already in dictionary add dataframe to its list value to prevent override the value of the same key
                        {key1: [df1, df2],
                         key2: [df1, df2],}
                """
                if sheet_name in departments_transformed_data:
                    departments_transformed_data[sheet_name].append(df)
                else:
                    departments_transformed_data[sheet_name] = [df]
           
            except Exception as e:
                 logging.error(f"An Error occurred while transforming by Departments data in {sheet_name}: {e}")
                 #continue #if sheet has a problem continue with another sheet

        logging.info("Finished Transforming By Departments data")
    except Exception as e:
          logging.error(f"An Error occurred while transforming by Departments data {e}")
    return departments_transformed_data
```
# `extract_quarter_year(text)`

## Purpose
The `extract_quarter_year` function extracts the quarter and year from a given Arabic text string. It uses regular expressions to identify the Arabic quarter and year, maps the Arabic quarter to its corresponding English abbreviation using a predefined dictionary, and returns these values.

## Parameters
- `text` (str): The input text from which to extract the quarter and year.

## Returns
- `tuple`: A tuple containing two elements:
  - `quarter` (str or None): The English abbreviation for the quarter (e.g., "Q1", "Q2"), or `None` if no quarter is found.
  - `year` (str or None): The year extracted from the text (e.g., "2024"), or `None` if no year is found.

## Regular Expression Patterns
- **`quarter_pattern`**: This pattern matches "الربع" followed by a space and captures the subsequent word representing the quarter (e.g., "الربع الأول" for "Q1").
- **`year_pattern`**: This pattern matches a sequence of four digits representing the year (e.g., "2024").


## Example Usage
```python
result = extract_quarter_year("الربع الأول 2023")
# result -> ("Q1", "2023")

result = extract_quarter_year("الربع الثالث 2024")
# result -> ("Q3", "2024")
```

## Code Snippet
```python

def extract_quarter_year(text):
    # Regular expression patterns for quarter and year
    quarter_pattern = r'(الربع\s\w+)' #This pattern matches "الربع" followed by a space and captures the following word which represents the quarter.
    year_pattern = r'(\d{4})' #This pattern matches a sequence of four digits which represent the year.
    # Search for the quarter and year in the text
    quarter_match = re.search(quarter_pattern, text) #Searches the text for the quarter pattern.
    year_match = re.search(year_pattern, text) # Searches the text for the year pattern.
    
    # Extract the matched quarter and year
    quarter_arabic = quarter_match.group(1) if quarter_match else None
    # Map the Arabic quarter to the corresponding value in mapping_quarters
    quarter = mapping_quarters.get(quarter_arabic) if quarter_arabic else Non
    year = year_match.group(1) if year_match else None

```

# `transform_by_countries_data(sheets_data2)`

## Purpose
The `transform_by_countries_data` function processes data from multiple Excel sheets, extracting, renaming, and organizing specific columns into a structured format. The transformed data is stored in a dictionary where each sheet's name is a key, and its value is a list of DataFrames representing the transformed data.

## Parameters
- `sheets_data2` (dict): A dictionary where keys are sheet names, and values are DataFrames containing the data from the corresponding sheets.

## Returns
- `dict`: A dictionary where keys are sheet names, and values are lists of transformed DataFrames. The structure is as follows:
  ```python
  {
      'sheet_name1': [df1, df2],
      'sheet_name2': [df1, df2],
      ...
  }

## Code Snippet
```python
def transform_by_countries_data(sheets_data2):
    """
    Transforms data from multiple sheets by extracting specific columns and values, renaming columns, and organizing the data into a dictionary.

    Args:
        sheets_data2 (dict): A dictionary where keys are sheet names and values are DataFrames containing the sheet data.

    Returns:
        dict: A dictionary where keys are sheet names and values are lists of transformed DataFrames.

    The function performs the following steps:
    1. Extracts the quarter and year from a row containing the value 'الربع'.
    2. Identifies the rows where the column 'الدولة' and 'دول أخرى' are located.
    3. Sets new column names from the identified row containing 'الدولة'.
    4. Filters the DataFrame to include rows between the identified start and end rows.
    5. Renames columns using a predefined dictionary.
    6. Inserts new columns 'Year' and 'Quarter' into the DataFrame.
    7. Adds the transformed DataFrame to a dictionary.

    The dictionary returned has the following structure:
    {
        'sheet_name1': [df1, df2],
        'sheet_name2': [df1, df2],
        ...
    }

    If an error occurs during the transformation of a sheet, the function logs the error.
    """
    countries_transformed_data = {}

    try:
        logging.info("Transforming By Countries data...")
        for sheet_name, df in sheets_data2:
            try:
                """Get the row which has 'الربع' in its value and pass row to extract_quarter_year()"""
                y_Q_row = df[df.apply(lambda row: row.astype(str).str.contains('الربع').any(), axis=1)]
                y_Q_row = y_Q_row.iloc[0,0]
                quarter, year = extract_quarter_year(y_Q_row)

                # Get the index of the row where any column contains the value: 'الدولة'
                start_index = df[df.apply(lambda row: row.astype(str).str.contains('الدولة').any(), axis=1)].index[0]
               
                # Set new column names from the specified row
                df.columns = df.iloc[start_index].tolist()
                #filter dataframe with needed rows
                df= df.iloc[start_index+1:]
                #rename columns
                df.rename(columns=sections_columns_renamed, inplace=True)
                # Drop rows where the specified column 'الإجمالي' has empty values
                df.dropna(subset=['الإجمالي'], inplace=True)
                """
                - Insert the new column 'Year' at the third position (index 2)
                - Insert the new column 'Quarter' at the fourth position (index 3)
                """
          
                try:
                    # allow_duplicates=False parameter prevents inserting a column with the same name as an existing column.
                    df.insert(2, 'Year', year, allow_duplicates=False)
                    df.insert(3, 'Quarter', quarter, allow_duplicates=False)

                except Exception as i:
                    logging.warning(f"An error occured while insert columns: {i}")  

                """
                    Add the transformed DataFrame to the dictionary
                        if sheet name already in dictionary add dataframe to its list value to prevent override the value of the same key
                        {key1: [df1, df2],
                         key2: [df1, df2],}
                """
                if sheet_name in countries_transformed_data:
                    countries_transformed_data[sheet_name].append(df)
                else:
                    countries_transformed_data[sheet_name] = [df]
            except Exception as e:
                logging.error(f"An error occured while transforming by Countries data in {sheet_name}: {e}")
        
        logging.info("Finished transformations By countries data") 

    except Exception as e:
       logging.error(f"An error occured while transforming by Countries data in {sheet_name}: {e}")

    return countries_transformed_data
```

# `load_transformed_dataframes(transformed_dataframes, dest_engine, schema_name)`

## Purpose
The `load_transformed_dataframes` function loads transformed DataFrames into specified tables within a database. It ensures that only new records are inserted by checking for existing unique key combinations in the destination tables. The function logs the process and calculates the total execution time.

## Parameters
- `transformed_dataframes` (dict): 
  - A dictionary where keys are sheet names, and values are lists of transformed DataFrames corresponding to each sheet.
- `dest_engine` (sqlalchemy.engine.base.Engine): 
  - A SQLAlchemy engine object representing the destination database connection.
- `schema_name` (str): 
  - The name of the schema in the destination database where the tables reside.

## Returns
- `float`: 
  - The total execution time in seconds from the start of reading data until the DataFrames are loaded into the database tables.

## Function Workflow
1. **Map Sheet Names to Table Names**:
   - The function uses a predefined mapping (`table_mappings`) to associate sheet names with their corresponding destination table names.

2. **Iterate Over DataFrames**:
   - For each sheet name and its associated DataFrames:
     - A temporary table is created to store the new data.
     - A 'STG_CreatedDate' column is added to the DataFrame with the current datetime.
     - The DataFrame is loaded into the temporary table.

3. **Insert New Records**:
   - The function inserts new records into the destination table by checking if the unique key combination (e.g., `Section_number`, `Year`, `Quarter`) does not already exist in the table.

4. **Drop Temporary Table**:
   - After the insertion, the temporary table is dropped to clean up.

5. **Error Handling**:
   - The function logs any errors encountered during the loading process and continues with the remaining DataFrames.

6. **Calculate and Log Execution Time**:
   - The total time taken for the data loading process is calculated and logged.

## Logging
- The function logs:
  - The start and end of the data loading process.
  - Any errors encountered.
  - Successful data loading for each table.
  - The total execution time for loading the DataFrames.

## Code Snippet
```python
def load_transformed_dataframes(transformed_dataframes, dest_engine, schema_name):
    """
        Load the transformed DataFrames into database tables.

        This function takes a dictionary of transformed DataFrames, a SQLAlchemy engine object for the destination database, 
        and the schema name of the destination tables. It loads each DataFrame into a corresponding table in the database. 
        If a table already contains a record with the same unique key combination, the record is not inserted.

        Parameters:
            transformed_dataframes (dict): A dictionary where keys are sheet names and values are corresponding transformed DataFrames.
            dest_engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine object for the destination database.
            schema_name (str): Name of the schema where the destination tables are located.

        Returns:
            float: Total execution time in seconds from the start of reading data until loading to the database tables.

        The function performs the following steps:
        1. Maps sheet names to their corresponding destination table names.
        2. For each DataFrame in the transformed_dataframes dictionary:
            a. Creates a temporary table to hold the new data.
            b. Adds a 'STG_CreatedDate' column with the current datetime.
            c. Loads the DataFrame into the temporary table.
            d. Inserts new records into the destination table where the unique key combination does not exist.
            e. Drops the temporary table after the insertion.
    """
    global table_mappings #to used in log()
    execution_times = []
    # Specify the list of table names for each DataFrame
    table_mappings = {
        '1.1': 'Exports_by_departments',
        '2.1': 'Imports_by_departments',
        '1.4': 'Non_oil_exports_by_country_and_major_divisions',
        '2.4': 'Imports_by_major_countries_and_divisions'
    }
    try:
        logging.info("loading Transformed dataframes to database...")
        for sheet_name, dfs in transformed_dataframes.items():
            table_name = table_mappings[sheet_name]
            # Create a temporary table to hold the new data
            temp_table_name = f"temp_{table_name}"

            try:
                for df in dfs:
                    df.fillna(0.0, inplace=True)
                    #df =df.convert_dtypes() #to load data correctly in temp table
                    # Add 'STG_CreatedDate' column with the current datetime
                    df['STG_CreatedDate'] = datetime.now()
     
                     # Use NVARCHAR(None) for NVARCHAR(MAX)
                    if 'departments' in table_name:   
                        datatypes={'Section_number':NVARCHAR(None), 'Section_description':NVARCHAR(None),
                                   'Current_Quarter_Of_Pevious_Year_Quarter':NVARCHAR(None), 
                                   'Previous_Quarter':NVARCHAR(None), 'Current_Quarter':NVARCHAR(None)}
                        df.to_sql(temp_table_name, con=dest_engine, schema=schema_name, if_exists='replace', index=False, dtype=datatypes)
                        # Insert new records where the combination of 'Section_number', 'Year' and 'Quarter' does not exist
                        insert_query = f"""
                            INSERT INTO {schema_name}.{table_name} ({', '.join(df.columns)})
                            SELECT {', '.join(df.columns)}
                            FROM {schema_name}.{temp_table_name} AS temp
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM {schema_name}.{table_name} AS main
                                WHERE main.Section_number = temp.Section_number
                                AND main.Year = temp.Year
                                AND main.Quarter = temp.Quarter
                            )
                            """
                    else:
                        df.to_sql(temp_table_name, con=dest_engine, schema=schema_name, if_exists='replace', index=False, dtype={'الدولة':NVARCHAR(None)})
                        # Insert new records where 
                        insert_query = f"""
                        INSERT INTO {schema_name}.{table_name} ({', '.join([f'[{col}]' if ' ' in col or not col.isalnum() else col for col in df.columns])})
                        SELECT {', '.join([f'[{col}]' if ' ' in col or not col.isalnum() else col for col in df.columns])}
                        FROM {schema_name}.{temp_table_name} AS temp
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM {schema_name}.{table_name} AS main
                            WHERE main.الدولة = temp.الدولة
                            AND main.Year = temp.Year
                            AND main.Quarter = temp.Quarter
                        )
                    """

                    with dest_engine.connect() as connection:
                        connection.execute(insert_query)
            
                    # Drop the temporary table
                    with dest_engine.connect() as conn:
                        conn.execute(f"DROP TABLE IF EXISTS {schema_name}.{temp_table_name}")
                     # Calculate load time
                    load_time = time.time() - start_time
                    execution_times.append(load_time)
                    logging.info(f"Successfully loaded into {table_name}")

            except Exception as ei:
                logging.error(f"Error while loading to {table_name}: {ei}")
        total_execution_time = sum(execution_times)
        logging.info(f"Successfully loaded Transformed data into {schema_name} database in {total_execution_time:.2f} seconds.")

    except Exception as error:
        logging.error(f"Error while loading dataframes to database destination: {error}")

    return format(total_execution_time, ".2f")
```
# `log_data_load(engine_dmdq, db_name, schema_name, table_names, src_table, execution_time, data_frames)`

## Description
The `log_data_load` function records details about the data loading process into a logging table in a specified database. This logging is crucial for monitoring and auditing the data load operations.

## Parameters
- **`engine_dmdq`** (`sqlalchemy.engine.base.Engine`): 
  - The SQLAlchemy engine instance connected to the DM_Quality database, where the log details will be recorded.
- **`db_name`** (`str`): 
  - The name of the destination database where the data was loaded.
- **`schema_name`** (`str`): 
  - The schema name within the destination database where the logging table resides.
- **`table_names`** (`list of str`): 
  - A list of table names for which the data loading process is being logged.
- **`src_table`** (`str`): 
  - The name of the source table or file from which data was loaded, used for logging and tracking.
- **`execution_time`** (`float`): 
  - The total time taken to load the data, typically measured in seconds.
- **`data_frames`** (`list of DataFrames`): 
  - A list of DataFrames that were loaded into the corresponding tables in the database.

## Raises
- **`Exception`**: 
  - If an error occurs during the logging process, the function logs the error and raises the exception.

## Function Workflow
1. **Iterate Over Table Names and DataFrames**:
   - For each table name and its corresponding DataFrames:
     - **Calculate Rows and Columns**:
       - The function computes the number of rows for each DataFrame and sums them to get the total rows inserted.
       - It also calculates the number of columns, assuming that all DataFrames for the same table have the same structure.
     - **Log the Data Load**:
       - The function calls the `Generate_Frequency_of_load` method to get the load frequency.
       - It then uses the `Insert_TO_DMDQ` method to insert the log details into the DM_Quality database.

2. **Logging Information**:
   - If the logging is successful, a message is logged indicating that the data load was logged successfully for each table.
   
3. **Error Handling**:
   - If an error occurs during the logging process, the function logs the error message and raises the exception.


## Code Snippet

```python
def log_data_load(engine_dmdq, db_name, schema_name, table_names, src_table, execution_time, data_frames):
    """
    Log data loading details to a database table for monitoring and auditing purposes.
    
    Parameters:
    - engine_dmdq: The SQLAlchemy engine instance for the DM_Quality database.
    - db_name: The name of the destination database.
    - schema_name: The name of the schema where the logging table resides.
    - table_names: A list of table names for which data loading is being logged.
    - src_table: The name of the source table (or file) for logging purposes.
    - execution_time: The total execution time for the data load process.
    - data_frames: The list of DataFrames that were loaded into the database.
    
    Raises:
    - Exception: If there is an error during the logging of data load details.
    """
    try:
        for table_name, dataframes in zip(table_names, data_frames):
            """
            Because we have list of dataframes for each table ('table1',[df1, df2,..]):
            1. get shape for every dataframe and add to tuples like that:
               rows = (146, 145, 150)  cols= (26, 26, 26)
            2. Sum rows to get total rows inserted in each table
            3. Get the first elemnt in cols tuple, as number of cols the same for all dataframes of the same table
            """
            rows, cols = zip(*(df.shape for df in dataframes))
            rows = sum(rows)
            cols = cols[0]
            count = e.Generate_Frequency_of_load(engine_dmdq, table_name)
            src_type = "EXCEL"
            rejected_rows = 0       
            e.Insert_TO_DMDQ(engine_dmdq, db_name, schema_name, table_name, execution_time, cols, rows, count, datetime.now(), src_table, src_type, rejected_rows)
            logging.info(f"Data load logged successfully for {table_name}.")
    except Exception as error:
        logging.error(f"Error logging data load: {error}")
        raise
```
# `check_for_xlsx_files()`

## Purpose
The `check_for_xlsx_files` function checks if there are any Excel files (i.e., files with a `.xlsx` extension) in the current working directory.

## Returns
- **`bool`**: 
  - Returns `True` if there is at least one file with a `.xlsx` extension in the current working directory.
  - Returns `False` if no such files are found.

### Function Workflow
1. **Get the Current Working Directory**:
   - The function uses `os.getcwd()` to get the path of the current working directory.
   
2. **List Files in the Directory**:
   - It then lists all files in this directory using `os.listdir(current_directory)`.

3. **Check for `.xlsx` Files**:
   - The function iterates over the list of files and checks if any file ends with the `.xlsx` extension.
   
4. **Return the Result**:
   - If an `.xlsx` file is found, the function immediately returns `True`.
   - If the loop completes without finding an `.xlsx` file, it returns `False`.



## Code Snippet

```python
def check_for_xlsx_files():
    """
    Check if there are any files ending with .xlsx in the current working directory.

    Returns:
    bool: True if there is at least one .xlsx file, False otherwise.
    """
    current_directory = os.getcwd()
    files = os.listdir(current_directory)
    
    for file in files:
        if file.endswith('.xlsx'):
            return True
    return False
```


# `main()`

## Purpose
The `main` function orchestrates the entire ETL (Extract, Transform, Load) process. It checks for `.xlsx` files in the current working directory, reads the necessary data, transforms it, loads it into the database, and logs the operation. If there are no `.xlsx` files, the process is skipped.

## Workflow
1. **Logging Start of the ETL Process**:
   - The process begins with a logging statement to indicate that the ETL process has started.

2. **Set Configuration Keys**:
   - `dest_config_key`: The key used to retrieve the configuration for the destination database.
   - `dmdq_config_key`: The key used to retrieve the configuration for the data quality database.

3. **Check for `.xlsx` Files**:
   - The function calls `check_for_xlsx_files()` to determine if there are any Excel files in the current working directory.
   - If no `.xlsx` files are found, the function logs this information and terminates the process.

4. **ETL Process Execution**:
   - If `.xlsx` files are found, the following steps are executed within a `try` block:
   
   a. **Establish Database Connections**:
      - The function calls `establish_connections()` to connect to the destination and data quality databases. This function is assumed to be defined elsewhere.
      - The connections returned include `Engine_DMDQ`, `Engine`, `SchemaName`, and `database_name`.

   b. **Read Excel Sheets**:
      - The function reads department and country data from the Excel files using `read_departments_sheets(file_path)` and `read_countries_sheets(file_path)`.
      - These functions return lists of tuples where each tuple contains a sheet name and a DataFrame.

   c. **Transform Data**:
      - The data from the sheets is transformed using `transform_by_departments_data(departments_sheets_data)` and `transform_by_countries_data(countries_sheets_data)`.
      - These functions return dictionaries where the keys are sheet names and the values are transformed DataFrames.
      - The dictionaries are combined into a single dictionary `transform_dfs`.

   d. **Load Data into the Database**:
      - The transformed data is loaded into the database using `load_transformed_dataframes(transform_dfs, Engine, SchemaName)`.
      - The function returns the `execution_time` taken to load the data.

   e. **Log the Data Load Operation**:
      - The function logs the details of the data load operation using `log_data_load()`. It includes information about the tables, execution time, and the number of rows inserted.

   f. **Move Processed Files to Archive**:
      - After successfully processing, the Excel files are moved to an archive directory using `move_file_to_archive(file_path)`.

5. **Error Handling**:
   - Any exceptions that occur during the ETL process are caught and logged with an error message.

## Notes
- **Dependencies**:
  - This function assumes that several other functions (`establish_connections`, `read_departments_sheets`, `read_countries_sheets`, `transform_by_departments_data`, `transform_by_countries_data`, `load_transformed_dataframes`, `log_data_load`, `move_file_to_archive`) are defined elsewhere in the codebase.
  
- **File Handling**:
  - The function handles files using a wildcard `*.xlsx`, meaning it processes all Excel files in the directory.
  
- **Logging**:
  - The function uses extensive logging to monitor the progress and capture errors, which is essential for debugging and auditing.

## Code Snippet

```python
def main():

    logging.info("Starting ETL process...")
    dest_config_key = 'STG_DEV'  
    dmdq_config_key = 'ByDB_General' 
    file_path = "*.xlsx"

    #if there is xlsx file in current working dir, start ETL process
    if check_for_xlsx_files(): 
        try:
            # Assuming establish_connections is correctly defined elsewhere
            Engine_DMDQ, Engine, SchemaName, database_name = establish_connections(dest_config_key, dmdq_config_key) 

            #read sheets in excel file and return list of tuples(sheet_name, dataframe)
        
            departments_sheets_data = read_departments_sheets(file_path)
            countries_sheets_data = read_countries_sheets(file_path)
  
            # return dictionary, key=sheet_name & value= transformed dataframe
            departments_transform_dfs= transform_by_departments_data(departments_sheets_data)
            countries_transform_dfs = transform_by_countries_data(countries_sheets_data)
            #This method creates a new dictionary 'transform_dfs'by unpacking the items from both dictionaries.
            transform_dfs = {**departments_transform_dfs, **countries_transform_dfs}
            #print(transform_dfs)
            # Load data to the database
            execution_time = load_transformed_dataframes(transform_dfs, Engine, SchemaName)
            # Log the data load operation
            log_data_load(Engine_DMDQ, database_name, SchemaName, list(table_mappings.values()), 'GSTAT', execution_time, list(transform_dfs.values()))        
            logging.info(f"ETL process completed successfully in {execution_time} seconds.")
        
            #move file to 'Archive' after finished processing
            move_file_to_archive(file_path)
        except Exception as error:
            logging.error(f"An error occurred in the ETL process: {error}")
    else:
        logging.info("There is no new files to be processed")

# Check if the script is being run directly and, if so, execute the main function
if __name__ == '__main__':
    main()
```
## ETL Notes 
- **Pre-Execution Data Exploration**: Begin with a thorough review of the source data to identify preprocessing requirements and ensure the extraction process aligns with the data's intended structure and format


## Contact Information

For further assistance or inquiries, please contact the development team.
