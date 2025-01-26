import pandas as pd
from datetime import datetime
import time
import re
import logging
import sqlalchemy
from sqlalchemy import text  #used when create temp table
from sqlalchemy.exc import SQLAlchemyError
import os #to get the current working directory
import shutil # to move file to another directory
import glob #module to find all files matching the pattern
#to identify columns with Arabic chars with NVARCHAR datatype
from sqlalchemy.dialects.mssql import NVARCHAR 

# Import custom modules
import ETL_Config as c
import ETL_com_functions as e

"""
We configure logging using basicConfig() to set the logging level to INFO. 
This means that only messages with severity level INFO and higher will be logged.
"""
logging.basicConfig(level=logging.INFO)

# Initialize global variables for database connections and configurations
Engine_DMDQ, Engine, SchemaName, database_name, num_src, start_time = None, None, None, None, None, None

def get_database_config(config_key):
    """Retrieve database configuration from ETL configuration module."""
    try:
        return c.config["servers"][config_key]
    except KeyError as error:
        logging.error(f"Configuration key {config_key} not found: {error}")
        raise

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


# Function to remove digits from a string, for ex: 1.الربع الأول -> الربع الأول
def remove_digits(input_string):
    return re.sub(r'.\d', '', input_string)

"""Function to extract only the digits from a string but check first if column_name is string, 
else return column_name as is
for ex: 2023* -> 2023
"""
def extract_year(column_name):
    if isinstance(column_name, str):
        match = re.search(r'\d{4}', column_name)
        return match.group(0) if match else column_name
    else:
        return column_name

mapping_quarters = {
    "الربع الأول": "Q1",
    "الربع الثاني": "Q2",
    "الربع الثالث": "Q3",
    "الربع الرابع": "Q4"}

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
            
    
sections_columns_renamed = {
    'الحيوانات الحية والمنتجات الحيوانية': '1_الحيوانات_الحية_والمنتجات_الحيوانية',
    'منتجات نباتية': '2_منتجات نباتية',
    'شحوم ودهون وزيوت حيوانية أو نباتية ومنتجات تفككها؛ دهون غذائية محضرة؛ شموع من أصل حيواني أو نباتي': '3_شحوم_ودهون_وزيوت_حيوانية_أو_نباتية_ومنتجات_تفككها',
    'شحوم ودهون وزيوت حيوانية أو نباتية ومنتجات تفككها، دهون غذائية محضرة، شموع من أصل حيواني أو نباتي': '3_شحوم_ودهون_وزيوت_حيوانية_أو_نباتية_ومنتجات_تفككها',
    'منتجات صناعة الأغذية؛ مشروبات؛ سوائل كحولية وخل؛ تبغ وأبدال تبغ مصنعة': '4_منتجات_صناعة_الأغذية',
    'منتجات صناعة الأغذية، مشروبات، سوائل كحولية وخل، تبغ وأبدال تبغ مصنعة': '4_منتجات_صناعة_الأغذية',
    'المنتجات المعدنية': '5_المنتجات_المعدنية',
    'منتجات الصناعات الكيماوية وما يتصل بها': '6_منتجات_الصناعات_الكيماوية_وما_يتصل_بها',
    'لدائن ومصنوعاتها؛ مطاط ومصنوعاته': '7_لدائن_ومصنوعاتها؛_مطاط_ومصنوعاته',
    'لدائن ومصنوعاتها، مطاط ومصنوعاته': '7_لدائن_ومصنوعاتها؛_مطاط_ومصنوعاته',
    'صلال وجلود خام و جلود مدبوغة وجلود بفراء ومصنوعات هذه المواد؛ أصناف عدة الحيوانات و السراجة؛ لوازم السفر؛ حقائب يدوية وأوعية مماثلة لها؛ مصنوعات من مصارين الحيوانات (عدا مصارين دودة القز)': '8_صلال_وجلود_خام_و_جلود_مدبوغة_وجلود_بفراء_ومصنوعات_هذه_المواد',
    'صلال وجلود خام و جلود مدبوغة وجلود بفراء ومصنوعات هذه المواد، أصناف عدة الحيوانات و السراجة، لوازم السفر، حقائب يدوية وأوعية مماثلة لها، مصنوعات من مصارين الحيوانات (عدا مصارين دودة القز)': '8_صلال_وجلود_خام_و_جلود_مدبوغة_وجلود_بفراء_ومصنوعات_هذه_المواد',

    'خشـب ومصنوعاتــه؛ فحم خشبـــي؛ فلين ومصنوعاته؛ مصنوعات من القش أو من الحلفا أو من مواد الضفر الأُخر؛ أصناف صناعتي الحصر والسلال': '9_خشـب_ومصنوعاتــه',
    'خشـب ومصنوعاتــه، فحم خشبـــي، فلين ومصنوعاته، مصنوعات من القش أو من الحلفا أو من مواد الضفر الأُخر، أصناف صناعتي الحصر والسلال': '9_خشـب_ومصنوعاتــه',
    'عجائن من خشب أو من مواد ليفية سليلوزية أخر؛ ورق أو ورق مقوى (نفايا وفضلات) بغرض إعادة التصنيع (مسترجعة)؛ ورق وورق مقوى ومصنوعاتهما': '10_عجائن_من_خشب_أو_من_مواد_ليفية_سليلوزية_أخر',
    'عجائن من خشب أو من مواد ليفية سليلوزية أخر، ورق أو ورق مقوى (نفايا وفضلات) بغرض إعادة التصنيع (مسترجعة)، ورق وورق مقوى ومصنوعاتهما': '10_عجائن_من_خشب_أو_من_مواد_ليفية_سليلوزية_أخر',

    'مواد نسـجية ومصنوعات من هذه المواد': '11_مواد_نسـجية_ومصنوعات_من_هذه_المواد',
    'أحذية، أغطية رأس، مظلات مطر، مظلات شمس، عصي مشي، عصي بمقاعد، سياط، وسياط الفروسية، أجزاء هذه الأصناف؛ ريش محضر وأصناف مصنوعة منه؛ أزهار اصطناعية؛ مصنوعات من شعر بشري': '12_أحذية،_أغطية_رأس،_مظلات_مطر،_مظلات_شمس،_عصي_مشي،_عصي_بمقاعد،_سياط،_وسياط_الفروسية،_أجزاء_هذه_الأصناف',
    'أحذية، أغطية رأس، مظلات مطر، مظلات شمس، عصي مشي، عصي بمقاعد، سياط، وسياط الفروسية، أجزاء هذه الأصناف، ريش محضر وأصناف مصنوعة منه، أزهار اصطناعية، مصنوعات من شعر بشري': '12_أحذية،_أغطية_رأس،_مظلات_مطر،_مظلات_شمس،_عصي_مشي،_عصي_بمقاعد،_سياط،_وسياط_الفروسية،_أجزاء_هذه_الأصناف',

    'مصنوعات من حجر أو جص أو إسمنت أو حرير صخري (اسبستوس) أو ميكا أو من مواد مماثلة؛ مصنوعات من خزف؛ زجاج ومصنوعاته': '13_مصنوعات_من_حجر_أو_جص_أو_إسمنت_أو_حرير_صخري_اسبستوس_أو_ميكا_أو_من_مواد_مماثلة',
    'مصنوعات من حجر أو جص أو إسمنت أو حرير صخري (اسبستوس) أو ميكا أو من مواد مماثلة، مصنوعات من خزف، زجاج ومصنوعاته': '13_مصنوعات_من_حجر_أو_جص_أو_إسمنت_أو_حرير_صخري_اسبستوس_أو_ميكا_أو_من_مواد_مماثلة',

    'لؤلؤ طبيعي أو مستنبت، أحجار كريمة أو شبه كريمة، معادن ثمينة، معادن عادية مكسوة بقشرة من معادن ثمينة، مصنوعات من هذه المواد؛ حلي الغواية (مقلدة)؛ نقود': '14_لؤلؤ_طبيعي_أو_مستنبت،_أحجار_كريمة_أو_شبه_كريمة،_معادن_ثمينة،_معادن_عادية_مكسوة_بقشرة_من_معادن_ثمينة،_مصنوعات_من_هذه_المواد',
    'لؤلؤ طبيعي أو مستنبت، أحجار كريمة أو شبه كريمة، معادن ثمينة، معادن عادية مكسوة بقشرة من معادن ثمينة، مصنوعات من هذه المواد، حلي الغواية (مقلدة)، نقود': '14_لؤلؤ_طبيعي_أو_مستنبت،_أحجار_كريمة_أو_شبه_كريمة،_معادن_ثمينة،_معادن_عادية_مكسوة_بقشرة_من_معادن_ثمينة،_مصنوعات_من_هذه_المواد',

    'معادن عادية ومصنوعاتها': '15_معادن_عادية_ومصنوعاتها',
    'آلات وأجهزة آلية؛ معدات كهربائية؛ أجزاؤها؛ أجهزة تسجيل واذاعة الصوت والصورة وأجهزة تسجيل واذاعة الصوت والصورة في الإذاعة المرئية (التلفزيون)، أجزاء ولوازم هذه الأجهزة': '16_آلات_وأجهزة_آلية',
    'آلات وأجهزة آلية، معدات كهربائية، أجزاؤها، أجهزة تسجيل واذاعة الصوت والصورة وأجهزة تسجيل واذاعة الصوت والصورة في الإذاعة المرئية (التلفزيون)، أجزاء ولوازم هذه الأجهزة': '16_آلات_وأجهزة_آلية',

    'عربات، طائرات، بواخر، ومعدات نقل مماثلة': '17_عربات،_طائرات،_بواخر،_ومعدات_نقل_مماثلة',
    'أدوات وأجهزة للبصريات أو للتصوير الفوتوغرافي أو للتصوير السينمائي أو للقياس أو للفحص والضبط الدقيق، أدوات وأجهزة للطب أو الجراحة؛ أصناف صناعة الساعات؛ أدوات موسيقية؛ أجزاء ولوازم هذه الأدوات والأجهزة': '18_أدوات_وأجهزة_للبصريات_أو_للتصوير_الفوتوغرافي_أو_للتصوير_السينمائي_أو_للقياس_أو_للفحص_والضبط_الدقيق',
    'أدوات وأجهزة للبصريات أو للتصوير الفوتوغرافي أو للتصوير السينمائي أو للقياس أو للفحص والضبط الدقيق، أدوات وأجهزة للطب أو الجراحة، أصناف صناعة الساعات، أدوات موسيقية، أجزاء ولوازم هذه الأدوات والأجهزة': '18_أدوات_وأجهزة_للبصريات_أو_للتصوير_الفوتوغرافي_أو_للتصوير_السينمائي_أو_للقياس_أو_للفحص_والضبط_الدقيق',

    'أسلحة وذخائر؛ أجزاؤها ولوازمها': '19_أسلحة_وذخائر',
    'أسلحة وذخائر، أجزاؤها ولوازمها': '19_أسلحة_وذخائر',

    'سلع ومنتجات متـنوعة': '20_سلع_ومنتجات_متـنوعة',
    'تحف فنية، قطع للمجموعات وقطع أثرية': '21_تحف_فنية،_قطع_للمجموعات_وقطع_أثرية',
    'الأقسام الدولة': 'الدولة'
}
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

    return quarter, year

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
            rejected_rows = 0       # num_src - count_of_dest
            e.Insert_TO_DMDQ(engine_dmdq, db_name, schema_name, table_name, execution_time, cols, rows, count, datetime.now(), src_table, src_type, rejected_rows)
            logging.info(f"Data load logged successfully for {table_name}.")
    except Exception as error:
        logging.error(f"Error logging data load: {error}")
        raise
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

if __name__ == '__main__':
    main()