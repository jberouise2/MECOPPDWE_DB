import os
import pandas as pd
from sqlalchemy import create_engine

def bypass_jumper_extract_test_result(dir, file, save):
    """
    Extract Test Result of Bypass Jumper and importing it to a csv file.

    Args:
        dir (str): working directory of file
        file (str): file name to extract data from
        save (str): save file name to extract to
    """
    #declaring working directory
    working_directory = dir

    #excel file path
    file_init = file

    save_path = save

    #changing the working directory
    os.chdir(working_directory)

    #importing excel file and parsing all sheets
    excel_file = pd.ExcelFile(file_init)

    #create empty dataframe list
    df_fin = []

    #extracting data from test report table
    for sheet_names in excel_file.sheet_names:
        test_report_id_data = pd.read_excel(file_init, sheet_name=sheet_names, skiprows=7, usecols='U', nrows=3)
        sector_id_data = pd.read_excel(file_init, sheet_name=sheet_names, skiprows=7, usecols='G', nrows=3)
        date_received_data = pd.read_excel(file_init, sheet_name=sheet_names, skiprows=7, usecols='U', nrows=3)
        date_tested_id = pd.read_excel(file_init, sheet_name=sheet_names, skiprows=7, usecols='G', nrows=3)
        equipment_id = 'BYPASS JUMPER'

        # Read the sheet without specifying row limits to capture the entire data
        df = pd.read_excel(file_init, sheet_name=sheet_names)

        # Locate the start row by finding the header row containing 'Item No.'
        start_row = df[df.eq('Item\nNo.').any(axis=1)].index[0]
        
        # Define column names based on header row
        columns = df.iloc[start_row].str.lower().tolist()

        # Extract the table data starting from the row after the header row
        table_data = df.iloc[start_row + 1:].reset_index(drop=True)

        # Find the end of the table by locating the first blank row or any non-relevant section heading
        end_row = table_data[table_data.isnull().all(axis=1)].index.min()  # first fully blank row
        if pd.isna(end_row):  # if no blank row is found, take all rows
            end_row = len(table_data)

        # Select only the rows containing the table data and reset index
        table_data = table_data.iloc[:end_row].reset_index(drop=True)
        
        #column new names
        column_new_name = ['item_no']

        # Set column headers
        table_data.columns = columns
        
        # Drop any rows that are entirely NaN (in case of trailing blanks)
        table_data = table_data.dropna(axis=1)
        #defining additional columns
        col_names = {
            "test_report_id": test_report_id_data.iloc[0,0],
            "sector_id": sector_id_data.iloc[1,0],
            "date_received": date_received_data.iloc[1,0],
            "date_tested": date_tested_id.iloc[2,0],
            "equipment_id": equipment_id
        }
        for col_name, col_val in col_names.items():
            table_data[col_name] = col_val
        
        df_fin.append(table_data)

    #combining all extracted tables to a single table dataframe
    df_fin = pd.concat(df_fin, ignore_index=True)
    
    #assigning datatype for date columns
    for date_col in ['date_tested', 'date_received']:
        #Convert to datetime (if not already in datetime format)
        df_fin[date_col] = pd.to_datetime(df_fin[date_col], errors='coerce')
        #Format the datetime column to the desired string format
        df_fin[date_col] = df_fin[date_col].dt.strftime('%Y/%m/%d')
        
    #dropping rows with na values for columns Control Number
    df_fin = df_fin[df_fin["control no."].notna()]

    #replacing column names
    df_fin.rename(columns={
                            "item\nno.": "item_no.",
                            "control no.": "control_no",
                            "voltage\nrating (kv)": "voltage_rating",
                            "test \nvoltage (kv)": "test_voltage"
                            }, inplace=True)

    #establishing common split delimiter
    df_fin.loc[:,'sector_id'] = df_fin['sector_id'].str.replace('SECTOR', '/ MERALCO')

    #splitting 'MERALCO' from sector_id and appending it to new columns company_id
    df_fin[['sector_id','company_id']] = df_fin['sector_id'].str.split(' / ', expand=True)
    #rearranging columns
    df_fin = df_fin.loc[:,[
                            "equipment_id",
                            "test_report_id",
                            "sector_id",
                            "company_id",
                            "control_no",
                            "brand",
                            "type",
                            "voltage_rating",
                            "test_voltage",
                            "result",
                            "date_received",
                            "date_tested"
                            ]]
    
    #stripping leading and trailing spaces
    for col in ["sector_id","company_id"]:
        df_fin.loc[:,col] = df_fin[col].str.strip()
    
    df_fin.to_csv(save_path, index=False)
    
    print(df_fin)
    print(f"\nSuccesfully Saved File to {save_path}\n")

    
############################################################################    
    
def bypass_jumper_to_postgres(dir, file, database):
    """
        Imports the csv file to postgres database.
        
    Args:
        dir (str): working directory of file
        file (str): csv file to migrate to postgres database
        database (str): database name
    """
    #declaring working directory
    working_directory = dir
    
    #csv file path
    file_init = file
    
    #changing the working directory
    os.chdir(working_directory)
    
    df_fin = pd.read_csv(file_init, sep=',')
    
    engine = create_engine(f"postgresql+psycopg2://airflow:airflow@localhost:5432/{database}")
    df_fin.to_sql('bypass_jumper', engine, if_exists='append', index=False)
    print(f"Successfully Migrated {df_fin.shape[0]} Data to {database} Database\n")