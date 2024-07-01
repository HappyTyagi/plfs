import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
conn =  create_engine(f'{os.getenv("ENGINE")}://{os.getenv("DTABASE_USER")}:{os.getenv("PASSWORD")}@{os.getenv("HOST")}/{os.getenv("IIP_DATABASE")}')
db_url = 'postgresql://postgres:root@localhost:5432/PLFS'

num_str = input("Enter an Year: ")
filePAth = 'D:/PLFS/PLFSSheet.xlsx'

excel_file = pd.ExcelFile(filePAth)
sheet_namesList = excel_file.sheet_names

for key in sheet_namesList:
    if 'Table No.6' in key:
        allExclData =  pd.read_excel(filePAth, sheet_name=key, header=None , dtype='str')

        readAllHeader = allExclData[0:5]  # Get Only heder 
        first_column_values = readAllHeader[1]
        
        indicator = first_column_values[0]   # Get indicator 
        sector = first_column_values[1]      # Get sector 
        gender = first_column_values[2]      # Get gender 
        age = first_column_values[3]         # Get age 
        status = first_column_values[4]      # Get status  

        allTransData = allExclData[5:]

        # allTransData["Indicator"] = [indicator]

        allTransData.insert(len(allTransData.columns),'year',num_str)
        allTransData.insert(len(allTransData.columns),'indicator',indicator)
        allTransData.insert(len(allTransData.columns),'sector',sector)
        allTransData.insert(len(allTransData.columns),'gender',gender)
        allTransData.insert(len(allTransData.columns),'age',age)
        allTransData.insert(len(allTransData.columns),'status',status)
        


        # allTransData = allTransData.dropna(axis=0, how='any')
        # allTransData = allTransData.T  # change axis 
        # allTransData.columns = allTransData.iloc[0]  # Set the header row as the DataFrame's header
        # allTransData = allTransData[1:]  # Remove the header row from the DataFrame

        # Reset the index after removing the header row
        allTransData.reset_index(drop=True, inplace=True)
        allTransData.to_excel('D:/PLFS/TEXT.xlsx')
        print("----"+allTransData)
        # for key in allTransData.columns:
        #     print("----"+key)
            


            # plfsData = pd.DataFrame({
            #     'plfs_fact_code': '',
            #     'indicator_value': indicator,
            #     'age_group_code': age,
            #     'group_code': '',
            #     'gender_code': gender,
            #     'indicator_code': '',
            #     'state_code': '2', 
            #     'frequency_code':  '1', 
            #     'sector_code': sector,
            #     'status_type_code': status,
            #     'indicator_specific_filter_id': '',
            #     'created_by': 'System',
            #     'updated_by': 'System',
            #     'created_at': 'System',
            #     'updated_at': 'System',
            #     'status': "Active"})

            # engine = create_engine(db_url)
            # table_name = 'plfs_fact'

            # plfsData.to_sql(table_name, engine, if_exists='append', index=False)
            # print(f'PLFS FACT Updated')
            # break


         


     



 

     
     





