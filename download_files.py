# -*- coding: utf-8 -*-
"""
Created on Sun Oct 13 15:37:08 2019

@author: hewi
"""

#### IF error : "ModuleNotFOundError: no module named PyPDF2"
   # then uncomment line below (i.e. remove the #):
       
#pip install PyPDF2

import pandas as pd
import PyPDF2
from pathlib import Path
import shutil, os
import os.path
import glob
import requests
import json


headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'}

###!!NB!! column with URL's should be called: "Pdf_URL" and the year should be in column named: "Pub_Year"

### File names will be the ID from the ID column (e.g. BR2005.pdf)

########## EDIT HERE:
    
### specify path to file containing the URLs
list_pth = 'data/GRI_2017_2020.xlsx'

###specify Output folder (in this case it moves one folder up and saves in the script output folder)
pth = 'output/'

###Specify path for existing downloads
dwn_pth = 'output/'

### cheack for files already downloaded
dwn_files = glob.glob(os.path.join(dwn_pth, "*.pdf")) 
exist = [os.path.basename(f)[:-4] for f in dwn_files]

###specify the ID column name
ID = "BRnum"


##########

### read in file
df = pd.read_excel(list_pth, sheet_name=0, index_col=ID)

### filter out rows with no URL
non_empty = df.Pdf_URL.notnull() == True
df = df[non_empty]
df2 = df.copy()


#writer = pd.ExcelWriter(pth+'check_3.xlsx', engine='xlsxwriter', options={'strings_to_urls': False})



### filter out rows that have been downloaded
df2 = df2[~df2.index.isin(exist)]

def verify_pdf(content: bytes) -> bool:
    PDF_MAGIC_BYTES = b'%PDF-'
    return content.startswith(PDF_MAGIC_BYTES)
    
def download_file(dataframe: pd.DataFrame, index: int) -> tuple[bool, int]:
    status = False
    status_code = 0
    save_file_name = str(pth + str(index) + ".pdf")

    pdf_url = dataframe.loc[index, "Pdf_URL"]
    secondary_pdf_url = dataframe.loc[index, "Report Html Address"]
    urls = [str(url) for url in [pdf_url, secondary_pdf_url] if pd.notna(url)] # Converts to valid entries to string

    try:
        for url in urls:
            if url == "nan":
                continue
            response = requests.get(url, timeout=5, stream=True, headers=headers)

            byte_length = response.headers.get("Content-Length")
            

            if not verify_pdf(response.content):
                continue

            if response.ok:
                with open(save_file_name, "wb") as file:
                    for content in response.iter_content(chunk_size=8192):
                        file.write(content)
                    print(f"Successfully downloaded and wrote file: {index}")
                    status = True
                    status_code = 200
                    break
            else:
                print(f"Error downloading file: {index}, status code: {response.status_code}")
            status_code = response.status_code

    except TimeoutError as te:
        print(f"Timeout error: {te}")
        dataframe.loc[index, "error"] = str(te)

    except ConnectionError as ce:
        print(f"Connection error: {ce}")
        dataframe.loc[index, "error"] = str(ce)

    except requests.exceptions.RequestException as e:
        dataframe.at[index, "error"] = str(e)
        print(f"Error with file {save_file_name} at url: {url}")
        print(f"{e}")
        status_code = 600
    

    return status, status_code


#df2.to_excel(writer, sheet_name="dwn")
#writer.save()
#writer.close()

def write_status_to_file(status: dict) -> None:
    filename = "logs/status.txt"
    with open(filename, "w") as file:
        for key, value in status.items():
            file.write(f"{key}: has status: {value}\n")

def write_status_to_json(status: dict) -> None:
    with open("logs/status.json", w) as file:
        json.dump(status, f, indent = 2)

def read_json(filepath: Path) -> dict:
    with open(filepath, 'r') as file:
        status = json.load(f)
    return status

def write_file_to_memory(filepath: Path) -> dict:
    status = {}
    with open(filepath, 'r') as file:
        content = [file.split() for file in file.readlines()]
    return status


def main() -> None:
    status = read_json("logs/status.json")

    for row in df2.index[400:420]:
        download_state = download_file(df2, row)
        status[row] = (download_state)
        write_status_to_json(status)

     

if __name__ == "__main__":
    main()
     
     

"""
        #if os.path.isfile(savefile):
            #try:
                pdfFileObj = open(savefile, 'rb')
               # creating a pdf reader object
                pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
                with open(savefile, 'rb') as pdfFileObj:
                    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
                    if pdfReader.numPages > 0:
                        df2.at[j, 'pdf_downloaded'] = "yes"
                    else:
                        df2.at[j, 'pdf_downloaded'] = "file_error"
               
            #except Exception as e:
               # df2.at[j, 'pdf_downloaded'] = str(e)
                #print(str(str(j)+" " + str(e)))
        #else:
            #df2.at[j, 'pdf_downloaded'] = "404"
            #print("not a file")
"""