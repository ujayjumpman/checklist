import streamlit as st
import pandas as pd
import requests
import json
import openpyxl
import time
import math
from io import BytesIO
from datetime import datetime
import io
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter
import ibm_boto3
from ibm_botocore.client import Config
import io

# Initialize lists to store counts of green (1) and non-green (0)
ews1 = []
ews2 = []
ews3 = []
lig1 = []
lig2 = []
lig3 = []

def EWS1(sheet, selected_year, selected_month):
    """EWS Tower 1: row 8 to 22 columns D, H, L, P"""
    rows = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
    cols = ['D', 'H', 'L', 'P']
    st.write(f"Selected Year: {selected_year}, Selected Month: {selected_month}")
    
    for col in cols:
        for row in rows:
            cell = sheet[f"{col}{row}"]
            value = cell.value if cell.value is not None else ""
            
            # Try to parse the datetime string into a datetime object
            try:
                dt_object = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
                year = dt_object.year
                month = dt_object.month
            except ValueError:
                # Handle the case where the value is not a valid datetime string
                year = None
                month = None
                # st.write(f"Invalid date format in cell {col}{row}: {value}")
            
            # Get background color of the cell
            fill = cell.fill
            if fill.start_color.type == 'rgb' and fill.start_color.rgb:
                bg_color = f"#{fill.start_color.rgb[-6:]}"  # Extract the last 6 chars for hex color
            else:
                bg_color = "#FFFFFF"  # Default to white if no background color is set

            # Adjust the condition: Check if the year or month matches the selected values
            if bg_color == "#92D050" and (
                str(year) != str(selected_year) or str(month) != str(selected_month)
            ):
                # st.write(f"Cell {col}{row}: {value}")
                ews1.append(1)
            
            # Additional condition for background color #0070C0
            if bg_color == "#0070C0":
                ews1.append(0)


def EWS2(sheet, selected_year, selected_month):
    """EWS Tower 2: row 8 to 22 columns U, Y, AC, AG"""
    rows = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
    cols = ['U', 'Y', 'AC', 'AG']
    for col in cols:
        for row in rows:
            cell = sheet[f"{col}{row}"]
            value = cell.value if cell.value is not None else ""

            try:
                dt_object = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
                year = dt_object.year
                month = dt_object.month
            except ValueError:
                # Handle the case where the value is not a valid datetime string
                year = None
                month = None
                # st.write(f"Invalid date format in cell {col}{row}: {value}")
        
            fill = cell.fill
            if fill.start_color.type == 'rgb' and fill.start_color.rgb:
                bg_color = f"#{fill.start_color.rgb[-6:]}"  # Extract the last 6 chars for hex color
            else:
                bg_color = "#FFFFFF"  # Default to white if no background color is set
    
            if bg_color == "#92D050" and (
                str(year) != str(selected_year) or str(month) != str(selected_month)
            ):
                # st.write(f"Cell {col}{row}: {value}")
                ews2.append(1)
            
            # Additional condition for background color #0070C0
            if bg_color == "#0070C0":
                ews2.append(0)


def EWS3(sheet, selected_year, selected_month):
    """EWS Tower 3: row 8 to 22 columns AL, AP, AT, AX"""
    rows = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
    cols = ['AL', 'AP', 'AT', 'AX']
    for col in cols:
        for row in rows:
            cell = sheet[f"{col}{row}"]
            value = cell.value if cell.value is not None else ""

            try:
                dt_object = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
                year = dt_object.year
                month = dt_object.month
            except ValueError:
                # Handle the case where the value is not a valid datetime string
                year = None
                month = None
                # st.write(f"Invalid date format in cell {col}{row}: {value}")
        
            fill = cell.fill
            if fill.start_color.type == 'rgb' and fill.start_color.rgb:
                bg_color = f"#{fill.start_color.rgb[-6:]}"  # Extract the last 6 chars for hex color
            else:
                bg_color = "#FFFFFF"  # Default to white if no background color is set

            if bg_color == "#92D050" and (
                str(year) != str(selected_year) or str(month) != str(selected_month)
            ):
                # st.write(f"Cell {col}{row}: {value}")
                ews3.append(1)
            
            # Additional condition for background color #0070C0
            if bg_color == "#0070C0":
                ews3.append(0)


def LIG1(sheet, selected_year, selected_month):
    """LIG Tower 1: row 30 to 44 columns AL, AP, AT, AX"""
    rows = [30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44]
    cols = ['AL', 'AP', 'AT', 'AX']
    for col in cols:
        for row in rows:
            cell = sheet[f"{col}{row}"]
            value = cell.value if cell.value is not None else ""

            try:
                dt_object = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
                year = dt_object.year
                month = dt_object.month
            except ValueError:
                # Handle the case where the value is not a valid datetime string
                year = None
                month = None
                # st.write(f"Invalid date format in cell {col}{row}: {value}")
        
            fill = cell.fill
            if fill.start_color.type == 'rgb' and fill.start_color.rgb:
                bg_color = f"#{fill.start_color.rgb[-6:]}"  # Extract the last 6 chars for hex color
            else:
                bg_color = "#FFFFFF"  # Default to white if no background color is set
           
            if bg_color == "#92D050" and (
                str(year) != str(selected_year) or str(month) != str(selected_month)
            ):
                # st.write(f"Cell {col}{row}: {value}")
                lig1.append(1)
            
            # Additional condition for background color #0070C0
            if bg_color == "#0070C0":
                lig1.append(0)


def LIG2(sheet, selected_year, selected_month):
    """LIG Tower 2: row 30 to 44 columns U, Y, AC, AG"""
    rows = [30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44]
    cols = ['U', 'Y', 'AC', 'AG']
    for col in cols:
        for row in rows:
            cell = sheet[f"{col}{row}"]
            value = cell.value if cell.value is not None else ""

            try:
                dt_object = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
                year = dt_object.year
                month = dt_object.month
            except ValueError:
                # Handle the case where the value is not a valid datetime string
                year = None
                month = None
            
            fill = cell.fill
            if fill.start_color.type == 'rgb' and fill.start_color.rgb:
                bg_color = f"#{fill.start_color.rgb[-6:]}"  # Extract the last 6 chars for hex color
            else:
                bg_color = "#FFFFFF"  # Default to white if no background color is set

            if bg_color == "#92D050" and (
                str(year) != str(selected_year) or str(month) != str(selected_month)
            ):
                # st.write(f"Cell {col}{row}: {value}")
                lig2.append(1)
            
            # Additional condition for background color #0070C0
            if bg_color == "#0070C0":
                lig2.append(0)

def LIG3(sheet, selected_year, selected_month):
    """LIG Tower 3: row 30 to 44 columns D, H, L, P"""
    rows = [30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44]
    cols = ['D', 'H', 'L', 'P']
    for col in cols:
        for row in rows:
            cell = sheet[f"{col}{row}"]
            value = cell.value if cell.value is not None else ""

            try:
                dt_object = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
                year = dt_object.year
                month = dt_object.month
            except ValueError:
                # Handle the case where the value is not a valid datetime string
                year = None
                month = None
            
            fill = cell.fill
            if fill.start_color.type == 'rgb' and fill.start_color.rgb:
                bg_color = f"#{fill.start_color.rgb[-6:]}"  # Extract the last 6 chars for hex color
            else:
                bg_color = "#FFFFFF"  # Default to white if no background color is set

            if bg_color == "#92D050" and (
                str(year) != str(selected_year) or str(month) != str(selected_month)
            ):
                # st.write(f"Cell {col}{row}: {value}")
                lig3.append(1)
            
            # Additional condition for background color #0070C0
            if bg_color == "#0070C0":
                lig3.append(0)

def Processjson(data):
    json_data = []
    for project, tower, green, non_green, finishing in zip(
    data["Project Name"],
    data["Tower"],
    data["Green (1)"],
    data["Non-Green (0)"],
    data["Finishing"]
):
        total = green + non_green
        structure = f"{math.ceil(green / total * 100)}%" if total > 0 else "0%"
        
        entry = {
            "Project": project,
            "Tower Name": tower,
            "Structure": structure,
            "Finishing": finishing
        }
        json_data.append(entry)
    
    return json_data

def ProcessEWS_LIG(exceldatas, selected_year, selected_month):

    wb = load_workbook(exceldatas, data_only=True)
    sheet_names = wb.sheetnames
    sheet_name = "Revised Baseline 45daysNGT+Rai"

    sheet = wb[sheet_name]

    ews1.clear()
    ews2.clear()
    ews3.clear()
    lig1.clear()
    lig2.clear()
    lig3.clear()
   
    EWS1(sheet, selected_year, selected_month)
    EWS2(sheet, selected_year, selected_month)
    EWS3(sheet, selected_year, selected_month)
    LIG1(sheet, selected_year, selected_month)
    LIG2(sheet, selected_year, selected_month)
    LIG3(sheet, selected_year, selected_month)

    # Filter data based on the selected month and year
    # We'll need a column in the sheet that contains the date or timestamp for filtering
    # For simplicity, assume that the sheet has a 'Date' column in the 1st row (add logic as per actual data)
    
    # Example of filtering the "Finishing" data based on the date column (we'll assume there's a 'Date' column).
    
    data = {
        "Project Name": ["EWS", "EWS", "EWS", "LIG", "LIG", "LIG"],
        "Tower": ["EWST1", "EWST2", "EWST3", "LIGT1", "LIGT2", "LIGT3"],
        "Green (1)": [ews1.count(1), ews2.count(1), ews3.count(1), lig1.count(1), lig2.count(1), lig3.count(1)],
        "Non-Green (0)": [ews1.count(0), ews2.count(0), ews3.count(0), lig1.count(0), lig2.count(0), lig3.count(0)],
        "Finishing": ["0%","0%","0%","0%","0%","0%"]
    }

    # Calculate average percentage of green
    green_counts = data["Green (1)"]
    non_green_counts = data["Non-Green (0)"]
    averages = []

    for green, non_green in zip(green_counts, non_green_counts):
        total = green + non_green
        avg = (green / total) * 100 if total > 0 else 0  # avoids division by zero
        averages.append(avg)

    data["Structure"] = data["Average (%)"] = [f"{(green / (green + non_green) * 100):.2f}%" if (green + non_green) > 0 else "0.00%" 
                       for green, non_green in zip(green_counts, non_green_counts)]
    
    json_data = Processjson(data)
    st.table(data)
    st.dataframe(data)

    return json_data