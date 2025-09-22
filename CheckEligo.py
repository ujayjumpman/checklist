 
# -*- coding: utf-8 -*-
import streamlit as st
import requests
import json 
import urllib.parse
import urllib3
import certifi
import pandas as pd
from datetime import datetime
import re
import logging
import os
from dotenv import load_dotenv
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import openpyxl
import io
from uuid import uuid4
import ibm_boto3
from ibm_botocore.client import Config
from tenacity import retry, stop_after_attempt, wait_exponential
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side
from io import BytesIO
import traceback
from Tower_G_and_H import *
from datetime import date
import concurrent.futures
from dateutil.relativedelta import relativedelta





# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables
load_dotenv()

# IBM COS Configuration
COS_API_KEY = os.getenv("COS_API_KEY")
COS_SERVICE_INSTANCE_ID = os.getenv("COS_SERVICE_INSTANCE_ID")
COS_ENDPOINT = os.getenv("COS_ENDPOINT")
COS_BUCKET = os.getenv("COS_BUCKET")

# WatsonX configuration
WATSONX_API_URL = os.getenv("WATSONX_API_URL_1")
MODEL_ID = os.getenv("MODEL_ID_1")
PROJECT_ID = os.getenv("PROJECT_ID_1")
API_KEY = os.getenv("API_KEY_1")

# API Endpoints
LOGIN_URL = "https://dms.asite.com/apilogin/"
IAM_TOKEN_URL = "https://iam.cloud.ibm.com/identity/token"

# Login Function
async def login_to_asite(email, password):
    headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    payload = {"emailId": email, "password": password}
    try:
        response = requests.post(LOGIN_URL, headers=headers, data=payload, verify=certifi.where(), timeout=50)
        if response.status_code == 200:
            try:
                session_id = response.json().get("UserProfile", {}).get("Sessionid")
                if session_id:
                    logger.info(f"Login successful, Session ID: {session_id}")
                    st.session_state.sessionid = session_id
                    st.sidebar.success(f"… Login successful, Session ID: {session_id}")
                    return session_id
                else:
                    logger.error("No Session ID found in login response")
                    st.sidebar.error(" No Session ID in response")
                    return None
            except json.JSONDecodeError:
                logger.error("JSONDecodeError during login")
                st.sidebar.error(" Failed to parse login response")
                return None
        logger.error(f"Login failed: {response.status_code} - {response.text}")
        st.sidebar.error(f" Login failed: {response.status_code}")
        return None
    except Exception as e:
        logger.error(f"Error during login: {str(e)}")
        st.sidebar.error(f" Login error: {str(e)}")
        return None

# Function to generate access token
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=10, max=60))
def get_access_token(api_key):
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
    data = {"grant_type": "urn:ibm:params:oauth:grant-type:apikey", "apikey": api_key}
    response = requests.post(IAM_TOKEN_URL, headers=headers, data=data, verify=certifi.where(), timeout=50)
    try:
        if response.status_code == 200:
            token_info = response.json()
            logger.info("Access token generated successfully")
            return token_info['access_token']
        else:
            logger.error(f"Failed to get access token: {response.status_code} - {response.text}")
            st.error(f" Failed to get access token: {response.status_code} - {response.text}")
            raise Exception("Failed to get access token")
    except Exception as e:
        logger.error(f"Exception getting access token: {str(e)}")
        st.error(f" Error getting access token: {str(e)}")
        return None

# Initialize COS client
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
def initialize_cos_client():
    try:
        logger.info("Attempting to initialize COS client...")
        cos_client = ibm_boto3.client(
            's3',
            ibm_api_key_id=COS_API_KEY,
            ibm_service_instance_id=COS_SERVICE_INSTANCE_ID,
            config=Config(
                signature_version='oauth',
                connect_timeout=180,
                read_timeout=180,
                retries={'max_attempts': 15}
            ),
            endpoint_url=COS_ENDPOINT
        )
        logger.info("COS client initialized successfully")
        return cos_client
    except Exception as e:
        logger.error(f"Error initializing COS client: {str(e)}")
        st.error(f" Error initializing COS client: {str(e)}")
        raise

async def validate_session():
    url = "https://dmsak.asite.com/api/workspace/workspacelist"
    headers = {'Cookie': f'ASessionID={st.session_state.sessionid}'}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                logger.info("Session validated successfully")
                return True
            else:
                logger.error(f"Session validation failed: {response.status} - {await response.text()}")
                return False

async def refresh_session_if_needed():
    if 'sessionid' not in st.session_state or not st.session_state.sessionid:
        logger.warning("No session ID found in session state, attempting login")
        new_session_id = await login_to_asite(os.getenv("ASITE_EMAIL"), os.getenv("ASITE_PASSWORD"))
        if new_session_id:
            st.session_state.sessionid = new_session_id
            return new_session_id
        else:
            raise Exception("Failed to establish initial session")

    if not await validate_session():
        logger.info("Session invalid, attempting to refresh")
        new_session_id = await login_to_asite(os.getenv("ASITE_EMAIL"), os.getenv("ASITE_PASSWORD"))
        if new_session_id:
            st.session_state.sessionid = new_session_id
            logger.info(f"Session refreshed successfully, new Session ID: {new_session_id}")
            return new_session_id
        else:
            raise Exception("Failed to refresh session")
    logger.info("Session is valid, no refresh needed")
    return st.session_state.sessionid

# Fetch Workspace ID
async def GetWorkspaceID():
    await refresh_session_if_needed()
    url = "https://dmsak.asite.com/api/workspace/workspacelist"
    headers = {
        'Cookie': f'ASessionID={st.session_state.sessionid}',
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    response = requests.get(url, headers=headers)
    st.session_state.workspaceid = response.json()['asiteDataList']['workspaceVO'][1]['Workspace_Id']
    st.write(f"Workspace ID: {st.session_state.workspaceid}")

# Fetch Project IDs
async def GetProjectId():
    await refresh_session_if_needed()
    url = f"https://adoddleak.asite.com/commonapi/qaplan/getQualityPlanList;searchCriteria={{'criteria': [{{'field': 'planCreationDate','operator': 6,'values': ['11-Mar-2025']}}], 'projectId': {str(st.session_state.workspaceid)}, 'recordLimit': 1000, 'recordStart': 1}}"
    headers = {
        'Cookie': f'ASessionID={st.session_state.sessionid}',
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        logger.info(f"GetProjectId response: {json.dumps(data, indent=2)}")
        if 'data' not in data or not data['data']:
            st.error(" No project data found in GetProjectId response")
            logger.error("No project data found in GetProjectId response")
            return
        st.session_state.ELIGO_Structure = data['data'][0]['planId']
        st.session_state.Eligo_Tower_F_Finishing = data['data'][1]['planId']
        st.session_state.Eligo_Tower_G_Finishing = data['data'][2]['planId']    
        st.session_state.Eligo_Tower_H_Finishing = data['data'][4]['planId']
        st.session_state.Eligo_Non_Tower_Area_Finishing = data['data'][3]['planId']
        st.write(f"ELIGO - Structure Project ID: {st.session_state.ELIGO_Structure}")
        st.write(f"ELIGO - Tower F Finishing Project ID: {st.session_state.Eligo_Tower_F_Finishing}")
        st.write(f"ELIGO - Tower G Finishing Project ID: {st.session_state.Eligo_Tower_G_Finishing}")
        st.write(f"ELIGO - Tower H Finishing Project ID: {st.session_state.Eligo_Tower_H_Finishing}")
        st.write(f"ELIGO - Non Tower Area Finishing Project ID: {st.session_state.Eligo_Non_Tower_Area_Finishing}")
    except Exception as e:
        st.error(f" Error fetching Project IDs: {str(e)}")
        logger.error(f"Error fetching Project IDs: {str(e)}")  


# Asynchronous Fetch Function with Retry Logic
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def fetch_data(session, url, headers, email=None, password=None):
    try:
        logger.info(f"Fetching data from URL: {url}")
        async with session.get(url, headers=headers) as response:
            content_type = response.headers.get('Content-Type', '')
            logger.info(f"Response Content-Type: {content_type}")
            if response.status == 200:
                if 'application/json' in content_type:
                    return await response.json()
                else:
                    text = await response.text()
                    logger.error(f"Unexpected Content-Type: {content_type}, Response: {text[:500]}")
                    raise ValueError(f"Unexpected Content-Type: {content_type}, expected application/json")
            elif response.status == 204:
                logger.info("No content returned (204)")
                return None
            else:
                text = await response.text()
                logger.error(f"Error fetching data: {response.status} - {text[:500]}")
                if response.status == 401:
                    raise Exception("Unauthorized: Session may have expired")
                raise Exception(f"Error fetching data: {response.status} - {text[:500]}")
    except Exception as e:
        logger.error(f"Fetch failed: {str(e)}")
        raise

# Fetch All Data with Async
async def GetAllDatas():
    record_limit = 1000
    all_external_data = []
    all_structure_data = []
    all_finishing_data = []

    # Ensure session is valid before starting
    await refresh_session_if_needed()
    headers = {'Cookie': f'ASessionID={st.session_state.sessionid}'}

    async with aiohttp.ClientSession() as session:
       
        # Fetch ELIGO Structure data
        start_record = 1
        st.write("Fetching ELIGO Structure data...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanAssociation/?projectId={st.session_state.workspaceid}&planId={st.session_state.ELIGO_Structure}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                await refresh_session_if_needed()
                headers['Cookie'] = f'ASessionID={st.session_state.sessionid}'
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more Structure data available (204)")
                    break
                if 'associationList' in data and data['associationList']:
                    all_structure_data.extend(data['associationList'])
                else:
                    all_structure_data.extend(data if isinstance(data, list) else [])
                st.write(f"Fetched {len(all_structure_data[-record_limit:])} Structure records (Total: {len(all_structure_data)})")
                if len(all_structure_data[-record_limit:]) < record_limit:
                    break
                start_record += record_limit
                await asyncio.sleep(1)  # Rate limiting
            except Exception as e:
                st.error(f" Error fetching Structure data: {str(e)}")
                logger.error(f"Structure data fetch failed: {str(e)}")
                break

        # Fetch ELIGO Tower F Finishing data
        start_record = 1
        st.write("Fetching ELIGO Tower F Finishing data...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanAssociation/?projectId={st.session_state.workspaceid}&planId={st.session_state.Eligo_Tower_F_Finishing}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                # Refresh session before each major fetch to ensure validity
                await refresh_session_if_needed()
                headers['Cookie'] = f'ASessionID={st.session_state.sessionid}'
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more Finishing data available (204)")
                    break
                if 'associationList' in data and data['associationList']:
                    all_finishing_data.extend(data['associationList'])
                else:
                    all_finishing_data.extend(data if isinstance(data, list) else [])
                st.write(f"Fetched {len(all_finishing_data[-record_limit:])} Finishing records (Total: {len(all_finishing_data)})")
                if len(all_finishing_data[-record_limit:]) < record_limit:
                    break
                start_record += record_limit
                await asyncio.sleep(1)  # Rate limiting
            except Exception as e:
                st.error(f" Error fetching Finishing data: {str(e)}")
                logger.error(f"Finishing data fetch failed: {str(e)}")
                break


        # Fetch ELIGO Tower G Finishing data
        start_record = 1
        st.write("Fetching ELIGO Tower G Finishing data...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanAssociation/?projectId={st.session_state.workspaceid}&planId={st.session_state.Eligo_Tower_G_Finishing}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                await refresh_session_if_needed()
                headers['Cookie'] = f'ASessionID={st.session_state.sessionid}'
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more ELIGO Tower G Finishing data available (204)")
                    break
                if 'associationList' in data and data['associationList']:
                    all_external_data.extend(data['associationList'])
                else:
                    all_external_data.extend(data if isinstance(data, list) else [])
                st.write(f"Fetched {len(all_external_data[-record_limit:])} ELIGO Tower G Finishing records (Total: {len(all_external_data)})")
                if len(all_external_data[-record_limit:]) < record_limit:
                    break
                start_record += record_limit
                await asyncio.sleep(1)  # Rate limiting
            except Exception as e:
                st.error(f" Error fetching ELIGO Tower G Finishing data: {str(e)}")
                logger.error(f"ELIGO Tower G Finishing data fetch failed: {str(e)}")
                break

    df_finishing = pd.DataFrame(all_finishing_data)
    df_structure = pd.DataFrame(all_structure_data)
    df_external = pd.DataFrame(all_external_data)
    desired_columns = ['activitySeq', 'qiLocationId']
    if 'statusName' in df_finishing.columns:
        desired_columns.append('statusName')
    elif 'statusColor' in df_finishing.columns:
        desired_columns.append('statusColor')
        status_mapping = {'#4CAF50': 'Completed', '#4CB0F0': 'Not Started', '#4C0F0': 'Not Started'}
        df_finishing['statusName'] = df_finishing['statusColor'].map(status_mapping).fillna('Unknown')
        df_structure['statusName'] = df_structure['statusColor'].map(status_mapping).fillna('Unknown')
        df_external['statusName'] = df_external['statusColor'].map(status_mapping).fillna('Unknown')
        
    else:
        st.error(" Neither statusName nor statusColor found in data!")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    eligo_tower_f_finishing = df_finishing[desired_columns]
    eligo_structure = df_structure[desired_columns]
    eligo_tower_g_finishing = df_external[desired_columns]

    st.write(f"ELIGO TOWER F FINISHING ({', '.join(desired_columns)})")
    st.write(f"Total records: {len(eligo_tower_f_finishing)}")
    st.write(eligo_tower_f_finishing)
    st.write(f"ELIGO STRUCTURE ({', '.join(desired_columns)})")
    st.write(f"Total records: {len(eligo_structure)}")
    st.write(eligo_structure)
    st.write(f"ELIGO TOWER G FINISHING ({', '.join(desired_columns)})")
    st.write(f"Total records: {len(eligo_tower_g_finishing)}")
    st.write(eligo_tower_g_finishing)

    return eligo_tower_f_finishing, eligo_structure, eligo_tower_g_finishing

# Fetch Activity Data with Async
async def Get_Activity():
    record_limit = 1000
    headers = {
        'Cookie': f'ASessionID={st.session_state.sessionid}',
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    all_finishing_activity_data = []
    all_structure_activity_data = []
    all_external_activity_data = []
  

    # Ensure session is valid before starting
    await refresh_session_if_needed()

    async with aiohttp.ClientSession() as session:
        # Fetch ELIGO Tower F Finishing Activity data
        start_record = 1
        st.write("Fetching Activity data for ELIGO Tower F Finishing...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanActivities/?projectId={st.session_state.workspaceid}&planId={st.session_state.Eligo_Tower_F_Finishing}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                await refresh_session_if_needed()
                headers['Cookie'] = f'ASessionID={st.session_state.sessionid}'
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more ELIGO Tower F Finishing Activity data available (204)")
                    break
                if 'activityList' in data and data['activityList']:
                    all_finishing_activity_data.extend(data['activityList'])
                else:
                    all_finishing_activity_data.extend(data if isinstance(data, list) else [])
                st.write(f"Fetched {len(all_finishing_activity_data[-record_limit:])} Finishing Activity records (Total: {len(all_finishing_activity_data)})")
                if len(all_finishing_activity_data[-record_limit:]) < record_limit:
                    break
                start_record += record_limit
                await asyncio.sleep(1)  # Rate limiting
            except Exception as e:
                st.error(f" Error fetching ELIGO Tower F Finishing Activity data: {str(e)}")
                logger.error(f"ELIGO Tower F Finishing Activity fetch failed: {str(e)}")
                break

        # Fetch ELIGO Structure Activity data
        start_record = 1
        st.write("Fetching Activity data for ELIGO Structure...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanActivities/?projectId={st.session_state.workspaceid}&planId={st.session_state.ELIGO_Structure}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                await refresh_session_if_needed()
                headers['Cookie'] = f'ASessionID={st.session_state.sessionid}'
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more Structure Activity data available (204)")
                    break
                if 'activityList' in data and data['activityList']:
                    all_structure_activity_data.extend(data['activityList'])
                else:
                    all_structure_activity_data.extend(data if isinstance(data, list) else [])
                st.write(f"Fetched {len(all_structure_activity_data[-record_limit:])} Structure Activity records (Total: {len(all_structure_activity_data)})")
                if len(all_structure_activity_data[-record_limit:]) < record_limit:
                    break
                start_record += record_limit
                await asyncio.sleep(1)  # Rate limiting
            except Exception as e:
                st.error(f" Error fetching Structure Activity data: {str(e)}")
                logger.error(f"Structure Activity fetch failed: {str(e)}")
                break

        # Fetch ELIGO Tower G Finishing Activity data
        start_record = 1
        st.write("Fetching Activity data for ELIGO Tower G Finishing...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanActivities/?projectId={st.session_state.workspaceid}&planId={st.session_state.Eligo_Tower_G_Finishing}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                await refresh_session_if_needed()
                headers['Cookie'] = f'ASessionID={st.session_state.sessionid}'
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more ELIGO Tower G Finishing Activity data available (204)")
                    break
                if 'activityList' in data and data['activityList']:
                    all_external_activity_data.extend(data['activityList'])
                else:
                    all_external_activity_data.extend(data if isinstance(data, list) else [])
                st.write(f"Fetched {len(all_external_activity_data[-record_limit:])} ELIGO Tower G Finishing Activity records (Total: {len(all_external_activity_data)})")
                if len(all_external_activity_data[-record_limit:]) < record_limit:
                    break
                start_record += record_limit
                await asyncio.sleep(1)  # Rate limiting
            except Exception as e:
                st.error(f" Error fetching ELIGO Tower G Finishing Activity data: {str(e)}")
                logger.error(f"ELIGO Tower G Finishing Activity fetch failed: {str(e)}")
                break

    def safe_select(df, cols):
        if df.empty:
            return pd.DataFrame(columns=cols)
        missing = [col for col in cols if col not in df.columns]
        if missing:
            logger.warning(f"Missing columns in activity data: {missing}")
            for col in missing:
                df[col] = None
        return df[cols]

    finishing_activity_data = safe_select(pd.DataFrame(all_finishing_activity_data), ['activityName', 'activitySeq', 'formTypeId'])
    structure_activity_data = safe_select(pd.DataFrame(all_structure_activity_data), ['activityName', 'activitySeq', 'formTypeId'])
    external_activity_data = safe_select(pd.DataFrame(all_external_activity_data), ['activityName', 'activitySeq', 'formTypeId'])
    
    st.write("ELIGO TOWER F FINISHING ACTIVITY DATA (activityName, activitySeq, formTypeId)")
    st.write(f"Total records: {len(finishing_activity_data)}")
    st.write(finishing_activity_data)
    st.write("ELIGO STRUCTURE ACTIVITY DATA (activityName, activitySeq, formTypeId) ")
    st.write(f"Total records: {len(structure_activity_data)}")
    st.write(structure_activity_data)
    st.write("ELIGO TOWER G FINISHING ACTIVITY DATA (activityName, activitySeq, formTypeId)")
    st.write(f"Total records: {len(external_activity_data)}")
    st.write(external_activity_data)
    
    return finishing_activity_data, structure_activity_data, external_activity_data
# Fetch Location/Module Data with Async
async def Get_Location():
    record_limit = 1000
    headers = {
        'Cookie': f'ASessionID={st.session_state.sessionid}',
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    all_finishing_location_data = []
    all_structure_location_data = []
    all_external_location_data = []
    

    # Ensure session is valid before starting
    await refresh_session_if_needed()

    async with aiohttp.ClientSession() as session:
        # Fetch ELIGO Tower F Finishing Location/Module data
        start_record = 1
        total_records_fetched = 0
        st.write("Fetching ELIGO Tower F Finishing Location/Module data...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanLocation/?projectId={st.session_state.workspaceid}&planId={st.session_state.Eligo_Tower_F_Finishing}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                await refresh_session_if_needed()
                headers['Cookie'] = f'ASessionID={st.session_state.sessionid}'
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more ELIGO Tower F Finishing Location data available (204)")
                    break
                if isinstance(data, list):
                    location_data = [{'qiLocationId': item.get('qiLocationId', ''), 'qiParentId': item.get('qiParentId', ''), 'name': item.get('name', '')} 
                                   for item in data if isinstance(item, dict)]
                    all_finishing_location_data.extend(location_data)
                    total_records_fetched = len(all_finishing_location_data)
                    st.write(f"Fetched {len(location_data)} Finishing Location records (Total: {total_records_fetched})")
                elif isinstance(data, dict) and 'locationList' in data and data['locationList']:
                    location_data = [{'qiLocationId': loc.get('qiLocationId', ''), 'qiParentId': loc.get('qiParentId', ''), 'name': loc.get('name', '')} 
                                   for loc in data['locationList']]
                    all_finishing_location_data.extend(location_data)
                    total_records_fetched = len(all_finishing_location_data)
                    st.write(f"Fetched {len(location_data)} Finishing Location records (Total: {total_records_fetched})")
                else:
                    st.warning(f"No 'locationList' in Finishing Location data or empty list.")
                    break
                if len(location_data) < record_limit:
                    break
                start_record += record_limit
                await asyncio.sleep(1)  # Rate limiting
            except Exception as e:
                st.error(f" Error fetching Finishing Location data: {str(e)}")
                logger.error(f"Finishing Location fetch failed: {str(e)}")
                break

        # Fetch ELIGO Structure Location/Module data
        start_record = 1
        total_records_fetched = 0
        st.write("Fetching ELIGO Structure Location/Module data...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanLocation/?projectId={st.session_state.workspaceid}&planId={st.session_state.ELIGO_Structure}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                await refresh_session_if_needed()
                headers['Cookie'] = f'ASessionID={st.session_state.sessionid}'
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more Structure Location data available (204)")
                    break
                if isinstance(data, list):
                    location_data = [{'qiLocationId': item.get('qiLocationId', ''), 'qiParentId': item.get('qiParentId', ''), 'name': item.get('name', '')} 
                                   for item in data if isinstance(item, dict)]
                    all_structure_location_data.extend(location_data)
                    total_records_fetched = len(all_structure_location_data)
                    st.write(f"Fetched {len(location_data)} Structure Location records (Total: {total_records_fetched})")
                elif isinstance(data, dict) and 'locationList' in data and data['locationList']:
                    location_data = [{'qiLocationId': loc.get('qiLocationId', ''), 'qiParentId': loc.get('qiParentId', ''), 'name': loc.get('name', '')} 
                                   for loc in data['locationList']]
                    all_structure_location_data.extend(location_data)
                    total_records_fetched = len(all_structure_location_data)
                    st.write(f"Fetched {len(location_data)} Structure Location records (Total: {total_records_fetched})")
                else:
                    st.warning(f"No 'locationList' in Structure Location data or empty list.")
                    break
                if len(location_data) < record_limit:
                    break
                start_record += record_limit
                await asyncio.sleep(1)  # Rate limiting
            except Exception as e:
                st.error(f" Error fetching Structure Location data: {str(e)}")
                logger.error(f"Structure Location fetch failed: {str(e)}")
                break

        # Fetch ELIGO Tower G Finishing Location/Module data
        start_record = 1
        total_records_fetched = 0
        st.write("Fetching ELIGO Tower G Finishing Location/Module data...")
        while True:
            url = f"https://adoddleak.asite.com/commonapi/qaplan/getPlanLocation/?projectId={st.session_state.workspaceid}&planId={st.session_state.Eligo_Tower_G_Finishing}&recordStart={start_record}&recordLimit={record_limit}"
            try:
                await refresh_session_if_needed()
                headers['Cookie'] = f'ASessionID={st.session_state.sessionid}'
                data = await fetch_data(session, url, headers)
                if data is None:
                    st.write("No more ELIGO Tower G Finishing Location data available (204)")
                    break
                if isinstance(data, list):
                    location_data = [{'qiLocationId': item.get('qiLocationId', ''), 'qiParentId': item.get('qiParentId', ''), 'name': item.get('name', '')} 
                                   for item in data if isinstance(item, dict)]
                    all_external_location_data.extend(location_data)
                    total_records_fetched = len(all_external_location_data)
                    st.write(f"Fetched {len(location_data)} ELIGO Tower G Finishing Location records (Total: {total_records_fetched})")
                elif isinstance(data, dict) and 'locationList' in data and data['locationList']:
                    location_data = [{'qiLocationId': loc.get('qiLocationId', ''), 'qiParentId': loc.get('qiParentId', ''), 'name': loc.get('name', '')} 
                                   for loc in data['locationList']]
                    all_external_location_data.extend(location_data)
                    total_records_fetched = len(all_external_location_data)
                    st.write(f"Fetched {len(location_data)} ELIGO Tower G Finishing Location records (Total: {total_records_fetched})")
                else:
                    st.warning(f"No 'locationList' in ELIGO Tower G Finishing Location data or empty list.")
                    break
                if len(location_data) < record_limit:
                    break
                start_record += record_limit
                await asyncio.sleep(1)  # Rate limiting
            except Exception as e:
                st.error(f" Error fetching Tower G Finishing Location data: {str(e)}")
                logger.error(f"Tower G Finishing Location fetch failed: {str(e)}")
                break


    finishing_df = pd.DataFrame(all_finishing_location_data)
    structure_df = pd.DataFrame(all_structure_location_data)
    external_df = pd.DataFrame(all_external_location_data)
    

    # Validate name field
    if 'name' in finishing_df.columns and finishing_df['name'].isna().all():
        st.error(" All 'name' values in Tower F Finishing Location data are missing or empty!")
    if 'name' in structure_df.columns and structure_df['name'].isna().all():
        st.error(" All 'name' values in Structure Location data are missing or empty!")
    if 'name' in external_df.columns and external_df['name'].isna().all():
        st.error(" All 'name' values in Tower G Finishing Location data are missing or empty!")
    
    st.write("ELIGO TOWER F FINISHING LOCATION/MODULE DATA")
    st.write(f"Total records: {len(finishing_df)}")
    st.write(finishing_df)
    st.write("ELIGO STRUCTURE LOCATION/MODULE DATA")
    st.write(f"Total records: {len(structure_df)}")
    st.write(structure_df)
    st.write("ELIGO TOWER G FINISHING LOCATION/MODULE DATA")
    st.write(f"Total records: {len(external_df)}")
    st.write(external_df)
  
    st.session_state.finishing_location_data = finishing_df
    st.session_state.structure_location_data = structure_df
    st.session_state.external_location_data = external_df


    return finishing_df, structure_df, external_df, 

# Process individual chunk
def process_chunk(chunk, chunk_idx, dataset_name, location_df):
    logger.info(f"Starting thread for {dataset_name} Chunk {chunk_idx + 1}")
    generated_text = format_chunk_locally(chunk, chunk_idx, len(chunk), dataset_name, location_df)
    logger.info(f"Completed thread for {dataset_name} Chunk {chunk_idx + 1}")
    return generated_text, chunk_idx

# Process data with manual counting
def process_manually(analysis_df, total, dataset_name, chunk_size=1000, max_workers=4):
    if analysis_df.empty:
        st.warning(f"No completed activities found for {dataset_name}.")
        return {"towers": {}, "total": 0}

    unique_activities = analysis_df['activityName'].unique()
    logger.info(f"Unique activities in {dataset_name} dataset: {list(unique_activities)}")
    logger.info(f"Total records in {dataset_name} dataset: {len(analysis_df)}")

    st.write(f"Saved ELIGO {dataset_name} data to eligo_{dataset_name.lower()}_data.json")
    chunks = [analysis_df[i:i + chunk_size] for i in range(0, len(analysis_df), chunk_size)]

    location_df = (
        st.session_state.finishing_location_data if dataset_name.lower() == "finishing"
        else st.session_state.structure_location_data if dataset_name.lower() == "structure"
        else st.session_state.external_location_data
    )

    chunk_results = {}
    progress_bar = st.progress(0)
    status_text = st.empty()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_chunk = {
            executor.submit(process_chunk, chunk, idx, dataset_name, location_df): idx
            for idx, chunk in enumerate(chunks)
        }

        completed_chunks = 0
        for future in as_completed(future_to_chunk):
            chunk_idx = future_to_chunk[future]
            try:
                generated_text, idx = future.result()
                chunk_results[idx] = generated_text
                completed_chunks += 1
                progress_percent = completed_chunks / len(chunks)
                progress_bar.progress(progress_percent)
                status_text.text(f"Processed chunk {completed_chunks} of {len(chunks)} ({progress_percent:.1%} complete)")
            except Exception as e:
                logger.error(f"Error processing chunk {chunk_idx + 1} for {dataset_name}: {str(e)}")
                st.error(f" Error processing chunk {chunk_idx + 1}: {str(e)}")

    parsed_data = {}
    for chunk_idx in sorted(chunk_results.keys()):
        generated_text = chunk_results[chunk_idx]
        if not generated_text:
            continue

        current_tower = None
        tower_activities = []
        lines = generated_text.split("\n")

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.startswith("Tower:"):
                try:
                    tower_parts = line.split("Tower:", 1)
                    if len(tower_parts) > 1:
                        if current_tower and tower_activities:
                            if current_tower not in parsed_data:
                                parsed_data[current_tower] = []
                            parsed_data[current_tower].extend(tower_activities)
                            tower_activities = []
                        current_tower = tower_parts[1].strip()
                except Exception as e:
                    logger.warning(f"Error parsing Tower line: {line}, error: {str(e)}")
                    if not current_tower:
                        current_tower = f"Unknown Tower {chunk_idx}"

            elif line.startswith("Total Completed Activities:"):
                continue
            elif not line.strip().startswith("activityName"):
                try:
                    parts = re.split(r'\s{2,}', line.strip())
                    if len(parts) >= 2:
                        activity_name = ' '.join(parts[:-1]).strip()
                        count_str = parts[-1].strip()
                        count_match = re.search(r'\d+', count_str)
                        if count_match:
                            count = int(count_match.group())
                            if current_tower:
                                tower_activities.append({
                                    "activityName": activity_name,
                                    "completedCount": count
                                })
                    else:
                        match = re.match(r'^\s*(.+?)\s+(\d+)$', line)
                        if match and current_tower:
                            activity_name = match.group(1).strip()
                            count = int(match.group(2).strip())
                            tower_activities.append({
                                "activityName": activity_name,
                                "completedCount": count
                            })
                except (ValueError, IndexError) as e:
                    logger.warning(f"Skipping malformed activity line: {line}, error: {str(e)}")

        if current_tower and tower_activities:
            if current_tower not in parsed_data:
                parsed_data[current_tower] = []
            parsed_data[current_tower].extend(tower_activities)

    aggregated_data = {}
    for tower_name, activities in parsed_data.items():
        tower_short_name = tower_name.split('/')[1] if '/' in tower_name else tower_name
        if tower_short_name not in aggregated_data:
            aggregated_data[tower_short_name] = {}
        
        for activity in activities:
            name = activity.get("activityName", "Unknown")
            count = activity.get("completedCount", 0)
            if name in aggregated_data[tower_short_name]:
                aggregated_data[tower_short_name][name] += count
            else:
                aggregated_data[tower_short_name][name] = count

    return {"towers": aggregated_data, "total": total}

# Local formatting function for manual counting
def format_chunk_locally(chunk, chunk_idx, chunk_size, dataset_name, location_df):
    towers_data = {}
    
    for _, row in chunk.iterrows():
        tower_name = row['tower_name']
        activity_name = row['activityName']
        count = int(row['CompletedCount'])
        
        if tower_name not in towers_data:
            towers_data[tower_name] = []
            
        towers_data[tower_name].append({
            "activityName": activity_name,
            "completedCount": count
        })
    
    output = ""
    total_activities = 0
    
    for tower_name, activities in sorted(towers_data.items()):
        output += f"Tower: {tower_name}\n"
        output += "activityName            CompletedCount\n"
        activity_dict = {}
        for activity in activities:
            name = activity['activityName']
            count = activity['completedCount']
            activity_dict[name] = activity_dict.get(name, 0) + count
        for name, count in sorted(activity_dict.items()):
            output += f"{name:<30} {count}\n"
            total_activities += count
    
    output += f"Total Completed Activities: {total_activities}"
    return output


def process_data(df, activity_df, location_df, dataset_name, use_module_hierarchy_for_finishing=False):
    # Keep only completed rows
    completed = df[df['statusName'] == 'Completed'].copy()
    
    # Updated strict whitelists for the report
    ALLOWED_MEP = [
        "Plumbing Works",
        "Slab Conducting",
        "Wall Conducting", 
        "Wiring & Switch Socket",
    ]
    
    ALLOWED_FINISHING = [
        "Floor Tile",
        "Wall Tile",
        "POP & Gypsum Plaster",
        "Waterproofing - Sunken",
    ]
    
    ALLOWED_CIVIL = [
        "Concreting",
        "Shuttering", 
        "Reinforcement",
        "De-Shuttering",
    ]

    # initialize count table with the exact order required in the report
    asite_activities = ALLOWED_MEP + ALLOWED_FINISHING + ALLOWED_CIVIL
    count_table = pd.DataFrame({'Count': [0] * len(asite_activities)}, index=asite_activities)

    if completed.empty:
        logger.warning(f"No completed activities found in {dataset_name} data.")
        return pd.DataFrame(), 0, count_table

    # Merge to bring names for locations and activities
    completed = completed.merge(location_df[['qiLocationId', 'name']], on='qiLocationId', how='left')
    completed = completed.merge(activity_df[['activitySeq', 'activityName']], on='activitySeq', how='left')
    
    if 'qiActivityId' not in completed.columns:
        completed['qiActivityId'] = completed['qiLocationId'].astype(str) + '$$' + completed['activitySeq'].astype(str)
    
    completed['name'] = completed['name'].fillna('Unknown')

    # Normalize activity names so filters match
    def normalize_activity_name(name):
        if not isinstance(name, str):
            return name
        typo_corrections = {
            "Wall Conduting": "Wall Conducting",
            "Slab conduting": "Slab Conducting",
            "Wiring and Switch Socket": "Wiring & Switch Socket",
            "Pop & Gypsum Plaster": "POP & Gypsum Plaster",
            "WallTile": "Wall Tile",
            "FloorTile": "Floor Tile",
            "wall tile": "Wall Tile",
            "floor tile": "Floor Tile",
            "Concreting": "Concreting",
        }
        for typo, correct in typo_corrections.items():
            if isinstance(name, str) and name.lower() == typo.lower():
                return correct
        return name

    completed['activityName'] = completed['activityName'].apply(normalize_activity_name).fillna('Unknown')

    # Build location path dictionaries
    parent_child_dict = dict(zip(location_df['qiLocationId'], location_df['qiParentId']))
    name_dict = dict(zip(location_df['qiLocationId'], location_df['name']))

    def get_full_path(location_id):
        path = []
        current_id = location_id
        max_depth = 10
        depth = 0
        
        while current_id and depth < max_depth:
            if current_id not in parent_child_dict or current_id not in name_dict:
                break
            parent_id = parent_child_dict.get(current_id)
            nm = name_dict.get(current_id, "Unknown")
            
            if not parent_id:
                if nm != "Quality":
                    path.append(nm)
                path.append("Quality")
            else:
                path.append(nm)
            current_id = parent_id
            depth += 1
        
        if not path:
            return "Unknown"
        return '/'.join(reversed(path))

    completed['full_path'] = completed['qiLocationId'].apply(get_full_path)

    # **CORRECTED FILTERING FUNCTIONS**
    # Function to check for "roof slab/" pattern (without space) for MEP activities
    def contains_roof_slab_no_space_mep(full_path):
        """
        Check if path contains 'roof slab/' (without space between slab and slash)
        Case-insensitive matching for MEP activities across all towers TF, TG, TH
        """
        if not isinstance(full_path, str):
            return False
        result = 'roof slab/' in full_path.lower()
        if result:
            logger.info(f"MEP: Found 'roof slab/' in path: {full_path}")
        return result

    # Function to check for "roof slab/" pattern (without space) for Interior Finishing activities  
    def contains_roof_slab_no_space_finishing(full_path):
        """
        Check if path contains 'roof slab/' (without space between slab and slash)
        Case-insensitive matching for Interior Finishing activities across all towers TF, TG, TH
        """
        if not isinstance(full_path, str):
            return False
        result = 'roof slab/' in full_path.lower()
        if result:
            logger.info(f"Interior Finishing: Found 'roof slab/' in path: {full_path}")
        return result

    # Function for Civil Works roof slab filter (existing logic - unchanged)
    def contains_roof_slab(full_path):
        """
        Check if path contains 'roof slab' followed by optional spaces and a slash
        Handles both 'roof slab/' and 'roof slab /' patterns (case-insensitive)
        This is used for Civil Works activities
        """
        if not isinstance(full_path, str):
            return False
        # Match 'roof slab' followed by optional whitespace and slash
        return bool(re.search(r'roof\s+slab\s*/', full_path, re.IGNORECASE))

    # Stilt roof slab filter (for exclusion in Civil Works only)
    def contains_stilt_roof_slab(full_path):
        if isinstance(full_path, str):
            return 'stilt roof slab' in full_path.lower()
        return False

    # Identify activity categories  
    mep_activities = set(ALLOWED_MEP)
    finishing_activities = set(ALLOWED_FINISHING)
    civil_activities = set(ALLOWED_CIVIL)

    # Debug: Count activities before filtering
    logger.info(f"Dataset: {dataset_name}")
    logger.info(f"Total completed activities before filtering: {len(completed)}")
    
    # Count activities by type
    mep_count_before = len(completed[completed['activityName'].isin(mep_activities)])
    finishing_count_before = len(completed[completed['activityName'].isin(finishing_activities)])
    civil_count_before = len(completed[completed['activityName'].isin(civil_activities)])
    
    logger.info(f"MEP activities before filtering: {mep_count_before}")
    logger.info(f"Interior Finishing activities before filtering: {finishing_count_before}")
    logger.info(f"Civil activities before filtering: {civil_count_before}")

    # **CORRECTED FILTERING LOGIC - Apply filtering based on activity type with SEPARATE LOGIC**
    filtered_rows = []
    for idx, row in completed.iterrows():
        activity_name = row['activityName']
        full_path = row['full_path']
        
        # **SEPARATE FILTERING FOR MEP ACTIVITIES - FIXED FOR ALL TOWERS INCLUDING TOWER G**
        if activity_name in mep_activities:
            if contains_roof_slab_no_space_mep(full_path):
                filtered_rows.append(idx)
                logger.debug(f"MEP Activity '{activity_name}' INCLUDED from path: {full_path}")
            else:
                logger.debug(f"MEP Activity '{activity_name}' EXCLUDED from path: {full_path}")
        
        # **SEPARATE FILTERING FOR INTERIOR FINISHING ACTIVITIES**
        elif activity_name in finishing_activities:
            if contains_roof_slab_no_space_finishing(full_path):
                filtered_rows.append(idx)
                logger.debug(f"Interior Finishing Activity '{activity_name}' INCLUDED from path: {full_path}")
            else:
                logger.debug(f"Interior Finishing Activity '{activity_name}' EXCLUDED from path: {full_path}")
        
        # **CIVIL WORKS: keep existing complex logic (no change)**
        elif activity_name in civil_activities:
            if dataset_name.lower() == 'structure':
                # For structure dataset: use roof slab filter but exclude stilt
                if 'roof slab' in full_path.lower() and 'stilt roof slab' not in full_path.lower():
                    filtered_rows.append(idx)
            else:
                # For finishing datasets: use flat number logic but exclude stilt
                parts = full_path.split('/')
                if len(parts) > 0:
                    last_part = parts[-1]
                    has_flat_number = bool(re.match(r'^\d+(?:(?:\s*\(LL\))|(?:\s*\(UL\))|(?:\s*LL)|(?:\s*UL))?$', last_part))
                    if has_flat_number and 'stilt roof slab' not in full_path.lower():
                        filtered_rows.append(idx)

    # Filter the dataframe to only include rows that passed the filters
    completed = completed.loc[filtered_rows].copy()
    
    # Debug: Count activities after filtering
    mep_count_after = len(completed[completed['activityName'].isin(mep_activities)])
    finishing_count_after = len(completed[completed['activityName'].isin(finishing_activities)])
    civil_count_after = len(completed[completed['activityName'].isin(civil_activities)])
    
    logger.info(f"MEP activities after filtering: {mep_count_after} (reduced by {mep_count_before - mep_count_after})")
    logger.info(f"Interior Finishing activities after filtering: {finishing_count_after} (reduced by {finishing_count_before - finishing_count_after})")
    logger.info(f"Civil activities after filtering: {civil_count_after} (reduced by {civil_count_before - civil_count_after})")

    if completed.empty:
        logger.warning(f"No completed activities found in {dataset_name} data after filtering.")
        return pd.DataFrame(), 0, count_table

    # Debug: Show sample paths for each activity type
    for activity_type, activities in [("MEP", mep_activities), ("Interior Finishing", finishing_activities)]:
        sample_data = completed[completed['activityName'].isin(activities)]
        if not sample_data.empty:
            logger.info(f"\nSample paths for {activity_type} activities:")
            for activity in activities:
                activity_data = sample_data[sample_data['activityName'] == activity]
                if not activity_data.empty:
                    sample_paths = activity_data['full_path'].unique()[:3]  # Show up to 3 sample paths
                    logger.info(f"  {activity}: {len(activity_data)} records")
                    for path in sample_paths:
                        logger.info(f"    Sample path: {path}")

    # Map tower name with Tower 4 split rule
    def get_tower_name(full_path):
        parts = full_path.split('/')
        if len(parts) < 2:
            return full_path
        tower = parts[1]
        if tower == "Tower 4" and len(parts) > 2:
            module = parts[2]
            module_number = module.replace("Module ", "").strip()
            try:
                module_num = int(module_number)
                if 1 <= module_num <= 4:
                    return "Tower 4(B)"
                elif 5 <= module_num <= 8:
                    return "Tower 4(A)"
            except ValueError:
                pass
        return tower

    completed['tower_name'] = completed['full_path'].apply(get_tower_name)

    # ENFORCE the allowed activities only
    allowed_all = set(ALLOWED_MEP + ALLOWED_FINISHING + ALLOWED_CIVIL)
    completed = completed[completed['activityName'].isin(allowed_all)]

    if completed.empty:
        logger.warning(f"No completed activities after applying allowed list in {dataset_name}.")
        return pd.DataFrame(), 0, count_table

    # Group and count unique locations per tower/activity
    analysis = (
        completed.groupby(['tower_name', 'activityName'])['qiLocationId']
        .nunique()
        .reset_index(name='CompletedCount')
        .sort_values(by=['tower_name', 'activityName'], ascending=True)
    )
    
    total_completed = analysis['CompletedCount'].sum()

    # Fill count_table from filtered data only
    activity_counts = (
        completed.groupby('activityName')['qiLocationId']
        .nunique()
        .reset_index(name='Count')
    )
    
    for activity in asite_activities:
        if activity in activity_counts['activityName'].values:
            count_table.loc[activity, 'Count'] = activity_counts.loc[
                activity_counts['activityName'] == activity, 'Count'
            ].iloc[0]

    logger.info(f"Total completed activities for {dataset_name}: {total_completed}")
    logger.info(f"Count table for {dataset_name}:\n{count_table.to_string()}")
    
    return analysis, total_completed, count_table




# Main analysis function


def AnalyzeStatusManually(email=None, password=None):
    start_time = time.time()

    if 'sessionid' not in st.session_state:
        st.error(" Please log in first!")
        return

    required_data = [
        'eligo_tower_f_finishing', 'eligo_structure', 'eligo_tower_g_finishing',
        'finishing_activity_data', 'structure_activity_data', 'external_activity_data',
        'finishing_location_data', 'structure_location_data', 'external_location_data',
        
    ]
    
    for data_key in required_data:
        if data_key not in st.session_state:
            st.error(f" Please fetch required data first! Missing: {data_key}")
            return

    try:
        finishing_data = st.session_state.eligo_tower_f_finishing
        structure_data = st.session_state.eligo_structure
        external_data = st.session_state.eligo_tower_g_finishing
       

        finishing_activity = st.session_state.finishing_activity_data
        structure_activity = st.session_state.structure_activity_data
        external_activity = st.session_state.external_activity_data
        

        finishing_locations = st.session_state.finishing_location_data
        structure_locations = st.session_state.structure_location_data
        external_locations = st.session_state.external_location_data
        
    except KeyError as e:
        st.error(f" Missing session state data: {str(e)}")
        return
    except Exception as e:
        st.error(f" Error retrieving session state data: {str(e)}")
        return

    main_datasets = [
        (finishing_data, "Tower F Finishing"),
        (structure_data, "ELIGO Structure"),
        (external_data, "Tower G Finishing"),
    ]

    for df, name in main_datasets:
        if not isinstance(df, pd.DataFrame):
            st.error(f" {name} data is not a DataFrame! Type: {type(df)}")
            st.write(f"Content preview: {str(df)[:200]}...")
            return
            
        required_columns = ['statusName', 'qiLocationId', 'activitySeq']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            st.error(f" Missing columns in {name} data: {missing_columns}")
            st.write(f"Available columns: {list(df.columns)}")
            return
    
    location_datasets = [
        (finishing_locations, "Tower F Finishing Location"),
        (structure_locations, "ELIGO Structure Location"),
        (external_locations, "Tower G Finishing Location")
    ]

    for df, name in location_datasets:
        if not isinstance(df, pd.DataFrame):
            st.error(f" {name} data is not a DataFrame! Type: {type(df)}")
            return
            
        required_columns = ['qiLocationId', 'name']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            st.error(f" Missing columns in {name} data: {missing_columns}")
            st.write(f"Available columns: {list(df.columns)}")
            return

    activity_datasets = [
        (finishing_activity, "Tower F Finishing Activity"),
        (structure_activity, "ELIGO Structure Activity"),
        (external_activity, "Tower G Finishing Activity"),
    ]

    for df, name in activity_datasets:
        if not isinstance(df, pd.DataFrame):
            st.error(f" {name} data is not a DataFrame! Type: {type(df)}")
            return
            
        required_columns = ['activitySeq', 'activityName']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            st.error(f" Missing columns in {name} data: {missing_columns}")
            st.write(f"Available columns: {list(df.columns)}")
            return

    def normalize_activity_name(name):
        if not isinstance(name, str):
            return name
        typo_corrections = {
            "Wall Conduting": "Wall Conducting",
            "Slab conduting": "Slab Conducting",
        }
        for typo, correct in typo_corrections.items():
            if name.lower() == typo.lower():
                return correct
        return name

    for df in [finishing_activity, structure_activity, external_activity]:
        df['activityName'] = df['activityName'].apply(normalize_activity_name)

    # Fetch and store COS slab cycle data
    st.write("Fetching COS slab cycle data...")

    # Check if required session state keys exist
    if not all(key in st.session_state for key in ['cos_client', 'bucket_name']):
        st.error("COS client not initialized. Please run 'Initialize and Fetch Data' first.")
        st.session_state['slab_df'] = pd.DataFrame()
    else:
        try:
            # Get the COS client and bucket from session state
            cos_client = st.session_state['cos_client']
            bucket_name = st.session_state['bucket_name']
            
            # Initialize file_list if it doesn't exist or is None
            if 'file_list' not in st.session_state or st.session_state['file_list'] is None:
                st.write("File list not found in session state. Fetching files from COS...")
                try:
                    response = cos_client.list_objects_v2(Bucket=bucket_name, Prefix="Eligo/")
                    if 'Contents' in response:
                        st.session_state['file_list'] = [{'Key': obj['Key']} for obj in response['Contents']]
                    else:
                        st.session_state['file_list'] = []
                        st.warning("No files found in Eligo folder.")
                except Exception as e:
                    st.session_state['file_list'] = []
            
            file_list = st.session_state['file_list']
            
            # Ensure file_list is a list before proceeding
            if not isinstance(file_list, list):
                st.error(f"file_list is not a list. Type: {type(file_list)}")
                st.session_state['slab_df'] = pd.DataFrame()
            else:
                # Call GetSlabReport function if it exists
                try:
                    GetSlabReport()  # Make sure this function is defined and works properly
                    #
                except NameError:
                    st.warning("GetSlabReport function not found. Skipping slab report generation.")
                except Exception as e:
                    st.warning(f"Error calling GetSlabReport: {str(e)}")
                
                # Find slab cycle files
                struct_files = [file for file in file_list if isinstance(file, dict) and 'Key' in file and 'Anti. Slab Cycle' in file['Key']]
            
        except Exception as e:
            st.error(f" Error fetching COS slab cycle data: {str(e)}")
            logging.error(f"Error fetching COS slab cycle data: {str(e)}")
            st.session_state['slab_df'] = pd.DataFrame()

    asite_data = []
    outputs = {}
    for dataset_name, data, activity, location in [
        ("Tower F Finishing", finishing_data, finishing_activity, finishing_locations),
        ("Structure", structure_data, structure_activity, structure_locations),
        ("Tower G Finishing", external_data, external_activity, external_locations),
        
    ]:
        try:
            analysis, total, count_table = process_data(data, activity, location, dataset_name)
            if analysis.empty and total == 0:
                logger.warning(f"No valid data processed for {dataset_name}. Skipping analysis.")
                st.warning(f"No completed activities found for {dataset_name}.")
                outputs[dataset_name] = {"towers": {}, "total": 0}
                continue
            output = process_manually(analysis, total, dataset_name)
            outputs[dataset_name] = output
            for tower, activities in output["towers"].items():
                for activity_name, count in activities.items():
                    normalized_name = normalize_activity_name(activity_name)
                    asite_data.append({
                        "Dataset": dataset_name,
                        "Tower": tower,
                        "Activity Name": normalized_name,
                        "Count": count
                    })
        except Exception as e:
            logger.error(f"Error processing {dataset_name} data: {str(e)}")
            st.error(f" Error processing {dataset_name} data: {str(e)}")
            outputs[dataset_name] = {"towers": {}, "total": 0}
            continue

    asite_df = pd.DataFrame(asite_data)

    for dataset_name in ["Tower F Finishing", "Structure", "Tower G Finishing"]:
        output = outputs.get(dataset_name, {"towers": {}, "total": 0})
        st.write(f"### ELIGO {dataset_name} Quality Analysis (Completed Activities):")
        if not output["towers"]:
            st.write("No completed activities found.")
            continue
        for tower, activities in output["towers"].items():
            st.write(f"{tower} activityName            CompletedCount")
            for name, count in sorted(activities.items()):
                st.write(f"{'':<11} {name:<23} {count:>14}")
            st.write(f"{'':<11} Total for {tower:<11}: {sum(activities.values()):>14}")
        st.write(f"Total Completed Activities: {output['total']}")

    # Fixed COS data processing section - replace the existing COS data processing code

    cos_data = []
    first_fix_counts = {}

    # Define COS datasets with proper session state key mapping
    cos_datasets = [
        ('cos_tname_eligo_tower_h_finishing', 'cos_df_eligo_tower_h_finishing', 'Tower H'),
        ('cos_tname_eligo_tower_g_finishing', 'cos_df_eligo_tower_g_finishing', 'Tower G'), 
        ('cos_tname_eligo_structure', 'cos_df_eligo_structure', 'Structure')
    ]

    # Debug: Check what's available in session state
    st.write("### Debug: Available COS session state keys:")
    cos_keys = [key for key in st.session_state.keys() if 'cos_' in key.lower()]
    st.write(f"Found COS keys: {cos_keys}")

    for tname_key, tdata_key, default_tower_name in cos_datasets:
        st.write(f"Processing: {tname_key} -> {tdata_key}")
        
        # Check if keys exist in session state
        if tname_key in st.session_state and tdata_key in st.session_state:
            tname = st.session_state.get(tname_key, default_tower_name)
            tower_data = st.session_state[tdata_key]
            
            st.write(f"Found data for {tname}: {type(tower_data)}")
            
            if tower_data is not None and isinstance(tower_data, pd.DataFrame) and not tower_data.empty:
                st.write(f"DataFrame shape: {tower_data.shape}")
                st.write(f"Columns: {list(tower_data.columns)}")
                
                # Create a copy and process dates
                tower_data = tower_data.copy()
                
                # Check for different possible column names for finish date
                finish_date_columns = ['Actual Finish', 'ActualFinish', 'Finish Date', 'Actual_Finish']
                finish_col = None
                for col in finish_date_columns:
                    if col in tower_data.columns:
                        finish_col = col
                        break
                
                if finish_col:
                    tower_data[finish_col] = pd.to_datetime(tower_data[finish_col], errors='coerce')
                    tower_data_filtered = tower_data[~pd.isna(tower_data[finish_col])].copy()
                    st.write(f"Filtered data shape: {tower_data_filtered.shape}")
                else:
                    st.warning(f"No finish date column found in {tname}. Using all data.")
                    tower_data_filtered = tower_data.copy()
                
                # Check for different possible column names for activity
                activity_columns = ['Activity Name', 'ActivityName', 'Activity', 'activityName']
                activity_col = None
                for col in activity_columns:
                    if col in tower_data_filtered.columns:
                        activity_col = col
                        break
                
                if activity_col:
                    st.write(f"Using activity column: {activity_col}")
                    st.write(f"Unique activities: {tower_data_filtered[activity_col].unique()[:10]}")  # Show first 10
                    
                    first_fix_counts[tname] = {}
                    
                    # Define activities to search for
                    target_activities = [
                        "EL-First Fix", "UP-First Fix", "CP-First Fix", "C-Gypsum and POP Punning",
                        "EL-Second Fix", "No. of Slab cast", "Electrical", "Installation of doors",
                        "Waterproofing Works", "Wall Tiling", "Floor Tiling", "Sewer Line",
                        "Storm Line", "GSB", "WMM", "Stamp Concrete", "Saucer drain", "Kerb Stone"
                    ]
                    
                    for activity in target_activities:
                        # Try exact match first
                        count = len(tower_data_filtered[tower_data_filtered[activity_col] == activity])
                        
                        # If no exact match, try partial match (case insensitive)
                        if count == 0:
                            partial_match = tower_data_filtered[
                                tower_data_filtered[activity_col].str.contains(activity, case=False, na=False)
                            ]
                            count = len(partial_match)
                            if count > 0:
                                st.write(f"Found {count} partial matches for '{activity}' in {tname}")
                        
                        cos_data.append({
                            "Tower": tname,
                            "Activity Name": activity,
                            "Count": count
                        })
                        
                        # Store first fix counts for combination calculation
                        if activity in ["UP-First Fix", "CP-First Fix"]:
                            first_fix_counts[tname][activity] = count
                            
                else:
                    st.error(f"No activity column found in {tname}. Available columns: {list(tower_data_filtered.columns)}")
            else:
                st.warning(f"No valid data found for {tname}")
                # Add empty entries to maintain structure
                for activity in [
                    "EL-First Fix", "UP-First Fix", "CP-First Fix", "C-Gypsum and POP Punning",
                    "EL-Second Fix", "No. of Slab cast", "Electrical", "Installation of doors",
                    "Waterproofing Works", "Wall Tiling", "Floor Tiling", "Sewer Line",
                    "Storm Line", "GSB", "WMM", "Stamp Concrete", "Saucer drain", "Kerb Stone"
                ]:
                    cos_data.append({
                        "Tower": default_tower_name,
                        "Activity Name": activity,
                        "Count": 0
                    })
        else:
            st.warning(f"Session state keys not found: {tname_key}, {tdata_key}")
            # Add empty entries for missing towers
            for activity in [
                "EL-First Fix", "UP-First Fix", "CP-First Fix", "C-Gypsum and POP Punning",
                "EL-Second Fix", "No. of Slab cast", "Electrical", "Installation of doors",
                "Waterproofing Works", "Wall Tiling", "Floor Tiling", "Sewer Line",
                "Storm Line", "GSB", "WMM", "Stamp Concrete", "Saucer drain", "Kerb Stone"
            ]:
                cos_data.append({
                    "Tower": default_tower_name,
                    "Activity Name": activity,
                    "Count": 0
                })

    # Add combined first fix counts
    for tname in first_fix_counts:
        up_count = first_fix_counts[tname].get("UP-First Fix", 0)
        cp_count = first_fix_counts[tname].get("CP-First Fix", 0)
        combined_count = up_count + cp_count
        cos_data.append({
            "Tower": tname,
            "Activity Name": "Min. count of UP-First Fix and CP-First Fix",
            "Count": combined_count
        })

    # Create the COS DataFrame
    cos_df = pd.DataFrame(cos_data)

    # Debug output
    st.write("### COS Data Debug:")
    st.write(f"Total COS records created: {len(cos_data)}")
    st.write("Sample COS data:")
    if not cos_df.empty:
        st.write(cos_df.head(10))
        st.write(f"COS DataFrame shape: {cos_df.shape}")
        st.write(f"Total activities with counts > 0: {len(cos_df[cos_df['Count'] > 0])}")
    else:
        st.write("COS DataFrame is empty!")

    # Log the dataframes
    logger.info(f"Asite DataFrame:\n{asite_df.to_string()}")
    logger.info(f"COS DataFrame:\n{cos_df.to_string()}")

    st.write("### Asite DataFrame (Debug):")
    st.write(asite_df)
    st.write("### COS DataFrame (Debug):")
    st.write(cos_df)
    
    combined_data = {
        "COS": cos_df,
        "Asite": asite_df
    }

    with st.spinner("Categorizing activities with WatsonX..."):
        ai_response = generatePrompt(combined_data, st.session_state.slabreport)
        st.session_state.ai_response = ai_response

    st.write("### Categorized Activity Counts (COS and Asite):")
    try:
        ai_data = json.loads(ai_response)
        st.json(ai_data)
    except json.JSONDecodeError as e:
        st.error(f"Failed to parse AI response as JSON: {str(e)}")
        st.write("Raw AI response:")
        st.text(ai_response)

    end_time = time.time()
    st.write(f"Total execution time: {end_time - start_time:.2f} seconds")
    
# COS File Fetching Function
def get_cos_files():
    try:
        cos_client = initialize_cos_client()
        if not cos_client:
            st.error(" Failed to initialize COS client.")
            return []

        response = cos_client.list_objects_v2(Bucket=COS_BUCKET, Prefix="Eligo/")
        if 'Contents' not in response:
            st.error(f" No files found in the 'Eligo' folder of bucket '{COS_BUCKET}'. Please ensure the folder exists and contains files.")
            logger.error("No objects found in Eligo folder")
            return []

        all_files = [obj['Key'] for obj in response.get('Contents', [])]
        st.write("**All files in Eligo folder:**")
        if all_files:
            st.write("\n".join(all_files))
        else:
            st.write("No files found.")
            logger.warning("Eligo folder is empty")
            return []

        # Pattern for Finishing Tracker files (Tower G and Tower H)
        finishing_pattern = re.compile(
            r"Eligo/Tower\s*([G|H])\s*Finishing\s*Tracker[\(\s]*(.*?)(?:[\)\s]*\.xlsx)$",
            re.IGNORECASE
        )
        # Pattern for Structure Work Tracker file
        structure_pattern = re.compile(
            r"Eligo/Structure\s*Work\s*Tracker[\(\s]*(.*?)(?:[\)\s]*\.xlsx)$",
            re.IGNORECASE
        )

        date_formats = [
            "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y"
        ]

        file_info = []
        for obj in response.get('Contents', []):
            key = obj['Key']
            # Check for Finishing Tracker files (Tower G and Tower H)
            finishing_match = finishing_pattern.match(key)
            if finishing_match:
                tower = finishing_match.group(1)
                date_str = finishing_match.group(2).strip('()').strip()
                parsed_date = None
                
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        break
                    except ValueError:
                        continue
                
                if parsed_date:
                    file_info.append({
                        'key': key,
                        'tower': tower,
                        'date': parsed_date,
                        'type': 'finishing'
                    })
                else:
                    logger.warning(f"Could not parse date in filename: {key} (date: {date_str})")
                    st.warning(f"Skipping file with unparseable date: {key}")
            # Check for Structure Work Tracker file
            else:
                structure_match = structure_pattern.match(key)
                if structure_match:
                    date_str = structure_match.group(1).strip('()').strip()
                    parsed_date = None
                    
                    for fmt in date_formats:
                        try:
                            parsed_date = datetime.strptime(date_str, fmt)
                            break
                        except ValueError:
                            continue
                    
                    if parsed_date:
                        file_info.append({
                            'key': key,
                            'tower': None,  # No specific tower associated
                            'date': parsed_date,
                            'type': 'structure'
                        })
                    else:
                        logger.warning(f"Could not parse date in filename: {key} (date: {date_str})")
                        st.warning(f"Skipping file with unparseable date: {key}")

        if not file_info:
            st.error(" No Excel files matched the expected patterns in the 'Eligo' folder. Expected formats: 'Tower G/H Finishing Tracker(date).xlsx' or 'Structure Work Tracker(date).xlsx'.")
            logger.error("No files matched the expected patterns")
            return []

        # Separate Finishing and Structure files
        finishing_files = {}
        structure_files = []
        for info in file_info:
            if info['type'] == 'finishing':
                tower = info['tower']
                if tower not in finishing_files or info['date'] > finishing_files[tower]['date']:
                    finishing_files[tower] = info
            elif info['type'] == 'structure':
                structure_files.append(info)

        # Select the latest Structure Work Tracker file (if multiple exist)
        if structure_files:
            latest_structure_file = max(structure_files, key=lambda x: x['date'])
            files = [info['key'] for info in finishing_files.values()] + [latest_structure_file['key']]
        else:
            files = [info['key'] for info in finishing_files.values()]

        if not files:
            st.error(" No valid Excel files found for Tower G, Tower H, or Structure Work Tracker after filtering.")
            logger.error("No valid files after filtering")
            return []

        st.success(f"Found {len(files)} matching files: {', '.join(files)}")
        return files
    except Exception as e:
        st.error(f" Error fetching COS files: {str(e)}")
        logger.error(f"Error fetching COS files: {str(e)}")
        return []

# Initialize session state variables
if 'cos_df_eligo_tower_f_finishing' not in st.session_state:
    st.session_state.cos_df_eligo_tower_f_finishing = None
if 'cos_df_eligo_structure' not in st.session_state:
    st.session_state.cos_df_eligo_structure = None
if 'cos_df_eligo_tower_g_finishing' not in st.session_state:
    st.session_state.cos_df_eligo_tower_g_finishing = None
if 'cos_tname_eligo_tower_f_finishing' not in st.session_state:
    st.session_state.cos_tname_eligo_tower_f_finishing = None
if 'cos_tname_eligo_structure' not in st.session_state:
    st.session_state.cos_tname_eligo_structure = None
if 'cos_tname_eligo_tower_g_finishing' not in st.session_state:
    st.session_state.cos_tname_eligo_tower_g_finishing = None

# ADD THESE MISSING INITIALIZATIONS:
if 'cos_client' not in st.session_state:
    st.session_state.cos_client = None
if 'bucket_name' not in st.session_state:
    st.session_state.bucket_name = None
if 'file_list' not in st.session_state:
    st.session_state.file_list = None
if 'slabreport' not in st.session_state:
    st.session_state.slabreport = pd.DataFrame()
if 'slab_df' not in st.session_state:
    st.session_state.slab_df = pd.DataFrame()



if 'ignore_month' not in st.session_state:
    st.session_state.ignore_month = False  
if 'ignore_year' not in st.session_state:
    st.session_state.ignore_year = False 



def process_file(file_stream, filename):
    try:
        workbook = openpyxl.load_workbook(file_stream)
        available_sheets = workbook.sheetnames
        logger.info(f"Available sheets in {filename}: {available_sheets}")

        # Check if the file is a Structure Work Tracker file
        is_structure_tracker = "Structure Work Tracker" in filename

        if is_structure_tracker:
            # Handle Structure Work Tracker file
            possible_sheet_names = [
                "Revised Baselines- 25 days SC", "Revised Baselines", "Baselines",
                "Structure Work", "Structure", "RevisedBaselines"
            ]
            sheet_name = None
            for name in possible_sheet_names:
                if name in available_sheets:
                    sheet_name = name
                    break
            if not sheet_name:
                # Fallback to the first sheet if no expected name is found
                sheet_name = available_sheets[0] if available_sheets else None
            if not sheet_name:
                st.error(f"No valid sheets found in {filename}. Available sheets: {', '.join(available_sheets)}")
                logger.error(f"No valid sheets found in {filename}")
                return [(None, None)]

            file_stream.seek(0)
            try:
                # Try different header rows (0, 1, 2) to find valid columns
                df = None
                actual_columns = None
                for header_row in [0, 1, 2]:
                    try:
                        file_stream.seek(0)
                        df = pd.read_excel(file_stream, sheet_name=sheet_name, header=header_row)
                        actual_columns = df.columns.tolist()
                        # Check if columns are all "Unnamed"
                        if all(col.startswith("Unnamed:") for col in actual_columns):
                            continue
                        logger.info(f"Columns in {sheet_name} (header={header_row}): {actual_columns}")
                        st.write(f"Columns in {sheet_name} (header row {header_row + 1}): {actual_columns}")
                        st.write(f"First 5 rows of {sheet_name} (header row {header_row + 1}):")
                        st.write(df.head(5))
                        break
                    except Exception as e:
                        logger.warning(f"Failed to read {sheet_name} with header row {header_row}: {str(e)}")
                        continue

                if df is None or actual_columns is None:
                    st.error(f"Could not find valid headers in {sheet_name}. All attempts yielded unnamed columns or errors.")
                    logger.error(f"No valid headers found in {sheet_name}")
                    return [(None, None)]

                # Define possible column names for mapping (more flexible for Structure Work Tracker)
                column_mapping = {
                    'Activity ID': ['Activity ID', 'Task ID', 'ID', 'ActivityID'],
                    'Activity Name': ['Activity Name', 'Task Name', 'Activity', 'Name', 'Task', 'Description'],
                    'Actual Finish': ['Actual Finish', 'Finish Date', 'Completion Date', 'Actual End', 'End Date', 'Finish']
                }

                # Find matching columns
                target_columns = ['Activity ID', 'Activity Name', 'Actual Finish']
                selected_columns = {}
                for target in target_columns:
                    for possible_name in column_mapping[target]:
                        if possible_name in actual_columns:
                            selected_columns[target] = possible_name
                            break

                # For Structure Work Tracker, proceed even if some columns are missing
                if not selected_columns:
                    logger.warning(f"No recognizable columns in {sheet_name}. Using all available columns.")
                    selected_columns = {col: col for col in actual_columns[:3]}  # Use first 3 columns as a fallback
                elif len(selected_columns) < len(target_columns):
                    missing_cols = [col for col in target_columns if col not in selected_columns]
                    st.warning(f"Some expected columns missing in {sheet_name}. Missing: {missing_cols}, Found: {list(selected_columns.keys())}")
                    logger.warning(f"Missing columns in {sheet_name}: {missing_cols}")

                # Select and rename columns
                df = df[list(selected_columns.values())]
                df.columns = list(selected_columns.keys())
                
                # For Structure Work Tracker, don't enforce Activity Name requirement
                if 'Activity Name' in df.columns:
                    df = df.dropna(subset=['Activity Name'])
                    df['Activity Name'] = df['Activity Name'].astype(str).str.strip()
                else:
                    st.warning(f"No 'Activity Name' column in {sheet_name}. Proceeding with available data.")
                    df = df.dropna(how='all')  # Drop rows where all values are NaN

                if 'Actual Finish' in df.columns:
                    df['Actual_Finish_Original'] = df['Actual Finish'].astype(str)
                    df['Actual Finish'] = pd.to_datetime(df['Actual Finish'], errors='coerce')
                    has_na_mask = (
                        pd.isna(df['Actual Finish']) |
                        (df['Actual_Finish_Original'].str.upper() == 'NAT') |
                        (df['Actual_Finish_Original'].str.lower().isin(['nan', 'na', 'n/a', 'none', '']))
                    )
                    # Use a fallback column for NA check if Activity Name is missing
                    display_col = 'Activity Name' if 'Activity Name' in df.columns else df.columns[0]
                    na_rows = df[has_na_mask][[display_col, 'Actual Finish']]
                    if not na_rows.empty:
                        st.write(f"Sample of rows with NA or invalid values in Actual Finish for {filename}:")
                        st.write(na_rows.head(10))
                        na_activities = na_rows.groupby(display_col).size().reset_index(name='Count')
                        st.write(f"Items with NA or invalid Actual Finish values in {filename}:")
                        st.write(na_activities)
                    else:
                        st.write(f"No NA or invalid values found in Actual Finish for {filename}")
                    df.drop('Actual_Finish_Original', axis=1, inplace=True)

                # Display unique values based on available columns
                display_col = 'Activity Name' if 'Activity Name' in df.columns else df.columns[0]
                st.write(f"Unique {display_col} in {sheet_name} ({filename}):")
                unique_activities = df[[display_col]].drop_duplicates()
                st.write(unique_activities)

                return [(df, "Structure Work Tracker")]
            except Exception as e:
                st.error(f"Error processing sheet {sheet_name} in {filename}: {str(e)}")
                logger.error(f"Error processing sheet {sheet_name} in {filename}: {str(e)}")
                return [(None, None)]
        else:
            # Handle Tower G or Tower H Finishing Tracker files
            tower_letter = None
            if "Tower G" in filename or "TowerG" in filename:
                tower_letter = "G"
            elif "Tower H" in filename or "TowerH" in filename:
                tower_letter = "H"

            if not tower_letter:
                st.error(f"Cannot determine tower letter from filename: {filename}")
                logger.error(f"Cannot determine tower letter from filename: {filename}")
                return [(None, None)]

            possible_sheet_names = [
                f"Tower {tower_letter} Finishing",
                f"TOWER {tower_letter} FINISHING",
                f"TOWER{tower_letter}FINISHING",
                f"TOWER {tower_letter}FINISHING",
                f"TOWER{tower_letter} FINISHING",
                f"Tower{tower_letter}Finishing",
                f"Finish"
            ]

            sheet_name = None
            for name in possible_sheet_names:
                if name in available_sheets:
                    sheet_name = name
                    break

            if not sheet_name:
                for available in available_sheets:
                    if f"TOWER {tower_letter}" in available.upper() and "FINISH" in available.upper():
                        sheet_name = available
                        break

            if not sheet_name:
                st.error(f"Required sheet for Tower {tower_letter} not found in file. Available sheets: {', '.join(available_sheets)}")
                logger.error(f"Required sheet for Tower {tower_letter} not found in {filename}")
                return [(None, None)]

            file_stream.seek(0)

            try:
                df = pd.read_excel(file_stream, sheet_name=sheet_name, header=0)

                expected_columns = [
                    'Module', 'Floor', 'Flat', 'Domain', 'Activity ID', 'Activity Name',
                    'Monthly Look Ahead', 'Baseline Duration', 'Baseline Start', 'Baseline Finish',
                    'Actual Start', 'Actual Finish', '% Complete', 'Start', 'Finish', 'Delay Reasons'
                ]

                if len(df.columns) < len(expected_columns):
                    st.warning(f"Excel file has fewer columns than expected ({len(df.columns)} found, {len(expected_columns)} expected).")
                    expected_columns = expected_columns[:len(df.columns)]

                df.columns = expected_columns[:len(df.columns)]

                target_columns = ["Module", "Floor", "Flat", "Activity ID", "Activity Name", "Actual Finish"]
                available_columns = [col for col in target_columns if col in df.columns]

                if len(available_columns) < len(target_columns):
                    missing_cols = [col for col in target_columns if col not in available_columns]
                    st.warning(f"Missing columns in file: {', '.join(missing_cols)}")
                    for col in missing_cols:
                        df[col] = None

                df = df[target_columns]
                df = df.dropna(subset=['Activity Name'])

                df['Activity Name'] = df['Activity Name'].astype(str).str.strip()

                if 'Floor' in df.columns:
                    df['Floor'] = df['Floor'].astype(str)
                    v_rows = df[df['Floor'].str.strip().str.upper() == 'V']
                    if not v_rows.empty:
                        df = pd.concat([df, v_rows], ignore_index=True)

                if 'Actual Finish' in df.columns:
                    df['Actual_Finish_Original'] = df['Actual Finish'].astype(str)
                    df['Actual Finish'] = pd.to_datetime(df['Actual Finish'], errors='coerce')
                    has_na_mask = (
                        pd.isna(df['Actual Finish']) |
                        (df['Actual_Finish_Original'].str.upper() == 'NAT') |
                        (df['Actual_Finish_Original'].str.lower().isin(['nan', 'na', 'n/a', 'none', '']))
                    )
                    na_rows = df[has_na_mask][['Activity Name', 'Actual Finish']]
                    if not na_rows.empty:
                        st.write("Sample of rows with NA or invalid values in Actual Finish:")
                        st.write(na_rows.head(10))
                        na_activities = na_rows.groupby('Activity Name').size().reset_index(name='Count')
                        st.write("Activities with NA or invalid Actual Finish values:")
                        st.write(na_activities)
                    else:
                        st.write("No NA or invalid values found in Actual Finish")
                    df.drop('Actual_Finish_Original', axis=1, inplace=True)

                st.write(f"Unique Activity Names in {sheet_name}:")
                unique_activities = df[['Module', 'Floor', 'Activity Name']].drop_duplicates()
                st.write(unique_activities)

                return [(df, f"Tower {tower_letter} Finishing")]

            except Exception as e:
                st.error(f"Error processing sheet {sheet_name}: {str(e)}")
                logger.error(f"Error processing sheet {sheet_name}: {str(e)}")
                return [(None, None)]

    except Exception as e:
        st.error(f"Error loading Excel file {filename}: {str(e)}")
        logger.error(f"Error loading Excel file {filename}: {str(e)}")
        return [(None, None)]


    
#Slab code
def GetSlabReport():
    foundeligo = False
    today = date.today()
    prev_month = today - relativedelta(months=1)
    month_year = today.strftime("%m-%Y")
    prev_month_year = prev_month.strftime("%m-%Y")
    
    try:
        logger.info("Attempting to initialize COS client...")
        cos_client = ibm_boto3.client(
            's3',
            ibm_api_key_id=COS_API_KEY,
            ibm_service_instance_id=COS_SERVICE_INSTANCE_ID,
            config=Config(
                signature_version='oauth',
                connect_timeout=180,
                read_timeout=180,
                retries={'max_attempts': 15}
            ),
            endpoint_url=COS_ENDPOINT
        )
        
        response = cos_client.list_objects_v2(Bucket="projectreportnew")
        files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.xlsx')]
        
        # NEW: Extract dates from Structure Work Tracker files and find the latest
        structure_files = []
        
        for file in files:
            if file.startswith("Eligo") and "Structure Work Tracker" in file:
                st.write(f"Found Structure Work Tracker file: {file}")
                
                # Extract date from filename using multiple regex patterns
                import re
                date_patterns = [
                    r'(\d{2}-\d{2}-\d{4})',  # DD-MM-YYYY
                    r'(\d{2}/\d{2}/\d{4})',  # DD/MM/YYYY  
                    r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
                    r'(\d{2}\.\d{2}\.\d{4})', # DD.MM.YYYY
                    r'(\d{1,2}-\d{1,2}-\d{4})', # D-M-YYYY or DD-M-YYYY
                    r'(\d{1,2}/\d{1,2}/\d{4})', # D/M/YYYY or DD/M/YYYY
                ]
                
                file_date = None
                date_str = None
                
                for pattern in date_patterns:
                    date_match = re.search(pattern, file)
                    if date_match:
                        date_str = date_match.group(1)
                        # Try to parse the date with different formats
                        date_formats = [
                            "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d.%m.%Y",
                            "%d-%m-%Y", "%d/%m/%Y"  # Handle single digit days/months
                        ]
                        
                        for fmt in date_formats:
                            try:
                                file_date = datetime.strptime(date_str, fmt)
                                break
                            except ValueError:
                                continue
                        
                        if file_date:
                            break
                
                if file_date:
                    structure_files.append({
                        'filename': file,
                        'date': file_date,
                        'date_str': date_str
                    })
                    st.write(f"  - Parsed date: {file_date.strftime('%Y-%m-%d')} from {date_str}")
                else:
                    st.warning(f"  - Could not parse date from filename: {file}")
        
        if not structure_files:
            st.error("No Structure Work Tracker files with valid dates found!")
            st.session_state.slabreport = "No Data Found"
            return
        
        # Sort by date and get the latest file (most recent first)
        structure_files.sort(key=lambda x: x['date'], reverse=True)
        latest_file_info = structure_files[0]
        latest_file = latest_file_info['filename']
        
        st.success(f"ðŸŽ¯ Selected latest Structure Work Tracker file: {latest_file}")
        st.info(f"ðŸ“… File date: {latest_file_info['date'].strftime('%Y-%m-%d')}")
        
        # Show all found files for debugging (sorted by date, newest first)
        st.write("ðŸ“‹ All Structure Work Tracker files found (sorted by date, newest first):")
        for i, file_info in enumerate(structure_files):
            marker = "ðŸ“ **SELECTED**" if i == 0 else "  "
            st.write(f"{marker} {file_info['filename']} - {file_info['date'].strftime('%Y-%m-%d')}")
        
        try:
            response = cos_client.get_object(Bucket="projectreportnew", Key=latest_file)
            
            if st.session_state.ignore_month and st.session_state.ignore_year:
                st.session_state.slabreport = ProcessGandH(
                    io.BytesIO(response['Body'].read()), 
                    st.session_state.ignore_year, 
                    st.session_state.ignore_month
                )
            else:
                st.session_state.slabreport = ProcessGandH(io.BytesIO(response['Body'].read()))
            
            foundeligo = True
            st.success(f"… Successfully processed latest file: {latest_file}")
            
        except Exception as e:
            st.error(f" Error processing latest file {latest_file}: {e}")
            st.session_state.slabreport = "No Data Found"
            
            # Try the second most recent file as fallback
            if len(structure_files) > 1:
                st.warning("Trying the second most recent file as fallback...")
                fallback_file_info = structure_files[1]
                fallback_file = fallback_file_info['filename']
                
                try:
                    response = cos_client.get_object(Bucket="projectreportnew", Key=fallback_file)
                    
                    if st.session_state.ignore_month and st.session_state.ignore_year:
                        st.session_state.slabreport = ProcessGandH(
                            io.BytesIO(response['Body'].read()), 
                            st.session_state.ignore_year, 
                            st.session_state.ignore_month
                        )
                    else:
                        st.session_state.slabreport = ProcessGandH(io.BytesIO(response['Body'].read()))
                    
                    foundeligo = True
                    st.success(f"… Successfully processed fallback file: {fallback_file}")
                    
                except Exception as fallback_error:
                    st.error(f" Fallback also failed: {fallback_error}")
                    st.session_state.slabreport = "No Data Found"
        
    except Exception as e:
        st.error(f" Error fetching COS files: {e}")
        logger.error(f"Error fetching COS files: {e}")
        st.session_state.slabreport = "No Data Found"

                   
    except Exception as e:
        print(f"Error fetching COS files: {e}")
        files = ["Error fetching COS files"]
        st.session_state.slabreport = "No Data Found"

def generatePrompt(combined_data, slab):
    try:
        st.write(slab)
        st.write(json.loads(slab))
        
        # Keep the simple approach from veridea that works
        cos_df = combined_data["COS"] if isinstance(combined_data["COS"], pd.DataFrame) else pd.DataFrame()
        asite_df = combined_data["Asite"] if isinstance(combined_data["Asite"], pd.DataFrame) else pd.DataFrame()

        # Tower mapping for eligo - map G, 2G, 3G etc. to Tower G
        def map_tower_names(df):
            if not df.empty and 'Tower' in df.columns:
                # Map all G variants to Tower G, H variants to Tower H, etc.
                df = df.copy()
                df['Tower'] = df['Tower'].astype(str)
                df.loc[df['Tower'].str.contains('G', case=False, na=False), 'Tower'] = 'Tower G'
                df.loc[df['Tower'].str.contains('H', case=False, na=False), 'Tower'] = 'Tower H' 
                df.loc[df['Tower'].str.contains('F', case=False, na=False), 'Tower'] = 'Tower F'
            return df

        # Apply tower mapping
        if not cos_df.empty:
            cos_df = map_tower_names(cos_df)
        if not asite_df.empty:
            asite_df = map_tower_names(asite_df)

        # Direct JSON conversion like in veridea (the working version)
        cos_json = cos_df[['Tower', 'Activity Name', 'Count']].to_json(orient='records', indent=2) if not cos_df.empty else "[]"
        asite_json = asite_df[['Tower', 'Activity Name', 'Count']].to_json(orient='records', indent=2) if not asite_df.empty else "[]"

        body = {
            "input": f"""
            Read the table data provided below for COS and Asite sources, which include tower-specific activity counts. Categorize the activities into the specified categories (MEP, Interior Finishing, ED Civil) for each tower in each source (COS and Asite). Compute the total count of each activity within its respective category for each tower and return the results as a JSON object with "COS" and "Asite" sections, where each section contains a list of towers, each with categories and their activities. For the MEP category in COS, calculate the total count between 'UP-First Fix' and 'CP-First Fix' and report it as 'Min. count of UP-First Fix and CP-First Fix' for each tower. If an activity is not found for a tower, include it with a count of 0. If the Structure Work category has no activities in COS, return an empty list for it. Ensure the counts are accurate, the output is grouped by tower and category, and the JSON structure is valid with no nested or repeated keys.

            The data provided is as follows:
            
            Slab:
            {slab}

            COS Table Data:
            {cos_json}

            Asite Table Data:
            {asite_json}

            Categories and Activities:
            COS:
            - MEP: EL-First Fix, UP-First Fix, CP-First Fix, Min. count of UP-First Fix and CP-First Fix, C-Gypsum and POP Punning, EL-Second Fix, Electrical
            - Interior Finishing: Installation of doors, Waterproofing Works, Wall Tiling, Floor Tiling
            - ED Civil: Concreting, Shuttering, Reinforcement, De-Shuttering
            Asite:
            - MEP: Wall Conducting, Plumbing Works, Wiring & Switch Socket, Slab Conducting
            - Interior Finishing: Waterproofing - Sunken, Wall Tile, Floor Tile, POP & Gypsum Plaster
            - ED Civil: Concreting, Shuttering, Reinforcement, De-Shuttering

            Slab:
            - Get total greens of Each Tower

            Example JSON format needed:
            {{
              "COS": [
                {{
                  "Tower": "Tower G",
                  "Categories": [
                    {{
                      "Category": "MEP",
                      "Activities": [
                        {{"Activity Name": "EL-First Fix", "Total": 0}},
                        {{"Activity Name": "UP-First Fix", "Total": 0}},
                        {{"Activity Name": "CP-First Fix", "Total": 0}},
                        {{"Activity Name": "Min. count of UP-First Fix and CP-First Fix", "Total": 0}},
                        {{"Activity Name": "C-Gypsum and POP Punning", "Total": 0}},
                        {{"Activity Name": "EL-Second Fix", "Total": 0}},
                        {{"Activity Name": "Electrical", "Total": 0}}
                      ]
                    }},
                    {{
                      "Category": "Interior Finishing",
                      "Activities": [
                        {{"Activity Name": "Installation of doors", "Total": 0}},
                        {{"Activity Name": "Waterproofing Works", "Total": 0}},
                        {{"Activity Name": "Wall Tiling", "Total": 0}},
                        {{"Activity Name": "Floor Tiling", "Total": 0}}
                      ]
                    }},
                    {{
                      "Category": "ED Civil",
                      "Activities": [
                        {{"Activity Name": "Concreting", "Total": 0}},
                        {{"Activity Name": "Shuttering", "Total": 0}},
                        {{"Activity Name": "Reinforcement", "Total": 0}},
                        {{"Activity Name": "De-Shuttering", "Total": 0}}
                      ]
                    }}
                  ]
                {{ "Tower": "Tower H", "Categories": [...] }},
                {{ "Tower": "Tower F", "Categories": [...] }}
              ],
              "Asite": [
                {{
                  "Tower": "Tower G",
                  "Categories": [
                    {{
                      "Category": "MEP",
                      "Activities": [
                        {{"Activity Name": "Wall Conducting", "Total": 0}},
                        {{"Activity Name": "Plumbing Works", "Total": 0}},
                        {{"Activity Name": "Wiring & Switch Socket", "Total": 0}},
                        {{"Activity Name": "Slab Conducting", "Total": 0}}
                      ]
                    }},
                    {{
                      "Category": "Interior Finishing",
                      "Activities": [
                        {{"Activity Name": "Waterproofing - Sunken", "Total": 0}},
                        {{"Activity Name": "Wall Tile", "Total": 0}},
                        {{"Activity Name": "Floor Tile", "Total": 0}},
                        {{"Activity Name": "POP & Gypsum Plaster", "Total": 0}}
                      ]
                    }},
                    {{
                      "Category": "ED Civil",
                      "Activities": [
                        {{"Activity Name": "Concreting", "Total": 0}},
                         {{"Activity Name": "Shuttering", "Total": 0}},
                        {{"Activity Name": "Reinforcement", "Total": 0}},
                        {{"Activity Name": "De-Shuttering", "Total": 0}}
                      ]
                    }}
                ]
                }},
                {{ "Tower": "Tower H", "Categories": [...] }},
                {{ "Tower": "Tower F", "Categories": [...] }}
              ],
              "Slab":{{
                 "Tower Name":"Total"
              }}
            }}

            Return only the JSON object, no additional text, explanations, or code. Ensure the counts are accurate, activities are correctly categorized, and the JSON structure is valid.
            """,
            "parameters": {
                "decoding_method": "greedy",
                "max_new_tokens": 8100,
                "min_new_tokens": 0,
                "stop_sequences": [],  
                "repetition_penalty": 1.0,
                "temperature": 0.1
            },
            "model_id": os.getenv("MODEL_ID_1"),
            "project_id": os.getenv("PROJECT_ID_1")
        }
        
        access_token = get_access_token(os.getenv("API_KEY_1"))
        if not access_token:
            logger.error("Failed to obtain access token for WatsonX API")
            return combined_data
            
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        
        response = requests.post(os.getenv("WATSONX_API_URL_1"), headers=headers, json=body, timeout=1000)
        
        if response.status_code != 200:
            logger.error(f"WatsonX API call failed: {response.status_code} - {response.text}")
            st.warning(f"WatsonX API failed with status {response.status_code}: {response.text}. Using fallback method to calculate totals.")
            return combined_data
            
        response_data = response.json()
        if 'results' not in response_data or not response_data['results']:
            logger.error("WatsonX API response does not contain 'results' key")
            st.warning("WatsonX API response invalid. Using fallback method to calculate totals.")
            return combined_data

        generated_text = response_data['results'][0].get('generated_text', '').strip()
        logger.info(f"Raw WatsonX API response: {generated_text[:1000]}...")
        if not generated_text:
            logger.error("WatsonX API returned empty generated text")
            st.warning("WatsonX API returned empty response. Using fallback method to calculate totals.")
            return combined_data

        # Fix 1: Enhanced JSON extraction with repair capability
        fixed_json_text = extract_and_repair_json(generated_text)
        if fixed_json_text is None:
            logger.error("Failed to extract or repair JSON from response")
            return combined_data
        
        try:
            parsed_json = json.loads(fixed_json_text)
            if not (isinstance(parsed_json, dict) and "COS" in parsed_json and "Asite" in parsed_json):
                logger.warning(f"Invalid JSON structure: {json.dumps(parsed_json, indent=2)}")
                return combined_data
            for source in ["COS", "Asite"]:
                if not isinstance(parsed_json[source], list):
                    logger.warning(f"Expected list for {source}, got: {type(parsed_json[source])}")
                    return combined_data
                for tower_data in parsed_json[source]:
                    if not isinstance(tower_data, dict) or "Tower" not in tower_data or "Categories" not in tower_data:
                        logger.warning(f"Invalid tower data in {source}: {tower_data}")
                        return combined_data
            return json.dumps(parsed_json, indent=2)  # Return standardized JSON
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON after repair attempt: {str(e)}")
            logger.error(f"Full response after repair: {fixed_json_text}")
            error_position = int(str(e).split('(char ')[1].split(')')[0]) if '(char ' in str(e) else 0
            context_start = max(0, error_position - 50)
            context_end = min(len(fixed_json_text), error_position + 50)
            logger.error(f"JSON error context: ...{fixed_json_text[context_start:error_position]}[ERROR HERE]{fixed_json_text[error_position:context_end]}...")
            st.warning(f"WatsonX API returned invalid JSON that couldn't be repaired. Error: {str(e)}. Using fallback method to calculate totals.")
            return combined_data
    
    except Exception as e:
        logger.error(f"Error in WatsonX API call: {str(e)}")
        st.warning(f"Error in WatsonX API call: {str(e)}. Using fallback method to calculate totals.")
        return combined_data




def extract_and_repair_json(text):
    # Try to find JSON content within the response
    json_match = re.search(r'\{.*\}', text, re.DOTALL)
    if json_match:
        json_text = json_match.group(0)
        
        # Common JSON repair operations
        try:
            # First try: Parse as is
            json.loads(json_text)
            return json_text
        except json.JSONDecodeError as e:
            try:
                # Fix 1: Try fixing missing commas between objects in arrays
                fixed1 = re.sub(r'}\s*{', '},{', json_text)
                json.loads(fixed1)
                logger.info("JSON fixed by adding missing commas between objects")
                return fixed1
            except json.JSONDecodeError:
                try:
                    # Fix 2: Try fixing trailing commas in arrays/objects
                    fixed2 = re.sub(r',\s*}', '}', fixed1)
                    fixed2 = re.sub(r',\s*]', ']', fixed2)
                    json.loads(fixed2)
                    logger.info("JSON fixed by removing trailing commas")
                    return fixed2
                except json.JSONDecodeError:
                    try:
                        # Fix 3: Try using a JSON repair library or more aggressive repairs
                        # Here we'll use a simple approach to balance braces/brackets
                        fixed3 = fixed2
                        count_open_braces = fixed3.count('{')
                        count_close_braces = fixed3.count('}')
                        if count_open_braces > count_close_braces:
                            fixed3 += '}' * (count_open_braces - count_close_braces)
                        
                        count_open_brackets = fixed3.count('[')
                        count_close_brackets = fixed3.count(']')
                        if count_open_brackets > count_close_brackets:
                            fixed3 += ']' * (count_open_brackets - count_close_brackets)
                        
                        # Fix unquoted keys (a common issue)
                        fixed3 = re.sub(r'([{,])\s*([a-zA-Z0-9_]+)\s*:', r'\1"\2":', fixed3)
                        
                        json.loads(fixed3)
                        logger.info("JSON fixed with aggressive repairs")
                        return fixed3
                    except json.JSONDecodeError:
                        # Final attempt: Try to load the JSON with a more permissive parser
                        try:
                            import demjson3  # type: ignore
                            parsed = demjson3.decode(json_text)
                            logger.info("JSON fixed with demjson3")
                            return json.dumps(parsed)
                        except Exception:
                            logger.error("All JSON repair attempts failed")
                            return None
    else:
        logger.error("No JSON-like content found in the response")
        return None

# Fix the getTotal function to handle the improved JSON structure
def getTotal(ai_data):
    try:
        if isinstance(ai_data, str):
            try:
                ai_data = json.loads(ai_data)
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing AI data JSON: {str(e)}")
                return [0] * len(st.session_state.get('sheduledf', pd.DataFrame()).index)
            
        if not isinstance(ai_data, dict) or "COS" not in ai_data or "Asite" not in ai_data:
            logger.error(f"AI data is not in expected format: {ai_data}")
            return [0] * len(st.session_state.get('sheduledf', pd.DataFrame()).index)

        share = []
        
        # Process sources properly
        for source in ["COS", "Asite"]:
            if not isinstance(ai_data[source], list):
                logger.error(f"{source} data is not a list: {ai_data[source]}")
                continue
                
            for tower_data in ai_data[source]:
                if not isinstance(tower_data, dict) or "Categories" not in tower_data:
                    logger.error(f"Invalid tower data format in {source}: {tower_data}")
                    continue
                    
                for category_data in tower_data["Categories"]:
                    if not isinstance(category_data, dict) or "Activities" not in category_data:
                        logger.error(f"Invalid category data format: {category_data}")
                        continue
                        
                    for activity in category_data["Activities"]:
                        if isinstance(activity, dict) and "Total" in activity:
                            total = activity["Total"]
                            share.append(int(total) if isinstance(total, (int, float)) and pd.notna(total) else 0)
                        else:
                            logger.warning(f"Activity missing Total field: {activity}")
                            share.append(0)
        
        # Ensure we have enough values for the schedule dataframe
        expected_length = len(st.session_state.get('sheduledf', pd.DataFrame()).index)
        if len(share) < expected_length:
            logger.warning(f"Not enough values in share list (got {len(share)}, need {expected_length}). Padding with zeros.")
            share.extend([0] * (expected_length - len(share)))
        elif len(share) > expected_length:
            logger.warning(f"Too many values in share list (got {len(share)}, need {expected_length}). Truncating.")
            share = share[:expected_length]
            
        return share
    except Exception as e:
        logger.error(f"Error processing AI data: {str(e)}")
        st.error(f"Error processing AI data: {str(e)}")
        return [0] * len(st.session_state.get('sheduledf', pd.DataFrame()).index)    
 



   
# Function to handle activity count display logic
def display_activity_count():
    try:
        if 'ai_response' not in st.session_state or not st.session_state.ai_response:
            st.error(" No AI-generated data available. Please run the analysis first.")
            return

        try:
            ai_data = json.loads(st.session_state.ai_response)
        except json.JSONDecodeError as e:
            st.error(f" Failed to parse AI response: {str(e)}")
            st.write("Raw AI response:")
            st.text(st.session_state.ai_response)
            return

        if not isinstance(ai_data, dict) or "COS" not in ai_data or "Asite" not in ai_data:
            st.error(" Invalid AI data format. Expected 'COS' and 'Asite' sections.")
            st.write("AI data content:")
            st.json(ai_data)
            return

        slab_df = st.session_state.get('slab_df', pd.DataFrame())
        logging.info(f"Slab cycle DataFrame in display_activity_count: {slab_df.to_dict()}")
        slab_display_df = pd.DataFrame(columns=['Tower', 'Completed'])
        slab_counts = {}
        if not slab_df.empty:
            new_rows = []
            for _, row in slab_df.iterrows():
                tower = row['Tower']
                completed = row['Completed']
                if tower == 'T4':
                    t4a_completed = completed // 2
                    t4b_completed = completed - t4a_completed
                    new_rows.append({'Tower': 'T4A', 'Completed': t4a_completed})
                    new_rows.append({'Tower': 'T4B', 'Completed': t4b_completed})
                    slab_counts['T4A'] = t4a_completed
                    slab_counts['T4B'] = t4b_completed
                else:
                    new_rows.append({'Tower': tower, 'Completed': completed})
                    slab_counts[tower] = completed
            slab_display_df = pd.DataFrame(new_rows)
            logging.info(f"Slab display DataFrame after processing: {slab_display_df.to_dict()}")
        else:
            logging.warning("Slab cycle DataFrame is empty in display_activity_count.")

        categories = {
            "COS": {
                "MEP": [
                    "EL-First Fix", "UP-First Fix", "CP-First Fix", "Min. count of UP-First Fix and CP-First Fix",
                    "C-Gypsum and POP Punning", "EL-Second Fix", "Electrical"
                ],
                "Interior Finishing": [
                    "Installation of doors", "Waterproofing Works", "Wall Tiling", "Floor Tiling"
                ],
                "ED Civil": [
                    "Concreting", "Shuttering", "Reinforcement", "De-Shuttering"
                ]

            },
            "Asite": {
                "MEP": [
                    "Wall Conducting", "Plumbing Works", "Wiring & Switch Socket", "Slab Conducting"
                ],
                "Interior Finishing": [
                    "Waterproofing - Sunken", "Wall Tile", "Floor Tile", "POP & Gypsum Plaster"
                ],
                "ED Civil": [
                    "Concreting", "Shuttering", "Reinforcement", "De-Shuttering"
                ]
            }
        }

        for source in ["COS", "Asite"]:
            st.subheader(f"{source} Activity Counts")
            source_data = ai_data.get(source, [])
            if not source_data:
                st.warning(f"No data available for {source}.")
                continue

            for tower_data in source_data:
                tower_name = tower_data.get("Tower", "Unknown Tower")
                st.write(f"#### {tower_name}")
                tower_categories = tower_data.get("Categories", [])

                if not tower_categories:
                    st.write("No categories available for this tower.")
                    continue

                tower_total = 0

                for category in categories[source]:
                    st.write(f"**{category}**")
                    category_data = next(
                        (cat for cat in tower_categories if cat.get("Category") == category),
                        {"Category": category, "Activities": []}
                    )

                    if not category_data["Activities"]:
                        st.write("No activities recorded.")
                        continue

                    activity_counts = []
                    for activity in categories[source][category]:
                        activity_info = next(
                            (act for act in category_data["Activities"] if act.get("Activity Name") == activity),
                            {"Activity Name": activity, "Total": 0}
                        )
                        count = int(activity_info["Total"]) if pd.notna(activity_info["Total"]) else 0
                        activity_counts.append({
                            "Activity Name": activity_info["Activity Name"],
                            "Count": count
                        })
                        tower_total += count

                    df = pd.DataFrame(activity_counts)
                    if not df.empty:
                        st.table(df)
                    else:
                        st.write("No activities in this category.")

                if source == "COS":
                    st.write("**Slab Cycle Counts**")
                    tower_slab_df = slab_display_df[slab_display_df['Tower'] == tower_name]
                    logging.info(f"Tower {tower_name} - Filtered slab counts: {tower_slab_df.to_dict()}")
                    if not tower_slab_df.empty:
                        st.table(tower_slab_df)
                        tower_total += tower_slab_df['Completed'].sum()
                    else:
                        st.write("No slab cycle data for this tower.")
                st.write(f"**Total for {tower_name}**: {tower_total}")

        total_cos = sum(
            act["Total"]
            for tower in ai_data.get("COS", [])
            for cat in tower.get("Categories", [])
            for act in cat.get("Activities", [])
            if isinstance(act.get("Total", 0), (int, float)) and pd.notna(act["Total"])
        )
        total_cos += sum(slab_counts.values())

        total_asite = sum(
            act["Total"]
            for tower in ai_data.get("Asite", [])
            for cat in tower.get("Categories", [])
            for act in cat.get("Activities", [])
            if isinstance(act.get("Total", 0), (int, float)) and pd.notna(act["Total"])
        )

        st.write("### Total Completed Activities")
        st.write(f"**COS Total**: {total_cos}")
        st.write(f"**Asite Total**: {total_asite}")

    except Exception as e:
        logging.error(f"Error in display_activity_count: {str(e)}")
        st.error(f" Error displaying activity counts: {str(e)}")
        st.write("AI response content (for debugging):")
        st.text(st.session_state.ai_response)




        
st.markdown(
    """
    <h1 style='font-family: "Arial Black", Gadget, sans-serif; 
               color: red; 
               font-size: 48px; 
               text-align: center;'>
        CheckList - Report
    </h1>
    """,
    unsafe_allow_html=True
)



# Combined function for Initialize All Data and Fetch COS
async def initialize_and_fetch_data(email, password):
    with st.spinner("Starting initialization and data fetching process..."):
        # Step 1: Login
        if not email or not password:
            st.sidebar.error("Please provide both email and password!")
            logger.error("Email or password not provided")
            return False
        try:
            st.sidebar.write("Logging in...")
            session_id = await login_to_asite(email, password)
            if not session_id:
                st.sidebar.error("Login failed!")
                logger.error("Login failed: No session ID returned")
                return False
            st.sidebar.success("Login successful!")
        except Exception as e:
            st.sidebar.error(f"Login failed: {str(e)}")
            logger.error(f"Login failed: {str(e)}\nStack trace:\n{traceback.format_exc()}")
            return False

        # Step 2: Get Workspace ID
        try:
            st.sidebar.write("Fetching Workspace ID...")
            await GetWorkspaceID()
            st.sidebar.success("Workspace ID fetched successfully!")
        except Exception as e:
            st.sidebar.error(f"Failed to fetch Workspace ID: {str(e)}")
            logger.error(f"Failed to fetch Workspace ID: {str(e)}\nStack trace:\n{traceback.format_exc()}")
            return False

        # Step 3: Get Project IDs
        try:
            st.sidebar.write("Fetching Project IDs...")
            await GetProjectId()
            st.sidebar.success("Project IDs fetched successfully!")
        except Exception as e:
            st.sidebar.error(f"Failed to fetch Project IDs: {str(e)}")
            logger.error(f"Failed to fetch Project IDs: {str(e)}\nStack trace:\n{traceback.format_exc()}")
            return False

        # Step 4: Get All Data
        try:
            st.sidebar.write("Fetching All Data...")
            finishing, structure, external = await GetAllDatas()
            st.session_state.eligo_tower_f_finishing = finishing
            st.session_state.eligo_structure = structure
            st.session_state.eligo_tower_g_finishing = external  
            
            st.sidebar.success("All Data fetched successfully!")
            logger.info(f"Stored eligo_tower_f_finishing: {len(finishing)} records, eligo_structure: {len(structure)} records, eligo_tower_g_finishing: {len(external)} records")
        except Exception as e:
            st.sidebar.error(f"Failed to fetch All Data: {str(e)}")
            logger.error(f"Failed to fetch All Data: {str(e)}\nStack trace:\n{traceback.format_exc()}")
            return False

        # Step 5: Get Activity Data
        try:
            st.sidebar.write("Fetching Activity Data...")
            finishing_activity_data, structure_activity_data, external_activity_data = await Get_Activity()
            # Validate DataFrames
            activity_dataframes = {
                "finishing_activity_data": finishing_activity_data,
                "structure_activity_data": structure_activity_data,
                "external_activity_data": external_activity_data,
            }
            for name, df in activity_dataframes.items():
                if df is None:
                    logger.error(f"{name} is None")
                    raise ValueError(f"{name} is None")
                if not isinstance(df, pd.DataFrame):
                    logger.error(f"{name} is not a DataFrame: {type(df)}")
                    raise ValueError(f"{name} is not a valid DataFrame")
                logger.info(f"{name} has {len(df)} records, empty: {df.empty}")
                if df.empty:
                    logger.warning(f"{name} is empty")
            # Store in session state
            st.session_state.finishing_activity_data = finishing_activity_data
            st.session_state.structure_activity_data = structure_activity_data
            st.session_state.external_activity_data = external_activity_data
            
            st.sidebar.success("Activity Data fetched successfully!")
            logger.info(f"Stored activity data - Finishing: {len(finishing_activity_data)} records, "
                        f"Structure: {len(structure_activity_data)} records, "
                        f"External: {len(external_activity_data)} records")
        except Exception as e:
            st.sidebar.error(f"Failed to fetch Activity Data: {str(e)}")
            logger.error(f"Failed to fetch Activity Data: {str(e)}\nStack trace:\n{traceback.format_exc()}")
            return False

        # Step 6: Get Location/Module Data
        try:
            st.sidebar.write("Fetching Location/Module Data...")
            finishing_location_data, structure_location_data, external_location_data = await Get_Location()
            # Validate DataFrames
            location_dataframes = {
                "finishing_location_data": finishing_location_data,
                "structure_location_data": structure_location_data,
                "external_location_data": external_location_data,
            }
            for name, df in location_dataframes.items():
                if df is None:
                    logger.error(f"{name} is None")
                    raise ValueError(f"{name} is None")
                if not isinstance(df, pd.DataFrame):
                    logger.error(f"{name} is not a DataFrame: {type(df)}")
                    raise ValueError(f"{name} is not a valid DataFrame")
                logger.info(f"{name} has {len(df)} records, empty: {df.empty}")
                if df.empty:
                    logger.warning(f"{name} is empty")
            # Store in session state
            st.session_state.finishing_location_data = finishing_location_data
            st.session_state.structure_location_data = structure_location_data
            st.session_state.external_location_data = external_location_data
            
            st.sidebar.success("Location/Module Data fetched successfully!")
            logger.info(f"Stored location data - Finishing: {len(finishing_location_data)} records, "
                        f"Structure: {len(structure_location_data)} records, "
                        f"External: {len(external_location_data)} records")
        except Exception as e:
            st.sidebar.error(f"Failed to fetch Location/Module Data: {str(e)}")
            logger.error(f"Failed to fetch Location/Module Data: {str(e)}\nStack trace:\n{traceback.format_exc()}")
            return False
        
        # Step 7: Fetch COS Files
        try:
            st.sidebar.write("Fetching COS files from Eligo folder...")
            files = get_cos_files()
            st.session_state.files = files
            if files:
                st.success(f"Found {len(files)} files in COS storage")
                for selected_file in files:
                    try:
                        st.write(f"Processing file: {selected_file}")
                        cos_client = initialize_cos_client()
                        if not cos_client:
                            st.error("Failed to initialize COS client")
                            continue
                        response = cos_client.get_object(Bucket=COS_BUCKET, Key=selected_file)
                        file_bytes = io.BytesIO(response['Body'].read())
                        result = process_file(file_bytes, selected_file)
                        if len(result) == 1:  # Handle single DataFrame for Tower G, Tower H, or Structure Work Tracker
                            (df_first, tname_first) = result[0]
                            if df_first is not None and not df_first.empty:
                                if "Tower G" in tname_first:
                                    st.session_state.cos_df_tower_g = df_first
                                    st.session_state.cos_tname_tower_g = tname_first
                                    st.write(f"Processed Data for {tname_first} - {len(df_first)} rows:")
                                    st.write(df_first.head())
                                elif "Tower H" in tname_first:
                                    st.session_state.cos_df_tower_h = df_first
                                    st.session_state.cos_tname_tower_h = tname_first
                                    st.write(f"Processed Data for {tname_first} - {len(df_first)} rows:")
                                    st.write(df_first.head())
                                elif "Structure Work Tracker" in tname_first:
                                    st.session_state.cos_df_structure = df_first
                                    st.session_state.cos_tname_structure = tname_first
                                    st.write(f"Processed Data for {tname_first} - {len(df_first)} rows:")
                                    st.write(df_first.head())
                            else:
                                st.warning(f"No valid data found in {selected_file}.")
                        else:
                            st.warning(f"Unexpected data format in {selected_file}. Expected a single DataFrame.")
                    except Exception as e:
                        st.error(f"Error loading {selected_file} from cloud storage: {str(e)}")
                        logger.error(f"Error loading {selected_file}: {str(e)}\nStack trace:\n{traceback.format_exc()}")
            else:
                st.warning("No expected Excel files available in the 'Eligo' folder of the COS bucket.")
        except Exception as e:
            st.sidebar.error(f"Failed to fetch COS files: {str(e)}")
            logger.error(f"Failed to fetch COS files: {str(e)}\nStack trace:\n{traceback.format_exc()}")
            return False

        # Step 8: Verify stored session state keys
        st.sidebar.write("Verifying stored data...")
        required_keys = [
            'eligo_tower_f_finishing', 'eligo_structure', 'eligo_tower_g_finishing',
            'finishing_activity_data', 'structure_activity_data', 'external_activity_data',
            'finishing_location_data', 'structure_location_data', 'external_location_data'
        ]
        missing_keys = [key for key in required_keys if key not in st.session_state]
        if missing_keys:
            st.sidebar.error(f"Missing session state keys: {', '.join(missing_keys)}")
            logger.error(f"Missing session state keys: {', '.join(missing_keys)}")
            st.error("Initialization and data fetching failed!")
            return False
        else:
            st.sidebar.success("All required data stored successfully!")
            logger.info("All required session state keys verified.")

        st.sidebar.write("Initialization and data fetching process completed!")
        return True


def generate_consolidated_Checklist_excel(ai_data):
    try:
        # Parse AI data if it's a string
        if isinstance(ai_data, str):
            ai_data = json.loads(ai_data)
        
        if not isinstance(ai_data, dict) or "COS" not in ai_data or "Asite" not in ai_data:
            st.error(" Invalid AI data format for Excel generation.")
            return None

        # Define the exact activities structure WITHOUT duplicates
        def get_exact_activities_structure():
            """Return the exact structure without duplicates"""
            return {
                "Civil Works": {
                    "activities": ["Concreting", "Shuttering", "Reinforcement", "De-Shuttering"],
                },
                "Interior Finishing Works": {
                    "activities": [
                        "Floor Tile", 
                        "Wall Tile",
                        "POP & Gypsum Plaster",
                        "Waterproofing - Sunken"
                    ],
                },
                "MEP Works": {
                    "activities": [
                        "Plumbing Works",
                        "Slab Conducting", 
                        "Wall Conducting",
                        "Wiring & Switch Socket"
                    ],
                }
            }

        # COS to Asite activity mapping
        cos_to_asite_mapping = {
            "EL-First Fix": "Wall Conducting",
            "UP-First Fix": "Plumbing Works",
            "CP-First Fix": "Plumbing Works",
            "Min. count of UP-First Fix and CP-First Fix": "Plumbing Works",
            "C-Gypsum and POP Punning": "POP & Gypsum Plaster",
            "EL-Second Fix": "Wiring & Switch Socket",
            "Waterproofing Works": "Waterproofing - Sunken",
            "Wall Tiling": "Wall Tile",
            "Floor Tiling": "Floor Tile",
            "Concreting": "Concreting",
            "Shuttering": "Shuttering",
            "Reinforcement": "Reinforcement", 
            "De-Shuttering": "De-Shuttering",
            "No. of Slab cast": "Slab Conducting"
        }

        # Initialize consolidated rows
        consolidated_rows = []

        # Process Slab data
        slab_data_dict = {}
        if "Slab" in ai_data:
            slab_data = ai_data["Slab"]
            for tower_name, total_count in slab_data.items():
                if tower_name not in ["Tower Name", "Total"]:
                    if "Tower" in tower_name:
                        tower_short = tower_name.replace("Tower ", "T").replace("(", "").replace(")", "")
                    else:
                        tower_short = tower_name
                    
                    count = int(total_count) if pd.notna(total_count) else 0
                    
                    if tower_short == "T4":
                        half_count = count // 2
                        remainder = count % 2
                        slab_data_dict["T4A"] = half_count + remainder
                        slab_data_dict["T4B"] = half_count
                    else:
                        slab_data_dict[tower_short] = count

        # Process COS data and aggregate by unique activity
        cos_data_dict = {}
        for tower_data in ai_data.get("COS", []):
            tower_name = tower_data.get("Tower", "Unknown Tower")
            if "Tower" in tower_name:
                tower_short = tower_name.replace("Tower ", "T").replace("(", "").replace(")", "")
            else:
                tower_short = tower_name
            
            for category_data in tower_data.get("Categories", []):
                for activity in category_data.get("Activities", []):
                    activity_name = activity.get("Activity Name", "Unknown Activity")
                    count = int(activity.get("Total", 0)) if pd.notna(activity.get("Total")) else 0
                    
                    if tower_short == "T4":
                        half_count = count // 2
                        remainder = count % 2
                        
                        key_4a = (f"T4A", activity_name)
                        cos_data_dict[key_4a] = cos_data_dict.get(key_4a, 0) + half_count + remainder
                        
                        key_4b = (f"T4B", activity_name)
                        cos_data_dict[key_4b] = cos_data_dict.get(key_4b, 0) + half_count
                    else:
                        key = (tower_short, activity_name)
                        cos_data_dict[key] = cos_data_dict.get(key, 0) + count

        # Process Asite data and aggregate by unique activity
        asite_data_dict = {}
        for tower_data in ai_data.get("Asite", []):
            tower_name = tower_data.get("Tower", "Unknown Tower")
            if "Tower" in tower_name:
                tower_short = tower_name.replace("Tower ", "T").replace("(", "").replace(")", "")
            else:
                tower_short = tower_name
            
            for category_data in tower_data.get("Categories", []):
                for activity in category_data.get("Activities", []):
                    activity_name = activity.get("Activity Name", "Unknown Activity")
                    count = int(activity.get("Total", 0)) if pd.notna(activity.get("Total")) else 0
                    
                    if tower_short == "T4":
                        half_count = count // 2
                        remainder = count % 2
                        
                        key_4a = (f"T4A", activity_name)
                        asite_data_dict[key_4a] = asite_data_dict.get(key_4a, 0) + half_count + remainder
                        
                        key_4b = (f"T4B", activity_name)
                        asite_data_dict[key_4b] = asite_data_dict.get(key_4b, 0) + half_count
                    else:
                        key = (tower_short, activity_name)
                        asite_data_dict[key] = asite_data_dict.get(key, 0) + count

        # Normalize COS data to use Asite activity names and aggregate
        normalized_cos_data = {}
        for (tower, cos_activity), count in cos_data_dict.items():
            if cos_activity in ["UP-First Fix", "CP-First Fix"]:
                asite_activity = "Plumbing Works"
                key = (tower, asite_activity)
                existing_count = normalized_cos_data.get(key, float('inf'))
                normalized_cos_data[key] = min(existing_count, count) if existing_count != float('inf') else count
            elif cos_activity == "Min. count of UP-First Fix and CP-First Fix":
                asite_activity = "Plumbing Works"
                key = (tower, asite_activity)
                normalized_cos_data[key] = count
            elif cos_activity in cos_to_asite_mapping:
                asite_activity = cos_to_asite_mapping[cos_activity]
                key = (tower, asite_activity)
                normalized_cos_data[key] = normalized_cos_data.get(key, 0) + count

        # Merge slab data for Concreting
        for tower_name, slab_count in slab_data_dict.items():
            key = (tower_name, "Concreting")
            normalized_cos_data[key] = slab_count

        # Get all towers from both datasets and FILTER OUT UNWANTED TOWERS
        all_towers = set()
        for key in normalized_cos_data.keys():
            all_towers.add(key[0])
        for key in asite_data_dict.keys():
            all_towers.add(key[0])

        # FILTER OUT UNWANTED TOWER NAMES
        unwanted_towers = ["Structure", "structure", "STRUCTURE", "Unknown Tower", "Quality", "quality", "Unknown", ""]
        filtered_towers = set()

        for tower in all_towers:
            # Skip unwanted tower names
            if tower in unwanted_towers:
                continue
            
            # Skip towers that don't start with 'T' (assuming valid towers are TF, TG, TH, etc.)
            if not tower.startswith('T'):
                continue
                
            # Check if tower has any meaningful data
            has_cos_data = any(normalized_cos_data.get((tower, activity), 0) > 0 
                              for activity in ["Concreting", "Shuttering", "Reinforcement", "De-Shuttering",
                                             "Floor Tile", "Wall Tile", "POP & Gypsum Plaster", "Waterproofing - Sunken",
                                             "Plumbing Works", "Slab Conducting", "Wall Conducting", "Wiring & Switch Socket"])
            
            has_asite_data = any(asite_data_dict.get((tower, activity), 0) > 0 
                                for activity in ["Concreting", "Shuttering", "Reinforcement", "De-Shuttering",
                                               "Floor Tile", "Wall Tile", "POP & Gypsum Plaster", "Waterproofing - Sunken", 
                                               "Plumbing Works", "Slab Conducting", "Wall Conducting", "Wiring & Switch Socket"])
            
            has_slab_data = slab_data_dict.get(tower, 0) > 0
            
            # Only include towers with actual data OR valid tower names like TF, TG, TH
            if has_cos_data or has_asite_data or has_slab_data or tower in ['TF', 'TG', 'TH', 'T4A', 'T4B']:
                filtered_towers.add(tower)

        all_towers = filtered_towers

        # Add debug output to see what's being processed
        st.write(f"DEBUG: Final towers to process: {sorted(all_towers)}")

        if not all_towers:
            st.warning("No valid towers found after filtering.")
            return None

        # Generate the exact structure WITHOUT duplicates
        activities_structure = get_exact_activities_structure()
        
        for tower in sorted(all_towers):
            for category, category_info in activities_structure.items():
                activities_list = category_info["activities"]
                
                for activity_name in activities_list:
                    # Get the total counts for this activity
                    cos_total = normalized_cos_data.get((tower, activity_name), 0)
                    asite_total = asite_data_dict.get((tower, activity_name), 0)
                    
                    open_missing_count = abs(cos_total - asite_total)
                    
                    consolidated_rows.append({
                        "Tower": tower,
                        "Category": category,
                        "Activity Name": activity_name,
                        "Completed Work*(Count of Flat)": cos_total,
                        "In Progress ": 0,
                        "Closed checklist against completed work": asite_total,
                        "Open/Missing check list": open_missing_count
                    })

        # Create DataFrame
        df = pd.DataFrame(consolidated_rows)
        if df.empty:
            st.warning("No data available to generate consolidated checklist.")
            return None

        # Create Excel file
        output = BytesIO()
        workbook = Workbook()

        if "Sheet" in workbook.sheetnames:
            workbook.remove(workbook["Sheet"])

        # Define styles
        header_font = Font(bold=True)
        category_font = Font(bold=True, italic=True)
        border = Border(
            left=Side(style='thin'), right=Side(style='thin'),
            top=Side(style='thin'), bottom=Side(style='thin')
        )
        center_alignment = Alignment(horizontal='center')

        # Create the consolidated sheet
        worksheet = workbook.create_sheet(title="Consolidated Checklist")
        current_row = 1

        # Group by Tower
        grouped_by_tower = df.groupby('Tower')

        for tower, tower_group in grouped_by_tower:
            # Tower header
            worksheet.cell(row=current_row, column=1).value = tower
            worksheet.cell(row=current_row, column=1).font = header_font
            current_row += 1

            # Group by Category in the specific order
            category_order = ["Civil Works", "Interior Finishing Works", "MEP Works"]
            
            for category in category_order:
                cat_group = tower_group[tower_group['Category'] == category]
                if cat_group.empty:
                    continue
                    
                # Category header
                worksheet.cell(row=current_row, column=1).value = f"May Checklist Status - {category}"
                worksheet.cell(row=current_row, column=1).font = category_font
                current_row += 1

                # Column headers
                headers = [
                    "ACTIVITY NAME",
                    "Completed Work*(Count of Flat)",
                    "In Progress ",
                    "Closed checklist against completed work",
                    "Open/Missing check list"
                ]
                for col, header in enumerate(headers, start=1):
                    cell = worksheet.cell(row=current_row, column=col)
                    cell.value = header
                    cell.font = header_font
                    cell.border = border
                    cell.alignment = center_alignment
                current_row += 1

                # Activity rows - maintain exact order from structure
                activity_order = activities_structure[category]["activities"]
                for activity_name in activity_order:
                    # Find the corresponding row in cat_group
                    activity_row = cat_group[cat_group['Activity Name'] == activity_name]
                    if not activity_row.empty:
                        row_data = activity_row.iloc[0]
                        
                        worksheet.cell(row=current_row, column=1).value = row_data["Activity Name"]
                        worksheet.cell(row=current_row, column=2).value = row_data["Completed Work*(Count of Flat)"]
                        worksheet.cell(row=current_row, column=3).value = row_data["In Progress "]
                        worksheet.cell(row=current_row, column=4).value = row_data["Closed checklist against completed work"]
                        worksheet.cell(row=current_row, column=5).value = row_data["Open/Missing check list"]
                        
                        for col in range(1, 6):
                            cell = worksheet.cell(row=current_row, column=col)
                            cell.border = border
                            cell.alignment = center_alignment
                        current_row += 1

                # Total row
                total_open_missing = tower_group[tower_group['Category'] == category]["Open/Missing check list"].sum()
                worksheet.cell(row=current_row, column=1).value = "TOTAL pending checklist MAY"
                worksheet.cell(row=current_row, column=4).value = total_open_missing
                
                for col in range(1, 5):
                    cell = worksheet.cell(row=current_row, column=col)
                    cell.font = category_font
                    cell.border = border
                    cell.alignment = center_alignment
                current_row += 1
                current_row += 1  # Extra space between categories

        # Adjust column widths
        for col in worksheet.columns:
            max_length = 0
            column = col[0].column_letter
            for cell in col:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            worksheet.column_dimensions[column].width = adjusted_width

        workbook.save(output)
        output.seek(0)

        logger.info("Excel file generated successfully with proper data distribution")
        return output

    except Exception as e:
        logger.error(f"Error generating consolidated Excel: {str(e)}", exc_info=True)
        st.error(f"Error generating Excel file: {str(e)}")
        return None

    


# Streamlit UI - Modified Button Code
st.sidebar.title(" Asite Initialization")
email = st.sidebar.text_input("Email", "impwatson@gadieltechnologies.com", key="email_input")
password = st.sidebar.text_input("Password", "Srihari@790$", type="password", key="password_input")

if st.sidebar.button("Initialize and Fetch Data"):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        success = loop.run_until_complete(initialize_and_fetch_data(email, password))
        if success:
            st.sidebar.success("Initialization and data fetching completed successfully!")
        else:
            st.sidebar.error("Initialization and data fetching failed!")
    except Exception as e:
        st.sidebar.error(f"Initialization and data fetching failed: {str(e)}")
    finally:
        loop.close()



# Combined function to handle both analysis and activity count display
def run_analysis_and_display():
    try:
        st.write("Running status analysis...")
        AnalyzeStatusManually()
        st.success("Status analysis completed successfully!")

        st.write("Processing AI data totals...")
        if 'ai_response' not in st.session_state or not st.session_state.ai_response:
            st.error("No AI data available to process totals. Please ensure analysis ran successfully.")
            return

        st.write("Displaying activity counts...")
        display_activity_count()
        st.success("Activity counts displayed successfully!")

        st.write("Generating consolidated checklist Excel file...")
        if 'ai_response' not in st.session_state or not st.session_state.ai_response:
            st.error("No AI data available to generate Excel. Please ensure analysis ran successfully.")
            return

        with st.spinner("Generating Excel file... This may take a moment."):
            excel_file = generate_consolidated_Checklist_excel(st.session_state.ai_response)
        
        if excel_file:
            timestamp = pd.Timestamp.now(tz='Asia/Kolkata').strftime('%Y%m%d_%H%M')
            file_name = f"Consolidated_Checklist_Eligo_{timestamp}.xlsx"
            
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.sidebar.download_button(
                    label="Download Checklist Excel",
                    data=excel_file,
                    file_name=file_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_excel_button",
                    help="Click to download the consolidated checklist in Excel format."
                )
            st.success("Excel file generated successfully! Click the button above to download.")
        else:
            st.error("Failed to generate Excel file. Please check the logs for details.")

    except Exception as e:
        st.error(f"Error during analysis, display, or Excel generation: {str(e)}")
        logging.error(f"Error during analysis, display, or Excel generation: {str(e)}")


st.sidebar.title("Status Analysis")

if st.sidebar.button("Analyze and Display Activity Counts"):
    run_analysis_and_display()

st.sidebar.title("Slab Cycle")
st.session_state.ignore_year = datetime.now().year
st.session_state.ignore_month = datetime.now().month