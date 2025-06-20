# Kaizen AnalytiX LLC - Proprietary and Confidential

from pyspark.dbutils import DBUtils
import pandas as pd
import numpy as np
import datetime as dt
import dateutil.relativedelta
from util.pods_io import Pods_IO
from util.utils import parse_args, commercial_ncommercial, spark_session
import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pyspark.pandas as ps
import time

# ------------------------------------------------------------------------------
# Helper Function: data_prep_handling
# ------------------------------------------------------------------------------

def data_prep_handling(order_jobs, dates_dict, pio):
    covid_exclude_start = dates_dict['covid_exclude_start']
    covid_exclude_end = dates_dict['covid_exclude_end']
    hist_length = dates_dict['hist_length']

    wh_list = pio.read_sf_table('input_warehouse_master').pandas_api()
    wh_list = wh_list[['WHID', 'REGION', 'MARKET']]

    # Convert dates
    order_jobs = order_jobs[(order_jobs.CONTAINERBOOKEDDATE != '0001-01-01') & (~order_jobs.CONTAINERBOOKEDDATE.isna())]
    order_jobs.CONTAINERBOOKEDDATE = pd.to_datetime(order_jobs.CONTAINERBOOKEDDATE, errors='coerce')

    # Assign booking month
    order_jobs['BOOKING_MONTH'] = order_jobs.CONTAINERBOOKEDDATE.dt.month
    order_jobs['BOOKING_YEAR'] = order_jobs.CONTAINERBOOKEDDATE.dt.year

    # Filter and fill values
    actual_input = order_jobs[order_jobs['CANCELDATE'].isna()]
    actual_input = actual_input.to_spark().withColumn(
        'CURR_DESTINATION_WAREHOUSEID',
        F.when(F.col('CURR_DESTINATION_WAREHOUSEID').isNull(), F.col('CURR_ORIGIN_WAREHOUSEID'))
         .otherwise(F.col('CURR_DESTINATION_WAREHOUSEID'))
    ).withColumn(
        'CURR_ORIGIN_WAREHOUSEID',
        F.when(F.col('CURR_ORIGIN_WAREHOUSEID').isNull(), F.col('CURR_DESTINATION_WAREHOUSEID'))
         .otherwise(F.col('CURR_ORIGIN_WAREHOUSEID'))
    ).pandas_api()

    # Remove unwanted types
    actual_input = actual_input[
        ~(((actual_input['PRODUCTTYPE'] == 'Interfranchise') & (actual_input['STORAGETYPE'] == 'Storage Center')) |
          ((actual_input['PRODUCTTYPE'] == 'Local') & (actual_input['STORAGETYPE'].isin(['Onsite', 'Storage Center'])))
         )
    ]
    actual_input = actual_input[actual_input['CURR_ORIGIN_WAREHOUSEID'].notnull()]

    act_end = actual_input['CONTAINERBOOKEDDATE'].max()
    return actual_input, wh_list, act_end

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main(job_run_id):
    pio = Pods_IO(job_run_id)
    config = pio.read_config()

    cr = config['Run Type']
    covid_exclude_start = config['covid_exclude_start']
    covid_exclude_end = config['covid_exclude_end']
    hist_length = config['hist_length']

    dates_dict = {
        'covid_exclude_start': covid_exclude_start,
        'covid_exclude_end': covid_exclude_end,
        'hist_length': hist_length
    }

    # Load and prep data
    order_jobs = pio.read_sf_table("input_jobs_data").pandas_api()
    actual_input, wh_list, act_end = data_prep_handling(order_jobs, dates_dict, pio)

    # Example: create booking month
    actual_input['BOOKING_MONTH'] = actual_input['CONTAINERBOOKEDDATE'].dt.month
    actual_input['BOOKING_YEAR'] = actual_input['CONTAINERBOOKEDDATE'].dt.year

    print("BOOKING_MONTH successfully extracted.")
