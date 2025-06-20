from pyspark.dbutils import DBUtils
import pandas as pd
from pyspark.sql import functions as F
import datetime as dt
from dateutil import relativedelta
from datetime import timedelta
from util.utils import parse_args, commercial_ncommercial, spark_session
from util.pods_io import Pods_IO
import sys

# -------------------------------
# ENTRY POINT
# -------------------------------

if __name__ == "__main__":
    spark = spark_session()
    dbutils = DBUtils(spark)

    try:
        job_run_id = dbutils.jobs.taskValues.get(taskKey="Setup_Stats_Run", key="job_run_id")
    except:
        job_run_id = 244  ## fallback

    print(f"Got run id of {job_run_id}")

    pio = Pods_IO(job_run_id)
    config = pio.read_config()

    cr = config['Run_Type']
    if (cr == 'commercial') or (cr == 'both'):
        main('commercial', job_run_id, config, pio)
    if (cr == 'non-commercial') or (cr == 'both'):
        main('non-commercial', job_run_id, config, pio)


# -------------------------------
# MAIN WORKFLOW
# -------------------------------

def main(current_run, job_run_id, config, pio):

    today = config['Today']
    days_prior_range = config['days_prior_range']

    ## Read the input files
    print("Reading the Input files")

    peak_df = pio.read_input_file('PeakOffPeak.csv').toPandas()
    peak_df.columns = [c.strip("'").upper() for c in peak_df.columns]

    pio.write_sf_table(peak_df, 'input_PeakOffPeak')

    peak_df = peak_df.rename(columns={
        'JAN': '1', 'FEB': '2', 'MAR': '3', 'APR': '4', 'MAY': '5', 'JUN': '6',
        'JUL': '7', 'AUG': '8', 'SEP': '9', 'OCT': '10', 'NOV': '11', 'DEC': '12'
    })

    if current_run == 'commercial':
        order_jobs = pio.read_temp_table('OrdersWithMoves_c').pandas_api()
    elif current_run == 'non-commercial':
        order_jobs = pio.read_temp_table('OrdersWithMoves_r').pandas_api()

    # ✅ Extract booking_month
    order_jobs['BOOKING_MONTH'] = pd.to_datetime(order_jobs['CONTAINERBOOKEDDATE'], errors='coerce').dt.strftime('%Y-%m')

    # Filter based on containerbookeddate and today's cutoff
    order_jobs = order_jobs[order_jobs['CONTAINERBOOKEDDATE'].dt.date <= today]

    # Replace NEW_MOVE_DATE with NEW_SCHEDULED_DATE if NULL
    order_jobs = order_jobs.to_spark().withColumn(
        'NEW_MOVE_DATE',
        F.when(F.col('NEW_MOVE_DATE').isNull(), F.col('NEW_SCHEDULED_DATE')).otherwise(F.col('NEW_MOVE_DATE'))
    ).pandas_api()

    # Create job distribution input
    jobs_distribution_input_prep(order_jobs, config, current_run, job_run_id, pio)

    # Peak mapping (commercial or residential)
    if current_run == 'commercial':
        peak_processor_commercial(peak_df, pio)
    elif current_run == 'non-commercial':
        peak_processor_residential(peak_df, pio)

    # Forecast transformation (placeholders)
    # Fcst_Results_IF = forecast_IF_prep(fcst_if)
    # Fcst_Results_LS = forecast_LS_prep(fcst_ls)
    # Fcst_Results_LO = forecast_LO_prep(fcst_lo)
    # combine_forecasts(Fcst_Results_IF, Fcst_Results_LS, Fcst_Results_LO, today, wh_list)


# -------------------------------
# JOBS DISTRIBUTION INPUT PREP
# -------------------------------

def jobs_distribution_input_prep(order_jobs, config, cr, job_run_id, pio):
    hist_start = config['hist_start']
    job_dist_hist_start = config['job_dist_hist_start']
    today = config['Today']

    cols_req = [
        'CONTAINERBOOKEDDATE', 'CANCELDATE', 'NEW_MOVE_DATE', 'NEW_SCHEDULED_DATE',
        'MOV_MOVE_DATE', 'WRT_MOVE_DATE', 'WTW_MOVE_DATE', 'RDL_MOVE_DATE', 'FPU_MOVE_DATE',
        'PRODUCTTYPE', 'STORAGETYPE', 'CURR_ORIGIN_WAREHOUSEID', 'CUSTOMERGROUPNAME',
        'ACCOUNTTYPE', 'BOOKING_MONTH'  # ✅ Include booking_month in final output
    ]

    dist_input = order_jobs[cols_req]
    dist_input = dist_input[dist_input['NEW_MOVE_DATE'].notnull()]

    # Filter unwanted rows
    dist_input = dist_input[
        ((dist_input['PRODUCTTYPE'] == 'Interfranchise') & (dist_input['STORAGETYPE'] == 'Storage Center')) |
        ((dist_input['PRODUCTTYPE'] == 'Local') & (dist_input['STORAGETYPE'] == 'Onsite'))
    ]

    # Write job distribution table with schema
    pio.write_temp_table(dist_input, 'Job_Distribution_Input', True)


# -------------------------------
# PEAK PROCESSORS
# -------------------------------

def peak_processor_residential(peak, pio):
    rows_list = []
    for i in range(len(peak)):
        dada = peak.iloc[i].reset_index()
        WHID = dada.iloc[0, 1]
        dada['WHID'] = WHID
        dada = dada.drop(dada.index[0])
        dada.columns = ['MONTH', 'PEAK', 'WHID']
        rows_list.append(dada)

    Peak_Results = pd.concat(rows_list).reset_index(drop=True)
    pio.write_temp_table(Peak_Results, 'Peak_Mapping_r', True)
    print("Peak-OffPeak - non-Commercial mapping file written to disk")

def peak_processor_commercial(peak, pio):
    row_list = []
    for i in range(len(peak)):
        dada = peak.iloc[i].reset_index()
        region = dada.iloc[0, 1]
        dada['Region'] = region
        dada = dada.drop(dada.index[0])
        dada.columns = ['MONTH', 'PEAK', 'REGION']
        row_list.append(dada)

    peak_results = pd.concat(row_list).reset_index(drop=True)
    pio.write_temp_table(peak_results, 'Peak_Mapping_c', True)
    print("Peak-OffPeak - Commercial Mapping File Created")
