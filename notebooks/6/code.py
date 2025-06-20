# Imports and Setup
from pyspark.dbutils import DBUtils
from util.utils import spark_session
import pandas as pd
import numpy as np
import datetime as dt
import dateutil.relativedelta
from datetime import timedelta
from util.pods_io import Pods_IO
from util.utils import parse_args, commercial_ncommercial
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import date_format
from pyspark.sql.window import Window
import pyspark.pandas as ps
import time

# MAIN FUNCTION
def main(job_run_id):
    pio = Pods_IO(job_run_id)
    config = pio.read_config()

    today = pd.to_datetime(config['Today'])
    order_jobs = pio.read_sf_table('input_OrdersWithMoves').pandas_api()

    # Clean CONTAINERBOOKEDDATE and extract BOOKING_MONTH
    order_jobs = order_jobs[(order_jobs.CONTAINERBOOKEDDATE != '0001-01-01') & (~order_jobs.CONTAINERBOOKEDDATE.isna())]
    order_jobs = order_jobs[order_jobs.CONTAINERBOOKEDDATE <= today]
    order_jobs['BOOKING_MONTH'] = pd.to_datetime(order_jobs['CONTAINERBOOKEDDATE'], errors='coerce').dt.strftime('%Y-%m')

    # Apply customer/account filters
    order_jobs['ACCOUNTTYPE'] = order_jobs['CUSTOMERGROUPNAME'].apply(commercial_ncommercial)
    order_jobs = order_jobs[order_jobs['COMMERCIAL'] != 'Non-Spirit']

    # Convert to Spark DF
    sdf = order_jobs.to_spark()

    # Apply booking_month logic to all job type DFs
    keep_cols = ['WAREHOUSE', 'JOB_DATE', 'BOOKING_MONTH']
    start_date = today - timedelta(days=365)
    end_date = today

    def extract_job_df(sdf, move_col, warehouse_col='CURR_ORIGIN_WAREHOUSEID'):
        return sdf.withColumn('WAREHOUSE',
                              F.when((F.col(move_col) >= start_date) &
                                     (F.col(move_col) <= end_date) &
                                     F.col(move_col).isNotNull(), F.col(warehouse_col))
                              ) \
                  .withColumn('JOB_DATE', F.when((F.col(move_col) >= start_date) &
                                                 (F.col(move_col) <= end_date) &
                                                 F.col(move_col).isNotNull(), F.col(move_col))) \
                  .withColumn('BOOKING_MONTH', date_format(F.col(move_col), 'yyyy-MM')) \
                  .select([F.col(col) for col in keep_cols]).dropna()

    new_df = extract_job_df(sdf, 'NEW_MOVE_DATE')
    mov_df = extract_job_df(sdf, 'MOV_MOVE_DATE')
    wrt_df = extract_job_df(sdf, 'WRT_MOVE_DATE')
    wtw_df = extract_job_df(sdf, 'WTW_MOVE_DATE')
    rdl_df = extract_job_df(sdf, 'RDL_MOVE_DATE')
    fpu_df = extract_job_df(sdf, 'FPU_MOVE_DATE')

    out_df = new_df.union(mov_df).union(wrt_df).union(wtw_df).union(rdl_df).union(fpu_df)

    # Convert to pandas for downstream analysis
    final_df = out_df.pandas_api()

    print(final_df.head())
    return final_df

# Entry Point
if __name__ == "__main__":
    spark = spark_session()
    dbutils = DBUtils(spark)
    try:
        job_run_id = dbutils.jobs.taskValues.get(taskKey="Setup_Stats_Run", key="job_run_id")
    except:
        job_run_id = 100
    print(f"Got run_id of {job_run_id}")
    main(job_run_id)
