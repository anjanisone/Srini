CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_HIST_RUN_HISTORY AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_HIST_RUN_HISTORY;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_INPUT_ORDERS_WITH_MOVES AS
SELECT
    *,
    TO_VARCHAR(DATE_TRUNC('month', TRY_TO_DATE(containerbookeddate)), 'YYYY-MM') AS booking_month
FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_INPUT_ORDERS_WITH_MOVES;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_INPUT_WAREHOUSE_MASTER AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_INPUT_WAREHOUSE_MASTER;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_ACTUAL_JOBS AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_ACTUAL_JOBS;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_COMBINED_ADJ_FORECAST AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_COMBINED_ADJ_FORECAST;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_COMBINED_ADJ_FORECAST_TEMP_1 AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_COMBINED_ADJ_FORECAST_TEMP_1;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_COMBINED_ADJ_FORECAST_TEMP_2 AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_COMBINED_ADJ_FORECAST_TEMP_2;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_COMBINED_RAW_FORECAST AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_COMBINED_RAW_FORECAST;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_FORECAST_BOOK_LO AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_FORECAST_BOOK_LO;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_FORECAST_BOOK_LS AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_FORECAST_BOOK_LS;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_FORECAST_FCST_IF AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_FORECAST_FCST_IF;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_FORECAST_ONRENT_IF AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_FORECAST_ONRENT_IF;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_FORECAST_ONRENT_LO AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_FORECAST_ONRENT_LO;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_FORECAST_ONRENT_LS AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_FORECAST_ONRENT_LS;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_WTW_FORECAST_WH_FCST AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_WTW_FORECAST_WH_FCST;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_WTW_FORECAST_WH_ONRENT AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_WTW_FORECAST_WH_ONRENT;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_REVENUE_INPUT_BOOKING_CURVE AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_REVENUE_INPUT_BOOKING_CURVE;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_REVENUE_INPUT_CPO_COO_RATE AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_REVENUE_INPUT_CPO_COO_RATE;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_REVENUE_INPUT_DELIVERY_HISTORY AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_REVENUE_INPUT_DELIVERY_HISTORY;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_REVENUE_INPUT_RATES AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_REVENUE_INPUT_RATES;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_REVENUE_INPUT_STORAGE_FORECAST AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_REVENUE_INPUT_STORAGE_FORECAST;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_REVENUE_INPUT_STORAGE_HISTORY AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_REVENUE_INPUT_STORAGE_HISTORY;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_REVENUE_INPUT_TRANSPORTATION_HISTORY AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_REVENUE_INPUT_TRANSPORTATION_HISTORY;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_REVENUE_INPUT_TRANSPORTATION_INVOICED AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_REVENUE_INPUT_TRANSPORTATION_INVOICED;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_REVENUE_OUTPUT AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_REVENUE_OUTPUT;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_INPUT_ALL_SCHEDULED_BOOKINGS AS
SELECT
    *,
    TO_VARCHAR(DATE_TRUNC('month', TRY_TO_DATE(containerbookeddate)), 'YYYY-MM') AS booking_month
FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_INPUT_ALL_SCHEDULED_BOOKINGS;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_INPUT_AOP_FORECAST AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_INPUT_AOP_FORECAST;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_INPUT_ORDERS_WITH_MOVES AS
SELECT
    *,
    TO_VARCHAR(DATE_TRUNC('month', TRY_TO_DATE(containerbookeddate)), 'YYYY-MM') AS booking_month
FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_INPUT_ORDERS_WITH_MOVES;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_STATS_DIST_STATS_MOVE AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_STATS_DIST_STATS_MOVE;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_STATS_EOM_DIST AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_STATS_EOM_DIST;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_STATS_IF_DEST_DIST AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_STATS_IF_DEST_DIST;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_STATS_WAREHOUSE_SELF_DIST AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_STATS_WAREHOUSE_SELF_DIST;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_FORECAST_FCST_LS AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_FORECAST_FCST_LS;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_INPUT_ALL_ONRENTS AS
SELECT
    *,
    TO_VARCHAR(DATE_TRUNC('month', TRY_TO_DATE(containerbookeddate)), 'YYYY-MM') AS booking_month
FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_INPUT_ALL_ONRENTS;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_FORECAST_BOOK_IF AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_FORECAST_BOOK_IF;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_FORECAST_FCST_LO AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_FORECAST_FCST_LO;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_WTW_FORECAST_WH AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_WTW_FORECAST_WH;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_REVENUE_INPUT_ACTUAL_REVENUE AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_REVENUE_INPUT_ACTUAL_REVENUE;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_REVENUE_INPUT_DELIVERY_INVOICED AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_REVENUE_INPUT_DELIVERY_INVOICED;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_REVENUE_INPUT_STORAGE_INVOICED AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_REVENUE_INPUT_STORAGE_INVOICED;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_INPUT_MODEL_PARAMS AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_INPUT_MODEL_PARAMS;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_STATS_CANCEL_RATIO AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_STATS_CANCEL_RATIO;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_STATS_DOW_DISTRIBUTION AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_STATS_DOW_DISTRIBUTION;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_OUTPUT_WTW_FORECAST_WH_BOOK AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_OUTPUT_WTW_FORECAST_WH_BOOK;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_REVENUE_INPUT_TRANSPORTATION_FORECAST AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_REVENUE_INPUT_TRANSPORTATION_FORECAST;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_REVENUE_INPUT_DELIVERY_FORECAST AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_REVENUE_INPUT_DELIVERY_FORECAST;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_INPUT_ORDERS_WITH_MOVES_ACTUAL AS
SELECT
    *,
    TO_VARCHAR(DATE_TRUNC('month', TRY_TO_DATE(containerbookeddate)), 'YYYY-MM') AS booking_month
FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_INPUT_ORDERS_WITH_MOVES_ACTUAL;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_INPUT_PEAK_OFFPEAK AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_INPUT_PEAK_OFFPEAK;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_INPUT_WAREHOUSE_MASTER AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_INPUT_WAREHOUSE_MASTER;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_STATS_CANCELLATION_CURVES AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_STATS_CANCELLATION_CURVES;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_STATS_DIST_STATS_RATIO AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_STATS_DIST_STATS_RATIO;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_STATS_DIST_STATS_WH AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_STATS_DIST_STATS_WH;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_STATS_WAREHOUSE_OTHER_DIST AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_STATS_WAREHOUSE_OTHER_DIST;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_STATS_WAREHOUSE_STATS_CVG_PRE AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_STATS_WAREHOUSE_STATS_CVG_PRE;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_INPUT_AOP_BACKCAST AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_INPUT_AOP_BACKCAST;

CREATE OR REPLACE TABLE CITIZENDEV_ONLY.MKT_BASE.STG_JOBS_FCST_RUN_STATS_WAREHOUSE_STATS_CVG_POST AS
SELECT * FROM CITIZENDEV_ONLY.MKT_BASE.JOBS_FCST_RUN_STATS_WAREHOUSE_STATS_CVG_POST;

