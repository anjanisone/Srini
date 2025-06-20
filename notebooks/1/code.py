# Read configuration
config = pio.read_config()
today = config['Today']

# Read Orders With Moves table
print('Reading Orders With Move')
order_jobs = pio.sio.read_table('ORDERS_WITH_MOVES_ZIP_JOBS_FORECAST').pandas_api()

# Clean column names
order_jobs.columns = [c.strip("'").upper() for c in order_jobs.columns]

# Extract booking_month from containerbookeddate
order_jobs['BOOKING_MONTH'] = pd.to_datetime(order_jobs['CONTAINERBOOKEDDATE'], errors='coerce').dt.strftime('%Y-%m')

# Read warehouse master
wh_master = pio.read_input_file('warehouse_master.csv').pandas_api()
wh_master['Start_Date'] = pd.to_datetime(wh_master['Start_Date'])
wh_master['Close_Date'] = pd.to_datetime(wh_master['Close_Date'])

pio.write_sf_table(wh_master, 'input_warehouse_master')

# Fix origin region
order_jobs['ORIGIN_REGION'] = order_jobs['ORIGIN_REGION'].fillna('Franchisees')

# Filter for commercial non-spirit orders
order_jobs = order_jobs[order_jobs['COMMERCIAL'] == 'Non-Spirit']

# Handle cancellation and filtering logic
if today >= '2024-05-02':
    order_jobs = order_jobs[order_jobs['CANCELLED'] == 0]
    order_jobs = order_jobs[order_jobs['ESTIMATED_FPU'] == 0]

# Cleanup invalid date rows
order_jobs = order_jobs[
    (order_jobs['CONTAINERBOOKEDDATE'] != '0001-01-01') &
    (~order_jobs['CONTAINERBOOKEDDATE'].isna())
]

order_jobs['ACCOUNTTYPE'] = order_jobs['CUSTOMERGROUPNAME'].apply(commercial_ncommercial)
runtype = config['Run_Type']

# Split and write by runtype
if runtype == 'commercial':
    a = order_jobs[order_jobs['ACCOUNTTYPE'] == 'SMB'].copy()
    a['wh_o'] = a['CURR_ORIGIN_WAREHOUSEID']
    a['wh_d'] = a['CURR_DESTINATION_WAREHOUSEID']
    pio.write_temp_table(a, 'OrdersWithMoves_c')
    print("Commercial - OrderWithMoves written to disk")

elif runtype == 'non-commercial':
    b = order_jobs[order_jobs['ACCOUNTTYPE'] == 'Residential'].copy()
    pio.write_temp_table(b, 'OrdersWithMoves_r')
    print("Residential - OrderWithMoves written to disk")

elif runtype == 'both':
    a = order_jobs[order_jobs['ACCOUNTTYPE'] == 'SMB'].copy()
    a['wh_o'] = a['CURR_ORIGIN_WAREHOUSEID']
    a['wh_d'] = a['CURR_DESTINATION_WAREHOUSEID']
    a['CURR_ORIGIN_WAREHOUSEID'] = a[origin_region_col].fillna('Franchisees')
    a['CURR_DESTINATION_WAREHOUSEID'] = a[dest_region_col].fillna('Franchisees')
    pio.write_temp_table(a, 'OrdersWithMoves_c')

    b = order_jobs[order_jobs['ACCOUNTTYPE'] == 'Residential'].copy()
    pio.write_temp_table(b, 'OrdersWithMoves_r')
    print("Both - OrderWithMoves (commercial + residential) written to disk")

else:
    raise Exception("Please check the runtype in the config file")

print('Batch Stats Main Setup Complete.')
