def IF_dist_input_prep(orders_df, config, cr, pio):
    setback_period = config['setback_period']
    if_hist = config['if_hist']

    od_df = orders_df[~orders_df['DESTINATION_WAREHOUSEID'].isna()]
    od_df = od_df[od_df['PRODUCTTYPE'] == 'Interfranchise']

    end_date = od_df['CONTAINERBOOKEDDATE'].max() - dateutil.relativedelta.relativedelta(days=setback_period)
    start_date = end_date - dateutil.relativedelta.relativedelta(months=12 * if_hist)
    od_df = od_df.loc[od_df['CONTAINERBOOKEDDATE'].between(pd.to_datetime(start_date), end_date)].reset_index(drop=True)

    od_df = od_df[~od_df['NEW_MOVE_DATE'].isna()]
    od_df['BOOKING_MONTH'] = pd.to_datetime(od_df['CONTAINERBOOKEDDATE'], errors='coerce').dt.strftime('%Y-%m')

    sdf = od_df.to_spark()
    sdf = sdf.withColumn(
        'WTW_MONTH',
        F.when(F.col('WTW_MOVE_DATE').isNull(), F.month('NEW_MOVE_DATE'))
         .otherwise(F.month('WTW_MOVE_DATE'))
    )
    od_df = sdf.pandas_api()

    od_df = od_df[['CUSTOMERGROUPNAME','ACCOUNTTYPE','PRODUCTTYPE','ORIGIN_WAREHOUSEID','DESTINATION_WAREHOUSEID','CONTAINERBOOKEDDATE','WTW_MONTH','BOOKING_MONTH']]

    if cr == 'commercial':
        pio.write_temp_table(od_df.to_spark(), 'IF_dest_input_c')
    elif cr == 'non-commercial':
        pio.write_temp_table(od_df.to_spark(), 'IF_dest_input_r')

    return od_df


def Other_dist_input_prep(orders_df, config, cr, pio):
    setback_period = config['setback_period']
    other_hist = config['other_hist']

    other_df = orders_df[~orders_df['FPU_MOVE_DATE'].isna()]
    other_df['FPU_MONTH'] = other_df['FPU_MOVE_DATE'].dt.month.astype(int)

    end_date = other_df['CONTAINERBOOKEDDATE'].max() - dateutil.relativedelta.relativedelta(days=setback_period)
    start_date = end_date - dateutil.relativedelta.relativedelta(months=12 * other_hist)
    other_df = other_df.loc[other_df['FPU_MOVE_DATE'].between(pd.to_datetime(start_date), end_date)].reset_index(drop=True)

    other_df['BOOKING_MONTH'] = pd.to_datetime(other_df['CONTAINERBOOKEDDATE'], errors='coerce').dt.strftime('%Y-%m')

    other_df = other_df[['CUSTOMERGROUPNAME','ACCOUNTTYPE','ORIGIN_WAREHOUSEID','CONTAINERBOOKEDDATE','CORE','OTHER','FPU_MONTH','PRODUCTTYPE','OTHER_ORIGIN','OTHER_DESTINATION','OTHER_REPOSITION','DESTINATION_WAREHOUSEID','BOOKING_MONTH']]

    if cr == 'commercial':
        pio.write_temp_table(other_df, 'other_jobs_input_c')
    elif cr == 'non-commercial':
        pio.write_temp_table(other_df, 'other_jobs_input_r')

    return other_df


def Self_dist_input_prep(orders_df, config, cr, pio):
    setback_period = config['setback_period']
    self_hist = config['self_hist']

    self_df = orders_df[
        ((~orders_df['NEW_MOVE_DATE'].isna()) | (~orders_df['FPU_MOVE_DATE'].isna())) &
        (orders_df['REBOOKEDORDER'] == 0)
    ]

    end_date = self_df['CONTAINERBOOKEDDATE'].max() - dateutil.relativedelta.relativedelta(days=setback_period)
    start_date = end_date - dateutil.relativedelta.relativedelta(months=12 * self_hist)
    self_df = self_df.loc[
        (self_df['NEW_MOVE_DATE'].between(pd.to_datetime(start_date), end_date)) |
        (self_df['FPU_MOVE_DATE'].between(pd.to_datetime(start_date), end_date))
    ].reset_index(drop=True)

    self_df['BOOKING_MONTH'] = pd.to_datetime(self_df['CONTAINERBOOKEDDATE'], errors='coerce').dt.strftime('%Y-%m')

    self_df = self_df[['CUSTOMERGROUPNAME','ACCOUNTTYPE','ORIGIN_WAREHOUSEID','CONTAINERBOOKEDDATE','NEW_MOVE_DATE','FPU_MOVE_DATE','ORIGINATIONDELIVERYTYPE','DESTINATIONDELIVERYTYPE','PRODUCTTYPE','RDL_MOVE_DATE','BOOKING_MONTH']]

    if cr == 'commercial':
        pio.write_temp_table(self_df, 'self_jobs_input_c')
    elif cr == 'non-commercial':
        pio.write_temp_table(self_df, 'self_jobs_input_r')

    return self_df
