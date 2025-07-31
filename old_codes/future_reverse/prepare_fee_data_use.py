def prepare_future_fee_data(future_data_fee, type):
    future_data_fee.sort_values(by=['type', 'Code', 'Date'], inplace=True)
    shift_num = 1
    future_data_fee['fee_last'] = future_data_fee.groupby(by=['type', 'Code'])['fee'].shift(shift_num)
    future_data_fee['fee_delta'] = future_data_fee['fee'] - future_data_fee['fee_last']
    future_data_fee['fee_ratio_last'] = future_data_fee.groupby(by=['type', 'Code'])['fee_ratio'].shift(shift_num)
    future_data_fee['fee_ratio_delta'] = future_data_fee['fee_ratio'] - future_data_fee['fee_ratio_last']
    future_data_fee['fee_close_last'] = future_data_fee.groupby(by=['type', 'Code'])['fee_close'].shift(shift_num)
    future_data_fee['fee_close_delta'] = future_data_fee['fee_close'] - future_data_fee['fee_close_last']
    future_data_fee['closedis_last'] = future_data_fee.groupby(by=['type', 'Code'])['close_discount_ratio'].shift(
        shift_num)
    future_data_fee['closedis_delta'] = future_data_fee['close_discount_ratio'] - future_data_fee['closedis_last']
    #
    # condition_up = (future_data_fee['fee_delta'] > 0) | (future_data_fee['fee_ratio_delta'] > 0) | (future_data_fee['closedis_delta'] > 0)
    # condition_down = (future_data_fee['fee_delta'] < 0) | (future_data_fee['fee_ratio_delta'] < 0) | (
    #             future_data_fee['closedis_delta'] < 0)
    # condition_up = (future_data_fee['fee_delta'] > 0) | (future_data_fee['fee_ratio_delta'] > 0)
    # condition_down = (future_data_fee['fee_delta'] < 0) | (future_data_fee['fee_ratio_delta'] < 0)
    condition_up = future_data_fee['closedis_delta'] > 0
    condition_down = future_data_fee['closedis_delta'] < 0
    if type == 'up':
        condition = condition_up
    elif type == 'down':
        condition = condition_down
    elif type == 'all':
        condition = condition_up | condition_down
    else:
        print('type error')
    fee_change_data = future_data_fee[condition]
    fee_change_data = fee_change_data[['type', 'Code', 'Date']]
    fee_change_data.reset_index(drop=True, inplace=True)
    return fee_change_data


def prepare_fee_data(future_data_fee, type):
    fee_change_data = prepare_future_fee_data(future_data_fee, type)
    return fee_change_data
