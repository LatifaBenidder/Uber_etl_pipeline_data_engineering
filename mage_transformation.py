import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    # Convertir la colonne 'Date' en datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime']) 
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday
    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday
    datetime_dim = datetime_dim[["datetime_id","tpep_pickup_datetime","pick_hour","pick_day","pick_month","pick_year","pick_weekday","tpep_dropoff_datetime","drop_hour","drop_day","drop_month","drop_year","drop_weekday"]]

    RatecodeID_dim = df[["RatecodeID"]]
    RatecodeID_dim['RatecodeID_id'] = RatecodeID_dim.index
    RatecodeID_dim = RatecodeID_dim[["RatecodeID_id","RatecodeID"]]
    RatecodeID_dim['RatecodeID_name'] = RatecodeID_dim['RatecodeID'].map({1: 'Standard rate', 2: 'JFK' , 3: 'Newark' , 4: 'Nassau or Westchester' , 5: 'Negotiated fare' , 6: 'Group ride'})
    payment_type_dim = df[["payment_type"]]
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim = payment_type_dim[["payment_type_id","payment_type"]]
    payment_type_dim['payment_type'].value_counts()
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map({1: 'Credit card', 2: 'Cash' , 3: 'No charge' , 4: 'Dispute' , 5: 'Unknown' , 6: 'Voided trip'})
    pickup_location_dim = df[["pickup_longitude","pickup_latitude"]]
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[["pickup_location_id","pickup_longitude","pickup_latitude"]]
    dropoff_location_dim = df[["dropoff_longitude","dropoff_latitude"]]
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[["dropoff_location_id","dropoff_longitude","dropoff_latitude"]]
    store_and_fwd_flag_dim = df[["store_and_fwd_flag"]]
    store_and_fwd_flag_dim['store_and_fwd_flag_id'] = store_and_fwd_flag_dim.index
    store_and_fwd_flag_dim['store_and_fwd_flag_name'] = store_and_fwd_flag_dim['store_and_fwd_flag'].map({"Y": 'store and forward trip', "N": 'not a store and forward trip'})
    store_and_fwd_flag_dim = store_and_fwd_flag_dim[["store_and_fwd_flag_id","store_and_fwd_flag","store_and_fwd_flag_name"]]
    df['trip_id'] = df.index
    fact_table = df.merge(store_and_fwd_flag_dim, left_on='trip_id', right_on='store_and_fwd_flag_id') \
               .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id') \
               .merge(pickup_location_dim,left_on='trip_id', right_on='pickup_location_id') \
               .merge(payment_type_dim,left_on='trip_id', right_on='payment_type_id') \
               .merge(RatecodeID_dim,left_on='trip_id', right_on='RatecodeID_id') \
               .merge(datetime_dim,left_on='trip_id', right_on='datetime_id') \
               [['trip_id','VendorID', 'datetime_id', 'passenger_count',
               'pickup_location_id', 'dropoff_location_id', 'RatecodeID_id', 'fare_amount', 'extra',
               'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'payment_type_id',
               'store_and_fwd_flag_id']]
    
    return {"datetime_dim":datetime_dim.to_dict(orient="dict"),
    "store_and_fwd_flag_dim":store_and_fwd_flag_dim.to_dict(orient="dict"),
    "dropoff_location_dim":dropoff_location_dim.to_dict(orient="dict"),
    "pickup_location_dim":pickup_location_dim.to_dict(orient="dict"),
    "payment_type_dim":payment_type_dim.to_dict(orient="dict"),
    "RatecodeID_dim":RatecodeID_dim.to_dict(orient="dict"),
    "RatecodeID_dim":RatecodeID_dim.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

