from datetime import datetime
import uuid

import pandas as pd
import pytz

def convert_uuid_to_str(df):
    for column in df.columns:
        if isinstance(df[column].iloc[0], uuid.UUID) or 'id' in column:  # Check if the first element is a UUID
            df[column] = df[column].apply(lambda x: str(x))
    return df


def batch_convert_uuid(dataframes):
    return [convert_uuid_to_str(df) if isinstance(df, pd.DataFrame) and not df.empty else df for df in dataframes]


def shift_date(df):
    if not isinstance(df, pd.DataFrame):
        return df
    # Check and convert 'created_at' column
    for column in ['create_date', 'created_at', 'updated_at']:
        if column in df.columns:
            # df[column] = pd.to_datetime(df[column])
            # Check if the column contains timezone information
            if df[column].dt.tz is not None:
                df[column] = df[column].apply(lambda x: x.tz_convert('Asia/Taipei'))
            else:
                # If no timezone, localize to UTC first, then convert
                df[column] = df[column].dt.tz_localize('UTC').dt.tz_convert('Asia/Taipei')
            
            # Format and convert back to datetime
            df[column] = df[column].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    # Reset index
    df.reset_index(drop=True, inplace=True)
    
    return df

def batch_shift_date(dataframes):
    return [shift_date(df) if isinstance(df, pd.DataFrame) and not df.empty else df for df in dataframes]

def asia_time_zone():
    # Get the current time in UTC
    current_time_utc = datetime.now(pytz.utc)

    # Convert to Asia/Taipei timezone
    time_in_taipei = current_time_utc.astimezone(pytz.timezone('Asia/Taipei'))

    return time_in_taipei