import pandas as pd

def date2str(date, format='%Y-%m-%d'):
    return pd.to_datetime(date).strftime(format)