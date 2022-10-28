import os
import pandas
from pathlib import Path
RAW_DATA_PATH = 'data/'
PROCESSED_DATA_PATH = 'processed_data/'


os.mkdir(PROCESSED_DATA_PATH)

raw_data_entries = os.listdir(RAW_DATA_PATH)

for file_name in raw_data_entries:
    print('preprocessing file: ' + file_name)
    dataframe = pandas.read_csv(RAW_DATA_PATH + file_name, index_col=0)
    if file_name.split('_')[0] == "City":
        dataframe = dataframe.dropna(subset=["2018-01-31"])
        dataframe = dataframe.fillna(method='ffill', axis=1)
    else:
        dataframe = dataframe.dropna()
    processed_file_name = file_name.split('.')[0] + '_processed.csv'
    filepath = Path(PROCESSED_DATA_PATH + processed_file_name)
    dataframe.to_csv(filepath)
print('preprocess done')
