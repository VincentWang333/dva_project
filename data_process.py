import os
import pandas
from pathlib import Path
DATA_PATH = 'data/'

entries = os.listdir(DATA_PATH)
for file_name in entries:
    print('preprocessing file: ' + file_name)
    file_name = 'County_zhvi_uc_condo_tier_0.33_0.67_sm_sa_month.csv'
    dataframe = pandas.read_csv(DATA_PATH + file_name)
    dataframe = dataframe.dropna()
    filepath = Path('folder/subfolder/out.csv')
print('preprocess done')
filepath.parent.mkdir(parents=True, exist_ok=True)
