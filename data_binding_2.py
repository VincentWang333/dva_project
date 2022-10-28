import os
import pandas
from pathlib import Path
import sys
RAW_DATA_PATH = 'processed_data/'
STATES = {
    'AL': 'Alabama',
    'AK': 'Alaska',
    'AZ': 'Arizona',
    'AR': 'Arkansas',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'HI': 'Hawaii',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'IA': 'Iowa',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'ME': 'Maine',
    'MD': 'Maryland',
    'MA': 'Massachusetts',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MS': 'Mississippi',
    'MO': 'Missouri',
    'MT': 'Montana',
    'NE': 'Nebraska',
    'NV': 'Nevada',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NY': 'New York',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VT': 'Vermont',
    'VA': 'Virginia',
    'WA': 'Washington',
    'WV': 'West Virginia',
    'WI': 'Wisconsin',
    'WY': 'Wyoming',
    'DC': 'District of Columbia',
    'MP': 'Northern Mariana Islands',
    'PW': 'Palau',
    'PR': 'Puerto Rico',
    'VI': 'Virgin Islands',
    'AA': 'Armed Forces Americas (Except Canada)',
    'AE': 'Armed Forces Africa/Canada/Europe/Middle East',
    'AP': 'Armed Forces Pacific'
}


def my_print(text):
    sys.stdout.write(str(text))
    sys.stdout.flush()


raw_data_entries = os.listdir(RAW_DATA_PATH)
housing_data_entries = [x for x in raw_data_entries if x.split('_')[
    0] == "City"]
cpi_data_entries = [x for x in raw_data_entries if x.split('_')[0] == "CPI"]
covid_data_entries = [x for x in raw_data_entries if x.split('_')[0] == "time"]
final_data = pandas.DataFrame(columns=["RegionID",
                                       "SizeRank",
                                       "RegionName",
                                       "RegionType",
                                       "StateName",
                                       "State",
                                       "Metro",
                                       "CountyName",
                                       "Period",
                                       "Price",
                                       "bedroom_count",
                                       "CPI_all_items",
                                       "covid_case"])
final_data_index = 0

for file_name in housing_data_entries:
    my_print('processing' + file_name)
    file_type = file_name.split('_')[0]
    dataframe = pandas.read_csv(
        RAW_DATA_PATH + file_name, index_col=0, dtype='unicode')
    info_columns = dataframe.columns[:7]
    time_series_columns = [
        x for x in dataframe.columns[7:] if int(x.split('-')[0]) >= 2018]
    bedroom_num = file_name.split('_')[3]
    for index, row in dataframe.iterrows():
        for time_index in time_series_columns:
            final_data.loc[final_data_index, "RegionID"] = index
            for info_index in info_columns:
                final_data.loc[final_data_index, info_index] = row[info_index]
            final_data.loc[final_data_index, "Period"] = time_index
            final_data.loc[final_data_index, "Price"] = row[time_index]
            final_data.loc[final_data_index, "bedroom_count"] = bedroom_num
            final_data_index += 1
    my_print('processing finished: ' + file_name)

for file_name in cpi_data_entries:
    my_print('processing' + file_name)
    dataframe = pandas.read_csv(
        RAW_DATA_PATH + file_name, index_col=0, dtype='unicode')
    for index, row in final_data.iterrows():
        year, month = row["Period"].split("-")[0], row["Period"].split("-")[1]
        cpi_row = dataframe.loc[(dataframe["Year"] == year) & (
            dataframe["Period"] == ('M'+month))]
        if cpi_row is not None:
            row["CPI_all_items"] = cpi_row["Value"].values[0]
    my_print('processing finished: ' + file_name)

for file_name in covid_data_entries:
    my_print('processing' + file_name)
    dataframe = pandas.read_csv(
        RAW_DATA_PATH + file_name, index_col=0, dtype='unicode')
    for index, row in final_data.iterrows():
        city_name, state_initial = row["RegionName"], row["State"]
        year, month = row["Period"].split("-")[0], row["Period"].split("-")[1]
        covid_city_data = dataframe.loc[(dataframe["Admin2"] == city_name) & (
            dataframe["Province_State"] == STATES.get(state_initial))]
        if covid_city_data is not None:
            covid_time_series_columns = covid_city_data.columns[10:]
            case_number = 0
            for covid_time_unit in covid_time_series_columns:

                covid_month = covid_time_unit.split(
                    '/')[0] if len(covid_time_unit.split('/')[0]) == 2 else '0'+covid_time_unit.split('/')[0]

                covid_year = '20'+covid_time_unit.split('/')[2]
                if year == covid_year and month == covid_month:
                    case_number += int(
                        covid_city_data[covid_time_unit].values[0])
            row["covid_case"] = case_number
    my_print('processing finished: ' + file_name)

final_data = final_data.fillna(0)
filepath = Path('final_data.csv')
final_data.to_csv(filepath)
print('preprocess done')
