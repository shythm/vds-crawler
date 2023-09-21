from vds_processing import process_vds
import pandas as pd
import multiprocessing
import sys
import os

# error handling sys.argv length is not 6
if len(sys.argv) != 6:
    raise ValueError("required arguments are not given. [start date, end date, holiday csv path, data path, output path]")

# input start date and end date via bash arguments whose format is YYYYMMDD
start_date = sys.argv[1]
end_date = sys.argv[2]
holiday_csv_path = sys.argv[3]
data_path = sys.argv[4]
output_path = sys.argv[5]

# check output path exists
if not os.path.exists(output_path):
    os.makedirs(output_path)

# get holiday dictionary where key is date and value is code from csv file
holiday_dict: dict = pd.read_csv(
    holiday_csv_path,
    delimiter=',',
    dtype={
        '날짜': str,
        '분류코드': int,
    },
).set_index('날짜').to_dict()['분류코드']

# get date list from start date to end date
dates = pd.date_range(start_date, end_date).strftime('%Y%m%d').tolist()

print(f"start processing vds data from {start_date} to {end_date}")

# process vds data with multiprocessing using len(dates) processes
with multiprocessing.Pool(processes=len(dates)) as pool:
    pool.starmap(process_vds, [
        (
            date,
            holiday_dict.get(date, 0),
            data_path,
            f"{output_path}/vds_data_5min_{date}.csv",
            False
        ) for date in dates])

print(f"end processing vds data from {start_date} to {end_date}")