import requests
from requests_toolbelt import MultipartEncoder
import os
import sys
import gzip
import pandas as pd

def download_ex_data(file_name: str, fields: dict) -> bytes:
    print(f"start crawling {file_name}")

    print(f" -> POST ...")
    multipart = MultipartEncoder(fields=fields)
    headers = {
        'Content-Type': multipart.content_type,
    }
    res = requests.post('http://data.ex.co.kr/portal/fdwn/log', headers=headers, data=multipart)

    print(f" -> decompessing ...")
    data = gzip.decompress(res.content)

    print(f" -> saving ...")
    with open(file_name, 'wb') as f:
        f.write(data)

    print(f"complete to download {file_name}")

def get_ex_data_path(date, parent_path):
    if not os.path.isdir(parent_path):
        os.mkdir(parent_path)

    return {
        'vds_data_path': f'{parent_path}/vds_data_{date}.csv',
        'con_zone_path': f'{parent_path}/con_zone_{date}.csv',
        'vds_zone_path': f'{parent_path}/vds_zone_{date}.csv',
        'vds_point_path': f'{parent_path}/vds_point_{date}.csv',
    }

if __name__ == '__main__':
    
    # error handling sys.argv length is not 4
    if len(sys.argv) != 4:
        raise ValueError("required arguments are not given. [start date, end date, parent path]")

    # input start date and end date via bash arguments whose format is YYYYMMDD
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    parent_path = sys.argv[3]
    
    # get date list from start date to end date using datetime module
    dates = pd.date_range(start_date, end_date).strftime('%Y%m%d').tolist()

    print(f"start downloading ex data from {start_date} to {end_date}")

    for date in dates:
        vds_data_post_fields = {
            'dataSupplyDate': f'{date}',
            'collectType': 'VDS',
            'dataType': '16',
            'collectCycle': '04',
            'supplyCycle': '01',
            'outFileName': f'vds_data_{date}.gz',
        }
        vds_data_file_name = get_ex_data_path(date, parent_path)['vds_data_path']
        download_ex_data(vds_data_file_name, vds_data_post_fields)

        con_zone_post_fields = {
            'dataSupplyDate': f'{date}',
            'collectType': 'ETC',
            'dataType': '78',
            'collectCycle': '04',
            'supplyCycle': '01',
            'outFileName': f'con_zone_{date}.gz',
        }
        con_zone_file_name = get_ex_data_path(date, parent_path)['con_zone_path']
        download_ex_data(con_zone_file_name, con_zone_post_fields)

        vds_zone_post_fields = {
            'dataSupplyDate': f'{date}',
            'collectType': 'ETC',
            'dataType': '79',
            'collectCycle': '04',
            'supplyCycle': '01',
            'outFileName': f'vds_zone_{date}.gz',
        }
        vds_zone_file_name = get_ex_data_path(date, parent_path)['vds_zone_path']
        download_ex_data(vds_zone_file_name, vds_zone_post_fields)

        vds_point_post_fields = {
            'dataSupplyDate': f'{date}',
            'collectType': 'VDS',
            'dataType': '84',
            'collectCycle': '04',
            'supplyCycle': '01',
            'outFileName': f'vds_point_{date}.gz',
        }
        vds_point_file_name = get_ex_data_path(date, parent_path)['vds_point_path']
        download_ex_data(vds_point_file_name, vds_point_post_fields)

    print(f"end downloading ex data from {start_date} to {end_date}")