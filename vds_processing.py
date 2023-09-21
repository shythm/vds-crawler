import requests
from requests_toolbelt import MultipartEncoder
import os
import gzip
import pandas as pd
import numpy as np
from datetime import datetime, time

# post mutlipart/form-data to get 5min data from ex data portal
def download_ex_data(file_name: str, fields: dict, verbose: bool) -> bytes:
    if os.path.isfile(file_name):
        if verbose:
            print(f"already {file_name} has been downloaded.")
    else:
        if verbose:
            print(f"start crawling {file_name}")

        if verbose:
            print(f"POST {file_name} ...")
        multipart = MultipartEncoder(fields=fields)
        headers = {
            'Content-Type': multipart.content_type,
        }
        res = requests.post('http://data.ex.co.kr/portal/fdwn/log', headers=headers, data=multipart)

        if verbose:
            print(f"decompessing {file_name} ...")
        data = gzip.decompress(res.content)

        if verbose:
            print(f"saving {file_name} ...")
        with open(file_name, 'wb') as f:
            f.write(data)

        if verbose:
            print(f"complete to download {file_name}")

# download original files from ex data portal
def prepare_original_files(date: str, verbose: bool = False) -> dict:
    parent_path = 'temp'

    # create parent_path if not exists
    if not os.path.isdir(parent_path):
        os.mkdir(parent_path)

    vds_data_post_fields = {
        'dataSupplyDate': f'{date}',
        'collectType': 'VDS',
        'dataType': '16',
        'collectCycle': '04',
        'supplyCycle': '01',
        'outFileName': f'vds_data_{date}.gz',
    }
    vds_data_file_name = f"{parent_path}/vds_data_{date}.csv"
    download_ex_data(vds_data_file_name, vds_data_post_fields, verbose)

    con_zone_post_fields = {
        'dataSupplyDate': f'{date}',
        'collectType': 'ETC',
        'dataType': '78',
        'collectCycle': '04',
        'supplyCycle': '01',
        'outFileName': f'con_zone_{date}.gz',
    }
    con_zone_file_name = f"{parent_path}/con_zone_{date}.csv"
    download_ex_data(con_zone_file_name, con_zone_post_fields, verbose)

    vds_zone_post_fields = {
        'dataSupplyDate': f'{date}',
        'collectType': 'ETC',
        'dataType': '79',
        'collectCycle': '04',
        'supplyCycle': '01',
        'outFileName': f'vds_zone_{date}.gz',
    }
    vds_zone_file_name = f"{parent_path}/vds_zone_{date}.csv"
    download_ex_data(vds_zone_file_name, vds_zone_post_fields, verbose)

    vds_point_post_fields = {
        'dataSupplyDate': f'{date}',
        'collectType': 'VDS',
        'dataType': '84',
        'collectCycle': '04',
        'supplyCycle': '01',
        'outFileName': f'vds_point_{date}.gz',
    }
    vds_point_file_name = f"{parent_path}/vds_point_{date}.csv"
    download_ex_data(vds_point_file_name, vds_point_post_fields, verbose)

    return {
        'vds_data_path': vds_data_file_name,
        'con_zone_path': con_zone_file_name,
        'vds_zone_path': vds_zone_file_name,
        'vds_point_path': vds_point_file_name,
    }

# extract delimiter of csv file using header
def extract_delimiter(path) -> str:
    with open(path, 'r', encoding='euc-kr') as f:
        if '|' in f.readline():
            return '|'
        else:
            return ','

# aggregate grouped vds_data_df and select columns
def aggregate_vds_data(grouped_df: pd.DataFrame) -> pd.Series:
    traffic = -1
    speed = -1
    share = -1

    # remove rows whose 교통량 is -1
    df = grouped_df[grouped_df['교통량'] != -1]

    if len(df) != 0:
        traffic = np.sum(df['교통량']) # 교통량의 합을 구해 검지기를 지난 차량의 수를 구한다.
        
        if traffic != 0:
            speed_sum = np.inner(df['평균속도'], df['교통량']) # 각 데이터에 대한 속도의 누적합을 구한다.
            speed = speed_sum / traffic # 평균속도를 구한다.
            share = np.mean(df[df['점유율'] != 0]['점유율']) # 점유율의 평균을 구한다.
        else:
            speed = 0
            share = 0

        
        # 점유율의 평균을 구하는데, 점유율이 0인 데이터는 제외한다. 그러나 점유율이 0이 아닌 데이터가 하나도 없으면 평균을 0으로 한다.


    return pd.Series({
        '도로번호': grouped_df['노선번호'].iloc[0],
        '도로명': grouped_df['도로명'].iloc[0],
        '콘존ID': grouped_df['콘존ID'].iloc[0],
        '구간명': grouped_df['콘존명'].iloc[0],
        '구간길이(m)': grouped_df['콘존길이'].iloc[0],
        '기점종점방향구분코드': grouped_df['기점종점방향구분코드'].iloc[0],
        # 집계일자
        # 집계시분
        # VDS_ID
        # 차로유형구분코드
        '교통량': traffic,
        '점유율': share,
        '평균속도': speed,
    })

# process vds data
def process_vds(date: str, holiday_code: int = 0, verbose: bool = False) -> None:
    print(f"start processing vds data of {date}")

    # download files from ex data portal
    if verbose:
        print(f"downloading original files of {date} ...")
    paths = prepare_original_files(date, verbose=verbose)

    # load original files
    if verbose:
        print(f"loading original files of {date} ...")

    # load con_zone data which columns is
    # (콘존ID, 콘존길이, 기점종점방향구분코드, 시작노드ID, 종료노드ID, 차로수, 노선번호, 제한속도, 노선구성순번, 콘존명, 버스전용차로유무, 도로등급구분코드)
    con_zone_df = pd.read_csv(
        paths['con_zone_path'],
        delimiter=extract_delimiter(paths['con_zone_path']),
        encoding='euc-kr',
        usecols=[0, 1, 2, 9],
        dtype={
            '콘존ID': str,
            '콘존길이': float,
            '기점종점방향구분코드': str,
            '콘존명': str,
        },
    )

    # load vds_zone data which columns is
    # (VDS_ID, 지점이정, VDS존시작이정, VDS존종료이정, 노선번호, VDS존유형구분코드, 노선구성순번, 기점종점방향구분코드, VDS존길이, 도로등급구분코드, 콘존ID)
    vds_zone_df = pd.read_csv(
        paths['vds_zone_path'],
        delimiter=extract_delimiter(paths['vds_zone_path']),
        encoding='euc-kr',
        usecols=[0, 2, 3, 4, 10],
        dtype={
            'VDS_ID': str,
            '노선번호': int,
            '콘존ID': str,
            'VDS존시작이정': float,
            'VDS존종료이정': float,
        },
    )

    # get dictionary whose key is 노선번호 and value is 도로명
    # (기준시간, 기준시, 기준일, VDS_ID, 요일명, 지점이정, 노드명, 도로이정, 노선번호, 도로명, 교통량, 평균속도)
    vds_point_df = pd.read_csv(
        paths['vds_point_path'],
        delimiter=extract_delimiter(paths['vds_point_path']),
        encoding='euc-kr',
        usecols=[8, 9],
    )
    road_name_dict = vds_point_df.set_index('노선번호').to_dict()['도로명']

    # add 도로명 column to vds_zone_df
    vds_zone_df['도로명'] = vds_zone_df['노선번호'].map(road_name_dict)

    # load vds_data data which columns is
    # (수집일자, 수집시분초, VDS_ID, 지점이정, 도로이정, 점유율, 평균속도, 교통량, 차로번호, 콘존ID, 콘존명, 콘존길이)
    vds_data_df = pd.read_csv(
        paths['vds_data_path'],
        delimiter=extract_delimiter(paths['vds_data_path']),
        encoding='euc-kr',
        usecols=[1, 2, 5, 6, 7, 8],
        dtype={
            '수집시분초': int, # query의 편의를 위해 int로 변환
            'VDS_ID': str,
            '점유율': float,
            '평균속도': float,
            '교통량': int,
            '차로번호': int,
        },
    )

    if verbose:
        print(f"preprocessing vds data of {date} ...")

    # left join vds_data_df and vds_zone_df
    vds_data_df = vds_data_df.merge(
        vds_zone_df, on='VDS_ID', how='left'
    ).merge(
        con_zone_df, on='콘존ID', how='left'
    )

    if holiday_code == 0: # case of 평일
        bus_road_query = '(차로번호 == 1) and (노선번호 == 10 and VDS존종료이정 >= 376.4 and VDS존시작이정 <= 423.0) and (수집시분초 >= 70000 and 수집시분초 <= 210000)'
    else:
        bus_road_query = '(차로번호 == 1) and ((노선번호 == 10 and VDS존종료이정 >= 282.0 and VDS존시작이정 <= 423.0) or (노선번호 == 500 and VDS존종료이정 >= 43.6 and VDS존시작이정 <= 70.5))'
        if holiday_code == 1: # 공휴일인 경우
            bus_road_query += 'and (수집시분초 >= 70000 and 수집시분초 <= 210000)'
        elif holiday_code == 2: # 설날/추석연휴 전날
            bus_road_query += 'and (수집시분초 >= 70000 and 수집시분초 < 240000)'
        elif holiday_code == 3: # 설날/추석연휴
            bus_road_query += 'and ((수집시분초 > 0 and 수집시분초 <= 10000) or (수집시분초 >= 70000 and 수집시분초 < 240000))'
        elif holiday_code == 4: # 설날/추석연휴 다음날
            bus_road_query += 'and (수집시분초 > 0 and 수집시분초 <= 10000)'

    # get bus road indexes
    bus_road_indexes = vds_data_df.query(bus_road_query).index

    # add 차로유형구분코드 column to vds_data_df
    road_type_column = np.ones(len(vds_data_df), dtype=int)
    road_type_column[bus_road_indexes] = 1
    vds_data_df['차로유형구분코드'] = road_type_column

    # add 집계일시분초 column to vds_data_df to group by 집계일시분초
    datetime_column = []
    date_obj = pd.to_datetime(date, format='%Y%m%d').date()

    for t in vds_data_df['수집시분초']:
        _t = t
        second = _t % 100
        _t //= 100
        minute = _t % 100
        _t //= 100
        hour = _t

        datetime_column.append(datetime.combine(date_obj, time(hour, minute, second)))

    vds_data_df['집계일시분초'] = datetime_column

    if verbose:
        print(f"grouping and aggregating vds data of {date} ...")

    # group by 'VDS_ID', '차로유형구분코드', 5 min of '집계일시분초'
    vds_data_grouped = vds_data_df.groupby(['VDS_ID', '차로유형구분코드', pd.Grouper(key='집계일시분초', freq='5min')])
    
    # aggregate vds_data_grouped
    vds_data_aggregated = vds_data_grouped.apply(aggregate_vds_data).reset_index()

    # add 집계일자, 집계시분 columns to vds_data_aggregated
    vds_data_aggregated['집계일자'] = date
    vds_data_aggregated['집계시분'] = vds_data_aggregated['집계일시분초'].dt.strftime('%H%M')

    # add empty column
    vds_data_aggregated[''] = ''

    if verbose:
        print(f"saving vds data of {date} ...")

    vds_data_output = vds_data_aggregated[[
        '도로번호',
        '도로명',
        '콘존ID',
        '구간명',
        '구간길이(m)',
        '기점종점방향구분코드',
        '집계일자',
        '집계시분',
        'VDS_ID',
        '차로유형구분코드',
        '교통량',
        '점유율',
        '평균속도',
        '',
    ]]

    # create output directory if not exists
    if not os.path.isdir('output'):
        os.mkdir('output')

    # save vds_data_output to csv file
    vds_data_output.to_csv(
        f"output/vds_data_5min_{date}.csv",
        index=False,
        encoding='euc-kr',
    )

    print(f"complete to process vds data of {date} !")
