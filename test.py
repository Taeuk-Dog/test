import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from area_codes import AREA_CODES
import os

# API 키 설정
API_KEY = "745161584c786f6437346e4b704d41"

# 데이터 저장 폴더 생성
DATA_DIR = "유동인구데이터"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def get_population_stats(max_retries=3, retry_delay=5):
    """실시간 인구 통계 데이터를 가져오는 함수"""
    base_url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/json/citydata_ppltn"
    
    all_data = []
    for area_name, area_code in AREA_CODES.items():
        retries = 0
        while retries < max_retries:
            try:
                url = f"{base_url}/1/5/{area_code}"
                response = requests.get(url)
                data = response.json()
                
                if 'SeoulRtd.citydata_ppltn' in data and len(data['SeoulRtd.citydata_ppltn']) > 0:
                    row = data['SeoulRtd.citydata_ppltn'][0]
                    processed_row = {
                        '지역코드': row['AREA_CD'],
                        '지역명': row['AREA_NM'],
                        '데이터 수집 시간': row['PPLTN_TIME'],
                        '실시간 인구 수준': row['AREA_CONGEST_LVL'],
                        '실시간 인구 메시지': row['AREA_CONGEST_MSG'],
                        '인구 최소값': row['AREA_PPLTN_MIN'],
                        '인구 최대값': row['AREA_PPLTN_MAX'],
                        '남성 비율': row['MALE_PPLTN_RATE'],
                        '여성 비율': row['FEMALE_PPLTN_RATE'],
                        '0-9세 비율': row['PPLTN_RATE_0'],
                        '10대 비율': row['PPLTN_RATE_10'],
                        '20대 비율': row['PPLTN_RATE_20'],
                        '30대 비율': row['PPLTN_RATE_30'],
                        '40대 비율': row['PPLTN_RATE_40'],
                        '50대 비율': row['PPLTN_RATE_50'],
                        '60대 비율': row['PPLTN_RATE_60'],
                        '70대 이상 비율': row['PPLTN_RATE_70'],
                        '거주인구 비율': row['RESNT_PPLTN_RATE'],
                        '비거주인구 비율': row['NON_RESNT_PPLTN_RATE']
                    }
                    all_data.append(processed_row)
                    print(f"{area_name} 데이터 수집 완료")
                    break  # 성공하면 다음 지역으로
                else:
                    print(f"{area_name} 데이터 없음 (시도 {retries + 1}/{max_retries})")
                    retries += 1
                    if retries < max_retries:
                        print(f"{retry_delay}초 후 재시도...")
                        time.sleep(retry_delay)
                    
            except Exception as e:
                print(f"{area_name} 데이터 수집 실패: {e} (시도 {retries + 1}/{max_retries})")
                retries += 1
                if retries < max_retries:
                    print(f"{retry_delay}초 후 재시도...")
                    time.sleep(retry_delay)
                continue
            
    return pd.DataFrame(all_data) if all_data else None

def save_to_csv(df):
    """데이터프레임을 CSV 파일로 저장하는 함수"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    filename = f"population_stats_{timestamp}.csv"
    # 파일 경로를 유동인구데이터 폴더 내부로 설정
    filepath = os.path.join(DATA_DIR, filename)
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    print(f"파일 저장 완료: {filepath}")

def main():
    while True:
        current_time = datetime.now()
        print(f"\n데이터 수집 시작: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        df = get_population_stats()
        retries = 0
        max_retries = 3
        
        while df is None and retries < max_retries:
            retries += 1
            print(f"\n전체 데이터 수집 실패. {retries}/{max_retries} 번째 재시도...")
            time.sleep(10)  # 10초 대기 후 재시도
            df = get_population_stats()
        
        if df is not None:
            save_to_csv(df)
            print("\n=== 데이터 수집 결과 ===")
            print(f"수집된 지역 수: {len(df)}")
            print("\n지역별 실시간 인구 현황:")
            summary = df[['지역명', '실시간 인구 수준', '인구 최소값', '인구 최대값']].head()
            print(summary)
            print("\n데이터 수집 완료")
        else:
            print("데든 재시도 실패")
        
        # 다음 30분까지 대기
        next_run = current_time + timedelta(minutes=30)
        next_run = next_run.replace(minute=(next_run.minute // 30) * 30, second=0, microsecond=0)
        sleep_seconds = (next_run - datetime.now()).total_seconds()
        
        if sleep_seconds > 0:
            print(f"다음 수집 시간: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"대기 시간: {int(sleep_seconds)}초")
            time.sleep(sleep_seconds)

if __name__ == "__main__":
    main()
