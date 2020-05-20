# -*- coding: utf-8 -*-
"""
Created on Sun May 17 13:22:52 2020

@author: jinlo
"""

#%% import
import pandas as pd
import numpy as np
import os

    files_alarm = pd.read_csv("C:\\Users\\jinlo\\Desktop\\LDCC\\chilsung\\alarm\\ALARM_20200417_21.csv")
files_alarm.head(20)
files_alarm['datatag_name'].head(10)

files_alarm



#%% 빈도 분석 

import psycopg2

files_alarm.groupby('line_cd').count()


# 중복 컬럼 빼기
files_alarm["sensor"] = files_alarm['datatag_name'].apply(lambda x:x.split(".")[-1])
files_alarm["line_cd"] = files_alarm['datatag_name'].apply(lambda x:x.split(".")[2])
files_alarm["fac_cd"] = files_alarm['datatag_name'].apply(lambda x:x.split(".")[4])

files_alarm.head(10)


#csc, FILL 만 분리, on/off만 분리하기. value=0,1,2,3,4도 있음.. 애들은.. 오류라 생각하
csc_data=files_alarm[files_alarm['line_cd']=='CSC']
csc_FIL_data=csc_data[csc_data['fac_cd']=='FIL']
csc_FIL_data=csc_FIL_data[(csc_FIL_data['datatag_value']=='On') | (csc_FIL_data['datatag_value']=='Off')]
csc_FIL_data.head(10)
csc_FIL_data.count()
csc_data.columns

csc_FIL_data.groupby('sensor').count()
csc_FIL_data.head(10)



#3/23~3/27기준으로 데이터 추출
import datetime

#print(csc_data['otime'].strftime('%Y-%m-%d'))
#csc_data['date']=datetime.datetime.strftime(csc_data['otime'],'%Y-%m-%d')

csc_test=csc_FIL_data[csc_FIL_data['otime']<'2020-03-28 00:00:00']

print(min(csc_test['otime']))

csc_test


#%% seonser 별로 분리하기 
FIL_DB186_DBX18_6.head(10)
test2.groupby('sensor').count()



FIL_DB186_DBX18_6=csc_test[csc_test['sensor']=='DB186_DBX18_6']

# on 1, off 0 으로 대체
import re

test2=FIL_DB186_DBX18_6
test2['datatag_value']=np.where(test2['datatag_value']=='On',1,0)

test2.head(10)

#index 만들기
test2[7588]




FIL_DB186_DBX18_6.groupby('sensor').count()
FIL_DB186_DBX18_6.head(10)

csc_data.groupby('datatag_value').count()

check_sensor_by_time = pd.DataFrame(alarm_data.groupby(["otime"])["message"].apply(list))

# Alarm 데이터 컬럼화
for idx in FIL_alarm_msg :
    check_sensor_by_time[idx] = check_sensor_by_time["sensor"].apply(lambda x : np.where(idx in x, 1, 0))
    
alarm_data.head(10)    


#%% 다중조건으로 정렬하기 


csc_FIL_data.head(10)
csc_FIL_data.shape
csc_FIL_data2=csc_FIL_data.sort_values(['sensor','otime'], ascending=(True, True))
csc_FIL_data3=csc_FIL_data2.reset_index(drop=True)   # index를 0부터 다시 초기화함. 

#%%  del_time 을 넣을 새로운 컬럼 생성
import numpy as np

csc_FIL_data3['del_time']=np.nan
csc_FIL_data3.head(100)
csc_FIL_data3['otime']=pd.to_datetime(csc_FIL_data3['otime'])

for i in range(len(csc_FIL_data3.index)-1):
    if csc_FIL_data3.datatag_value[i] =='On' and csc_FIL_data3.datatag_value[i+1] =='Off' and csc_FIL_data3.datatag_name[i] ==csc_FIL_data3.datatag_name[i+1]:
        csc_FIL_data3.del_time[i] = (csc_FIL_data3.otime[i+1] - csc_FIL_data3.otime[i]).total_seconds()
    
    
csc_FIL_data4=csc_FIL_data3[csc_FIL_data3['datatag_value']=='On']
csc_FIL_data4.head(10)
