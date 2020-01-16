import itertools
import sys
import time
import os
import pandas as pd
import numpy as np
from threading import Thread

def scan_CSV(name,DataFrame):
	DataFrame = pd.read_csv(name)
def converting(f1,f2,DFC):
	if f1 == DFC['Date'] and f2 == DFC['ad_id']:
		return DFC['spend']
	
os.system('cls' if os.name == 'nt' else 'clear')
print("ВВедите имя файла с информацией о продукте и скачиваниях")
print("Пример: in_data_a.csv")
print("> ",end='')
CompFileName = input();
CompDF = pd.read_csv(CompFileName)
thread1 = Thread(target=scan_CSV, args=(CompFileName, CompDF,))
thread1.start()
while thread1.is_alive():
	os.system('cls' if os.name == 'nt' else 'clear')
	print("loading.")
	time.sleep(0.1)
	os.system('cls' if os.name == 'nt' else 'clear')
	print("loading..")
	time.sleep(0.1)
	os.system('cls' if os.name == 'nt' else 'clear')
	print("loading...")
	time.sleep(0.1)
os.system('cls' if os.name == 'nt' else 'clear')
thread1.join()
print("Файл загружен в программу\n\n")
print("ВВедите имя файла с информацией о стоимости рекламы")
print("Пример: in_data_p.csv")
print("> ",end='')
ValueFileName = input();
ValueDF = pd.read_csv(ValueFileName)
thread1 = Thread(target=scan_CSV, args=(ValueFileName, ValueDF,))
thread1.start()
while thread1.is_alive():
	os.system('cls' if os.name == 'nt' else 'clear')
	print("loading.")
	time.sleep(0.1)
	os.system('cls' if os.name == 'nt' else 'clear')
	print("loading..")
	time.sleep(0.1)
	os.system('cls' if os.name == 'nt' else 'clear')
	print("loading...")
	time.sleep(0.1)
os.system('cls' if os.name == 'nt' else 'clear')
thread1.join()
print("Файл загружен в программу\n\n")

CompDF = CompDF.drop(['id'], axis=1)
ValueDF = ValueDF.drop(['campaign'], axis=1)
ValueDF["Date"] = pd.to_datetime(ValueDF["Date"], errors="coerce")
CompDF["Date"] = pd.to_datetime(CompDF["Date"], errors="coerce")
ResultDF = CompDF
ResultDF = ResultDF.merge(ValueDF[['ad_id','Date','spend']],on=['ad_id','Date'],how='left')
ResultDF = ResultDF.drop(['ad_id'],axis=1)
ResultDF = ResultDF.groupby(['app','Date','Campaign','os'])['Installs','spend'].sum()
ResultDF['cpi'] = ResultDF['spend']/ResultDF['Installs']
ResultDF['cpi'] = pd.to_numeric(ResultDF['cpi'], errors='coerce')
ResultDF.style.hide_index()
ResultDF = ResultDF.fillna(0)
#ResultDF = ResultDF.replace(np.inf, 0)
ResultDF.to_csv('out.csv')
print(ResultDF.dtypes)
print(ResultDF)
