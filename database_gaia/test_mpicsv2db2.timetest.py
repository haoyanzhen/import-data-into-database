"""
用于测试导入整表和导入部分表的用时
created by hyz in 2023.7.14
"""


import pandas as pd
import os
import mysql.connector
import time
from mpi4py import MPI
from datetime import datetime
import numpy as np

start = time.time()

# 连接到MySQL数据库
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="xxxx",
  database="xxxx" ##
)

# 创建游标
mycursor = mydb.cursor()

# 设定数据类型为int、string和int
with open('./gaiams_database_datatype.txt','r') as dt:
    data_types = dt.read().split('\n')

with open ('./gaiams_columns_name.txt','r') as gcn:
    cols = gcn.read().split('\n')

column_defs = [f"{col} {data_types[i]} NULL" if col != 'dec' else f"`{col}` {data_types[i]} NULL" for i, col in enumerate(cols)]

mini_columns = ['source_id', 'ref_epoch', 'ra', 'ra_error', 'dec', 'dec_error', 'parallax', 'parallax_error', 'pm', 'pmra', 'pmra_error', 'pmdec', 'pmdec_error', 
                'phot_g_mean_mag', 'phot_bp_mean_mag', 'phot_rp_mean_mag', 'radial_velocity', 'radial_velocity_error', 'teff_gspphot', 'logg_gspphot', 'mh_gspphot', 'distance_gspphot']
mini_index = [cols.index(_mc) for _mc in mini_columns]

column_defs_mini = np.array(column_defs)[mini_index]
    
def mini(file):
    if file.endswith('.csv'):
        df = pd.read_csv(file,comment='#',usecols=mini_columns)
        df.to_csv(file[:-4]+'.mini'+file[-4:], index=False)
        
        

def process_csv(file):
    '''导入整表'''
    if file.endswith('.csv'):
        # 从文件名获取表名
        table_name = file.split('.')[0].split('/')[-1]
        table_name = table_name.replace("-", "_")
        
        columns = [f'{col}' if col != 'dec' else f'`{col}`' for col in cols]
        
        mycursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})")
        mydb.commit()
        
        sql = f"LOAD DATA INFILE '{file}' IGNORE INTO TABLE {table_name} FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1001 ROWS ({', '.join(columns)})"
        mycursor.execute(sql)
        mydb.commit()
        print('[%s] insert file succ. rank:%s file:%s  '%(str(datetime.now()),rank,table_name),time.time()-start)


def process_csv_mini(file):
    '''导入整表'''
    if file.endswith('.csv'):
        file = file[:-4]+'.mini'+file[-4:]
        
        table_name = file.split('.')[0].split('/')[-1]
        table_name = table_name.replace("-", "_")
        table_name = table_name.replace(".", "_")
        
        columns_mini = [f'{col}' if col != 'dec' else f'`{col}`' for col in mini_columns]
        
        mycursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs_mini)})")
        mydb.commit()
        
        sql = f"LOAD DATA INFILE '{file}' IGNORE INTO TABLE {table_name} FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS ({', '.join(columns_mini)})"
        mycursor.execute(sql)
        mydb.commit()
        print('[%s] insert file succ. rank:%s file:%s  '%(str(datetime.now()),rank,table_name),time.time()-start)



# 循环遍历目录中的所有csv文件
csv_folder = '/var/lib/mysql-files/gaiaDR3/ms/'
csv_filelist = os.listdir(csv_folder)
csv_filelist.sort()  # 此函数无返回值
with open('gaiams_insert_filelist','w') as f:
    f.write('\n'.join(csv_filelist))

csv_files = [os.path.join(csv_folder, f) for f in csv_filelist[24:] if f.endswith('.csv')][:24]  ##

# 并行处理csv文件
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

for i, file in enumerate(csv_files):
    if i % size == rank:
        t0 = time.time()
        mini(file)
        t1 = time.time()
        process_csv(file)
        t2 = time.time()
        process_csv_mini(file)
        t3 = time.time()
        t_mini  = t1-t0
        t_p     = t2-t1
        t_pmini = t3-t2
        print('rank: %s %s/%s'%(rank,i,len(csv_files)//24))
        print('time used: mini: %f\t process: %f\t process_mini: %f' % (t_mini,t_p,t_pmini))
        t_mini  = comm.gather(t_mini, root=0)
        t_p     = comm.gather(t_p, root=0)
        t_pmini = comm.gather(t_pmini, root=0)

# 关闭游标和数据库连接
mycursor.close()
mydb.close()

if rank == 0:
    t_mini  = np.array(t_mini).mean()
    t_p     = np.array(t_p).mean()
    t_pmini = np.array(t_pmini).mean()

print('time used average: mini: %f\t process: %f\t process_mini: %f' % (t_mini,t_p,t_pmini))
print('time total',time.time()-start)

# 数据库使用经验
# 表名不允许出现-
# 列名不允许直接出现dec，可用`dec`表示
# 如数据中存在nan，需转为’null‘并在创建表或创建表后允许表为空
# 在插入数据的时候，若包含NULL，格式应类似：“INSERT INTO tablename (*cols) VALUES (xxx,xxx,NULL,...)”。即去掉NULL周围的引号
