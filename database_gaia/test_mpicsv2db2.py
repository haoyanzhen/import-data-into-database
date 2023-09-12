import pandas as pd
import os
import mysql.connector
import time
from mpi4py import MPI
from datetime import datetime

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
    

# 循环遍历目录中的所有csv文件
def process_csv(file):
    if file.endswith('.csv'):
        df = pd.read_csv(file,comment='#')
        df.fillna('NULL', inplace=True)
        df.replace('null', 'NULL', inplace=True)

        # 从文件名获取表名
        table_name = file.split('.')[0].split('/')[-1]
        table_name = table_name.replace("-", "_")
        
        # 从csv文件的第一行获取列名，并将dec转化为`dec`以支持sql格式
        columns = [f'{col}' if col != 'dec' else f'`{col}`' for col in cols]
        
        # 在MySQL数据库中创建表
        # print(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})")
        mycursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})")
        mydb.commit()
        
        # 批量导入数据(前1001行为数据说明)
        sql = f"LOAD DATA INFILE '{file}' IGNORE INTO TABLE {table_name} FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1001 ROWS ({', '.join(columns)})"
        mycursor.execute(sql)
        mydb.commit()
        print('[%s] insert file succ. rank:%s file:%s  '%(str(datetime.now()),rank,table_name),time.time()-start)

# 循环遍历目录中的所有csv文件
csv_folder = '/var/lib/mysql-files/gaiaDR3/ms/'
csv_filelist = os.listdir(csv_folder)
csv_filelist.sort()  # 此函数无返回值
with open('gaiams_insert_filelist','w') as f:
    f.write('\n'.join(csv_filelist))

csv_files = [os.path.join(csv_folder, f) for f in csv_filelist[24:] if f.endswith('.csv')] 

# 并行处理csv文件
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

for i, file in enumerate(csv_files):
    if i % size == rank:
        process_csv(file)
        print('rank:%s %s/%s'%(rank,i,len(csv_files)//24))

'''if size == 1:
    for i,file in enumerate(csv_files):
        process_csv(file)
        print('process: %s/%s'%(i,len(csv_files)))
'''
# 关闭游标和数据库连接
mycursor.close()
mydb.close()

print(time.time()-start)

# 数据库使用经验
# 表名不允许出现-
# 列名不允许直接出现dec，可用`dec`表示
# 如数据中存在nan，需转为’null‘并在创建表或创建表后允许表为空
# 在插入数据的时候，若包含NULL，格式应类似：“INSERT INTO tablename (*cols) VALUES (xxx,xxx,NULL,...)”。即去掉NULL周围的引号
