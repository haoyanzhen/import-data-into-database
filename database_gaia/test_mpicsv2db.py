import pandas as pd
import os
import mysql.connector
import time
from mpi4py import MPI

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
        
        # 生成插入数据列表
        str_value_list = []
        for index, row in df.iterrows():
            values = tuple(row)
            # str_values = (f'{values}'.replace("'NULL'",'NULL'))
            str_value_list.append(f'{values}'.replace("'NULL'",'NULL'))
            # mycursor.execute(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES " + str_values)

        # 分批插入
        # print(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES " + ','.join(str_value_list))
        len_cat = len(df)
        # print(len_cat)
        del df
        for i in range((len_cat-1)//10000 +1):
            mycursor.execute(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES " + ','.join(str_value_list[i*10000:(i+1)*10000]))

            # 提交更改到数据库
            mydb.commit()
            print('insert bulk succ %s/%s'%(i,(len_cat-1)//10000))

        del str_value_list
        print('insert all succ. rank:%s file:%s  '%(rank,table_name),time.time()-start)

# 循环遍历目录中的所有csv文件
csv_folder = '/data/gaiaDR3/ms/'
csv_files = [os.path.join(csv_folder, f) for f in os.listdir(csv_folder) if f.endswith('.csv')]

# 并行处理csv文件
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

for i, file in enumerate(csv_files):
    if i % size == rank:
        process_csv(file)
        print('rank:%s %s/%s'%(rank,i,len(csv_files)//24))

# 关闭游标和数据库连接
mycursor.close()
mydb.close()

print(time.time()-start)

# 数据库使用经验
# 表名不允许出现-
# 列名不允许直接出现dec，可用`dec`表示
# 如数据中存在nan，需转为’null‘并在创建表或创建表后允许表为空
# 在插入数据的时候，若包含NULL，格式应类似：“INSERT INTO tablename (*cols) VALUES (xxx,xxx,NULL,...)”。即去掉NULL周围的引号
