import pandas as pd
import os
import mysql.connector
import time
from mpi4py import MPI
from datetime import datetime
import numpy as np


# 连接数据库并读取表
class CSV2DB(object):
    def __init__(self, db):
        # super().__init__(self)
        
        self.HOME = '/var/lib/mysql-files/catalog2healpix/'
        database = db.upper() + '_hpix8'
        
        # connection
        self.mydb = mysql.connector.connect(
        host="xxxx",
        user="root",
        password="xxxx",
        database=database
        )
        self.mycursor = self.mydb.cursor()

        # datatype & names
        self.db = db
        self.datatypes   = np.load(self.HOME+'/datatype_%s.npy'%db,     allow_pickle=True)
        self.names       = np.load(self.HOME+'/column_names_%s.npy'%db, allow_pickle=True)
        self.column_defs = [f"{col}   {self.datatypes[i]} NULL"       if col != 'dec' else
                            f"`{col}` {self.datatypes[i]} NULL" 
                            for i, col in enumerate(self.names)]
        print('initial')
        
        # process
        self.process_main(db)
        
        self.mycursor.close()
        self.mydb.close()
    
    def process_main(self, db):
        # get file list
        csv_folder   = self.HOME + db.upper() + '_hpix8/'
        csv_filelist = os.listdir(csv_folder)
        csv_filelist.sort()  # 此函数无返回值
        csv_files    = [os.path.join(csv_folder, f) for f in csv_filelist if f.endswith('.csv')] 
        
        # MPI
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()
        print('main')
        
        ## exists table check
        self.mycursor.execute("SHOW TABLES;")
        exists_list = self.mycursor.fetchall()
        self.exists_list = np.array(exists_list).reshape(-1)

        # process for single csv in mysql
        for i, file in enumerate(csv_files):
            if i % self.size == self.rank:
                self.process_single_csv(file)
                print('rank:%s %s/%s'%(self.rank,i,len(csv_files)))

    def process_single_csv(self, file):
        if file.endswith('.csv'):
            # get table name
            table_name = os.path.split(file)[1][:-4]
            table_name = table_name.replace("-", "_")
            
            # get column names
            columns = [f'{col}' if col != 'dec' else f'`{col}`' for col in self.names]
            
            # drop table with problem
            if self.db == 'gsc24' and  table_name in self.exists_list:
                self.mycursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                exists_len = self.mycursor.fetchall()[0][0]
                with open(file,'r') as f:
                    table_len = len(f.readlines())-1
                if exists_len != table_len:
                    print(f'DROP::{table_name}')
                    self.mycursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                else:
                    print(f"pass::{table_name}")
                    return
                    
            # creat table in mysql
            # print(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})")
            self.mycursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(self.column_defs)})")
            self.mydb.commit()
            
            # load infile in mysql (ignore column line)
            # print(file)
            load_sql = f"LOAD DATA INFILE '{file}' IGNORE INTO TABLE {table_name} FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS ({', '.join(columns)})"
            self.mycursor.execute(load_sql)
            self.mydb.commit()
            
            print('[%s] insert file succ. rank:%s file:%s  '%(str(datetime.now()),self.rank,table_name),time.time()-start)
        

if __name__ == '__main__':
    start   = time.time()

    HOME    = '/var/lib/mysql-files/catalog2healpix/'
    # db_list = ['gsc22','gsc23','gsc24','ucac','ucac2','ucac3','ucac4','ucac5']
    db_list = ['ucac','ucac2','ucac3','ucac4']
    
    for db in db_list:
        CSV2DB(db)

    print('==end  ', time.time()-start)

# 数据库使用经验
# 表名不允许出现-
# 列名不允许直接出现dec，可用`dec`表示
# 如数据中存在nan，需转为’null‘并在创建表或创建表后允许表为空
# 在插入数据的时候，若包含NULL，格式应类似：“INSERT INTO tablename (*cols) VALUES (xxx,xxx,NULL,...)”。即去掉NULL周围的引号
