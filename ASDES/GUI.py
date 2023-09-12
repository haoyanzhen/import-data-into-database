"""用于天文时空转换计算、天文数据提取的应用程序
    包含Gaia, GSC, UCAC星表
    created by hyz in 2023.07.13
    update v2 by hyz in 2023.07.20
"""


import tkinter as tk
from tkinter.ttk import Notebook
import time
from astropy.time import Time
from astropy.coordinates import SkyCoord
from astropy import units as u
import os
import pandas as pd
import mysql.connector
import healpy as hp
import numpy as np
# import matplotlib as plt


class Root(tk.Tk):
    def __init__(self):
        super().__init__()
        self.HOME = './'
        
        self.title("ASDE Syetem v2.0")
        self.geometry("750x500")
        
        # initial mysql
        self.mydb = None
        self.mycursor = None
        
        # 欢迎界面
        # self.welcome = tk.Label(self, text="Welcome to ASDE\nAstronomy Spherical Data EXtraction System\n晟昱翔鹏（上海）科技有限公司", 
        #                         bg="cornflowerblue", fg="dimgray", padx=10, pady=10)
        self.welcome = tk.Label(self, text="Welcome to ASDE\nAstronomy Spherical Data EXtraction System\n内测版", 
                                bg="cornflowerblue", fg="dimgray", padx=10, pady=10)
        self.welcome.pack()
        self.bind("<Key>", self.end_welcome)
        
    def end_welcome(self, event=None):
        self.welcome.pack_forget()
        self.unbind("<Key>")
        self.main()
        
    def main(self):
        # 创建分页
        self.notebook = Notebook(self)
        f_astro = tk.Frame(self.notebook)
        f_db    = tk.Frame(self.notebook)
        
        # astro分区的经纬度转换
        self.astro_ra = tk.Entry(f_astro, bg='white', fg='black', width=10)
        self.astro_dec = tk.Entry(f_astro, bg='white', fg='black', width=10)
        self.astro_dis = tk.Entry(f_astro, bg='white', fg='black', width=10)
        # self.astro_coorl = ['ICRS','FK5','Galactic','GCRS','ITRS','GeocentricTrueEcliptic','BarycentricTrueEcliptic','HeliocentricTrueEcliptic','AltAz','HADec']
        self.astro_coorl = ['ICRS','FK5','Galactic','GCRS','ITRS','GeocentricTrueEcliptic','BarycentricTrueEcliptic','HeliocentricTrueEcliptic']
        self.astro_coor_fromv = tk.StringVar(f_astro)
        self.astro_coor_from = tk.ttk.OptionMenu(f_astro,self.astro_coor_fromv,'(coordinate system)',*self.astro_coorl)
        self.astro_coor_tov = tk.StringVar(f_astro)
        self.astro_coor_to = tk.ttk.OptionMenu(f_astro,self.astro_coor_tov,'(coordinate)',*self.astro_coorl)
        self.astro_coor_button = tk.Button(f_astro, text='坐标转换', command=self.astro_coor)
        
        tk.Label(f_astro, text='ra: ').grid(row=0,column=0)
        self.astro_ra.grid(row=0,column=1)
        tk.Label(f_astro,text='(deg)').grid(row=0,column=2)
        tk.Label(f_astro,text='dec: ').grid(row=0,column=3)
        self.astro_dec.grid(row=0,column=4)
        tk.Label(f_astro,text='(deg)').grid(row=0,column=5)
        tk.Label(f_astro,text='distance: ').grid(row=0,column=6)
        self.astro_dis.grid(row=0,column=7)
        tk.Label(f_astro,text='(km)').grid(row=0,column=8)
        tk.Label(f_astro,text='from: ').grid(row=1,column=0)
        self.astro_coor_from.grid(row=1,column=1,columnspan=3)
        tk.Label(f_astro,text='to: ').grid(row=1,column=4)
        self.astro_coor_to.grid(row=1,column=5,columnspan=3)
        self.astro_coor_button.grid(row=1,column=8)
        
        # 分割线
        row_splitline = 2
        split_line = tk.Label(f_astro, text='-'*100)
        split_line.grid(row=row_splitline,columnspan=9,pady=1)
        
        # astro分区的时间转换
        row_time = 3
        self.astro_time_t = tk.Entry(f_astro, bg='white', fg='black', width=10)
        self.timescalel = ['tai', 'tcb', 'tcg', 'tdb', 'tt', 'ut1', 'utc', 'local']
        self.timeformatl = ['byear', 'cxcsec', 'datetime', 'decimalyear', 'fits', 'gps', 'iso', 'isot', 'jd', 'jyear', 'mjd', 'plot_date', 'ymdhms']
        self.astro_timescale_fromv, self.astro_timescale_tov, self.astro_timeformat_fromv, self.astro_timeformat_tov = [tk.StringVar() for i in range(4)]
        self.astro_timescale_from = tk.ttk.OptionMenu(f_astro, self.astro_timescale_fromv,'(scale)',*self.timescalel)
        self.astro_timescale_to = tk.ttk.OptionMenu(f_astro, self.astro_timescale_tov,'(scale)',*self.timescalel)
        self.astro_timeformat_from = tk.ttk.OptionMenu(f_astro, self.astro_timeformat_fromv,'(format)',*self.timeformatl)
        self.astro_timeformat_to = tk.ttk.OptionMenu(f_astro, self.astro_timeformat_tov,'(format)',*self.timeformatl)
        self.astro_time_button = tk.Button(f_astro, text='时间转换', command=self.astro_time)
        
        tk.Label(f_astro, text='time: ').grid(row=row_time,column=0)
        self.astro_time_t.grid(row=row_time,column=1,columnspan=2)
        tk.Label(f_astro, text='from: ').grid(row=row_time,column=3)
        self.astro_timescale_from.grid(row=row_time,column=4,columnspan=2)
        self.astro_timeformat_from.grid(row=row_time,column=6,columnspan=2)
        tk.Label(f_astro, text='to: ').grid(row=row_time+1,column=3)
        self.astro_timescale_to.grid(row=row_time+1,column=4,columnspan=2)
        self.astro_timeformat_to.grid(row=row_time+1,column=6,columnspan=2)
        self.astro_time_button.grid(row=row_time+1,column=8)
        
        # 分割线
        row_splitline = 5
        split_line = tk.Label(f_astro, text='-'*100)
        split_line.grid(row=row_splitline,columnspan=9,pady=1)
        
        # astro分区的结果显示
        row_result = 6
        self.astro_result = tk.Text(f_astro, bg='white', fg='black', height=20, width=80)
        self.astro_result.grid(row=row_result,column=0,rowspan=3,columnspan=9)
        
        # database分区
        # database 数据库选择
        self.db_listl = ['gaia dr3','gsc22','gsc23','gsc24','ucac','ucac2','ucac3','ucac4','ucac5']
        self.db_listv = tk.StringVar()
        self.db_list  = tk.ttk.OptionMenu(f_db, self.db_listv, '(database choose)', *self.db_listl)
        self.db_ifoutl = ['<server>','local', 'no']
        self.db_ifoutv = tk.StringVar()
        self.db_ifout  = tk.ttk.OptionMenu(f_db, self.db_ifoutv, '(if output file)', *self.db_ifoutl)
        self.db_hpixmapb = tk.Button(f_db,text='hpix map' ,command=self.db_hpixmap)
        self.db_statb    = tk.Button(f_db,text='statement',command=self.db_stat)
        self.db_detailb  = tk.Button(f_db,text='advance'  ,command=self.db_detail)
        self.db_list.grid(    row=0,column=0,columnspan=2,sticky='we')
        self.db_ifout.grid(   row=0,column=3,columnspan=2,sticky='Ewe')
        self.db_hpixmapb.grid(row=0,column=5,columnspan=2,sticky='we')
        self.db_statb.grid(   row=0,column=7,columnspan=2,sticky='we')
        self.db_detailb.grid( row=0,column=9,columnspan=2,sticky='we')
        
        # database query
        self.db_areashapel = ['circle','polygon','strip']
        self.db_areashapev = tk.StringVar()
        self.db_areashape  = tk.ttk.OptionMenu(f_db, self.db_areashapev,'(area shape)',*self.db_areashapel)
        self.db_ra  = tk.Entry(f_db, bg='white', fg='black', width=10)
        self.db_dec = tk.Entry(f_db, bg='white', fg='black', width=10)
        self.db_rdu = tk.Entry(f_db, bg='white', fg='black', width=10)
        self.db_submit = tk.Button(f_db, text='submit', command=self.db_ex)
        
        self.db_areashape.grid(row=1,column=0,sticky='we')
        tk.Label(f_db, text='ra:  ').grid(row=1,column=1)
        self.db_ra.grid( row=1,column=2,columnspan=2)
        tk.Label(f_db, text='dec: ').grid(row=1,column=4)
        self.db_dec.grid(row=1,column=5,columnspan=2)
        tk.Label(f_db, text='r:   ').grid(row=1,column=7)
        self.db_rdu.grid(row=1,column=8,columnspan=2)
        self.db_submit.grid(row=1,column=10)
        
        # database introduce, process and output
        self.db_introbox   = tk.Text(f_db, bg='white', fg='black', height=14, width=80)
        self.db_introbox.grid(row=2,columnspan=11)
        db_introbox_sv = tk.Scrollbar(f_db,orient='vertical')
        db_introbox_sv.grid(row=2,column=11, sticky='Ens')
        db_introbox_sv.config(command=self.db_introbox.yview)
        self.db_introbox.config(yscrollcommand=db_introbox_sv.set)
        self.db_intro()
        # split line
        tk.Label(f_db, text='PROCESS:').grid(row=3,columnspan=11,sticky='W')
        self.db_processbox = tk.Text(f_db, bg='white', fg='black', height=10, width=80)
        self.db_processbox.grid(row=4,columnspan=11)
        db_processbox_sv = tk.Scrollbar(f_db,orient='vertical',relief='groove')
        db_processbox_sv.grid(row=4,column=11, sticky='Ens')
        db_processbox_sv.config(command=self.db_processbox.yview)
        self.db_processbox.config(yscrollcommand=db_processbox_sv.set)
        self.db_outputb    = tk.Button(f_db, text='check data', command=self.db_output)
        self.db_outputb.grid(row=5,columnspan=11)
        
        # notebook搭建
        self.notebook.add(f_astro, text='astro')
        self.notebook.add(f_db, text='database')
        self.notebook.pack(fill=tk.BOTH, expand=1)
            
    def astro_coor(self):
        self.astro_result.insert('insert','\ncoordinate ----------\n')
        try:
            ra  = float(self.astro_ra.get())
            dec = float(self.astro_dec.get())
            dis = self.astro_dis.get()
            if dis:
                dis = float(dis)
            coor_from = self.astro_coor_fromv.get().lower()
            coor_to = self.astro_coor_tov.get().lower()
            if coor_from == 'galactic':
                c = SkyCoord(l=ra*u.degree, b=dec*u.degree, frame=coor_from)
            elif coor_from.endswith('ecliptic'):
                c = SkyCoord(lon=ra*u.degree, lat=dec*u.degree, distance=dis*u.km, frame=coor_from)
            elif coor_from in ['itrs']:
                c = SkyCoord(x=ra, y=dec, z=dis, frame=coor_from)
            else:
                c = SkyCoord(ra=ra*u.degree, dec=dec*u.degree, frame=coor_from)
            self.astro_result.insert('insert',c.transform_to(coor_to))
        except Exception as e:
            self.astro_result.insert('insert',e)
        return
    
    def astro_time(self):
        self.astro_result.insert('insert','\ntime ----------\n')
        try:
            t0 = self.astro_time_t.get()
            scale_from = self.astro_timescale_fromv.get()
            scale_to = self.astro_timescale_tov.get()
            format_from = self.astro_timeformat_fromv.get()
            format_to = self.astro_timeformat_tov.get()
            print(scale_from,scale_to,format_from,format_to)
            t = Time(t0, format=format_from,scale=scale_from)

            # self.timescalel = ['tai', 'tcb', 'tcg', 'tdb', 'tt', 'ut1', 'utc', 'local']
            # self.timeformatl = ['byear', 'cxcsec', 'datetime', 'decimalyear', 'fits', 'gps', 
            #                     'iso', 'isot', 'jd', 'jyear', 'mjd', 'plot_date', 'ymdhms']
            t_scale = {'tai': t.tai, 'tcb': t.tcb, 'tcg': t.tcg, 'tdb': t.tdb, 'tt':  t.tt, 'ut1': t.ut1, 'utc': t.utc}
            t = t_scale[scale_to]
            t_format = {'byear': t.byear, 'cxcsec': t.cxcsec, 'datetime':t.datetime, 'decimalyear':t.decimalyear, 
                                'fits':t.fits, 'gps':t.gps, 'iso':t.iso, 'isot':t.isot, 'jd':t.jd, 'jyear':t.jyear, 
                                'mjd':t.mjd, 'plot_date':t.plot_date, 'ymdhms':t.ymdhms}
            t = t_format[format_to]
            self.astro_result.insert('insert', t)
        except Exception as e:
            self.astro_result.insert('insert',e)
        return
    
    def db_ex(self):
        # connection
        self.db_processbox.insert('insert','-'*60+'\n>>new request submit received\n')
        self.db_processbox.insert('insert','>>connecting...\n')
        db = self.db_listv.get()
        database = ("Gaia_DR3_MS" if db=='gaia dr3' else db.upper()+'_hpix8')
        print('database: ',database)
        self.mydb = mysql.connector.connect(
                        host="119.78.226.27",
                        user="root",
                        password="shao1234",
                        database=database
                        )
        self.mycursor = self.mydb.cursor()
        self.db_processbox.insert('insert','>>connect succ\n')
        self.db_processbox.insert('insert','-*-'*10+'\n')

        # hpix analyze
        self.db_processbox.insert('insert','>>hpix analyzing...\n')
        try:
            ra  = float(self.db_ra.get())
            dec = float(self.db_dec.get())
            rdu = float(self.db_rdu.get())
            areashape = self.db_areashapev.get()
            if areashape not in self.db_areashapel:
                areashape = 'circle'
        except Exception:
            self.db_processbox.insert('insert', Exception+'\n')
        
        nside = (256 if db=='gaia dr3' else 8)
        print('>>nside:\t\t', nside)
        self.db_processbox.insert('insert','>>hpix nside:%d\n' % nside)

        if areashape == 'circle':
            target_pix = hp.query_disc(nside, hp.pixelfunc.ang2vec(ra,dec,lonlat=True), np.radians(rdu), nest=True)
        elif areashape == 'polygon':
            _lefttop    = [ra-rdu, dec+rdu]
            _righttop   = [ra+rdu, dec+rdu]
            _leftbottom = [ra-rdu, dec-rdu]
            _rightbottom= [ra+rdu, dec-rdu]
            vertices = np.array([_lefttop, _righttop, _rightbottom, _leftbottom])
            vertices   = hp.ang2vec(vertices[:,0],vertices[:,1],lonlat=True)
            target_pix = hp.query_polygon(nside, vertices, nest=True)  #TODO
        elif areashape == 'stripe':
            target_pix = hp.query_strip(nside, ra, dec, nest=True)
        if len(target_pix) == 0:
            target_pix = [hp.ang2pix(nside,ra,dec, nest=True, lonlat=True)]
        self.db_processbox.insert('insert', '>>hpix analyze | target pixels:\t\t%s\n'%str(target_pix))
        self.db_processbox.insert('insert', '>>hpix analyze | length of target pixels:\t\t%d\n'%len(target_pix))
        self.db_processbox.insert('insert', '-*-'*10+'\n')

        # file analyze
        self.db_processbox.insert('insert','>>file analyzing...\n')
        target_list = []
        self.mycursor.execute("SHOW TABLES;")
        table_list = self.mycursor.fetchall()
        table_list = np.array(table_list).reshape(-1)
        for pix in target_pix:
            for i in table_list:
                if db == 'gaia dr3':
                    if i not in target_list:
                        if int(i.split('_')[2]) >= pix and int(i.split('_')[1]) <= pix:
                            target_list.append(i)
                elif db in self.db_listl:
                    if int(i.split('_')[1]) == pix:
                        target_list.append(i)
        self.db_processbox.insert('insert','>>file analyze | target files:\t\t%s\n'%str(target_list))
        self.db_processbox.insert('insert','-*-'*10+'\n')
        
        # query
        # coor
        self.db_processbox.insert('insert','>>querying...\n')
        save_root = '/data/mysql/'
        save_path = '/Users/qingyue/cursor-tutor/' + time.strftime('%Y%m%d_%H:%M:%S',time.localtime()) + '.csv'
        names_coor = (['RAJ2000','DEJ2000']     if db=='ucac5' else  
                      ['ra', '`dec`']           if db=='gaia dr3'  else  
                      ['RAmas', 'DECmas'])
        if names_coor[0] == 'RAmas':
            ra  *= 3.6e6
            dec *= 3.6e6
            rdu *= 3.6e6
        # usecols
        table_names = np.load(self.HOME+f'/configs/column_names_{db.replace(" ","")}.npy', allow_pickle=True)
        try:
            self.usecols = self.db_parambox.curselection()
        except Exception:
            self.usecols = None
        self.usecols = (','.join(table_names[np.array(self.usecols)]) if self.usecols else '*')
        self.db_processbox.insert('insert','>>query | usecols: %s\n'%str(self.usecols))
        # shape
        self.db_processbox.insert('insert','>>query | shape: %s\n'%areashape)
        if areashape   == 'circle':
            cond = 'POWER((%s-%s),2)+POWER((%s-%s),2)<=POWER(%s,2)' % (names_coor[0], str(ra), names_coor[1], str(dec), str(rdu))
        elif areashape == 'polygon':
            cond = 'ABS(%s-%s)<=%s AND ABS(%s-%s)<=%s' % (names_coor[0], str(ra), str(rdu), names_coor[1], str(dec), str(rdu))
        elif areashape == 'stripe':
            cond = '%s>=%s AND %s<=%s' % (names_coor[1], str(ra), names_coor[1], str(ra))
        # outfile
        ifoutfile = self.db_ifoutv.get()
        if ifoutfile not in self.db_ifoutl:
            ifoutfile = 'no'
        if ifoutfile == '<server>':
            outfile = 'into outfile "%s" \n FIELDS TERMINATED BY "," \n'%save_path + r' LINES TERMINATED BY "\n" ;'
        elif ifoutfile in ['local','no']:
            outfile = ';'
        # sql combine
        query_sql = ''
        for _table in target_list:
            query_sql += 'select %s from %s where %s \n'%(
                          self.usecols, str(_table), cond)
            query_sql += '  UNION  \n'
        query_sql =  query_sql[:-11]  # drop the last UNION
        query_sql += outfile
        self.db_processbox.insert('insert', '>>query | sql:\n\t\t%s\n'%query_sql)
        
        # execute and output
        self.mycursor.execute(query_sql)
        
        self.infoback = None
        if self.usecols == '*':
            self.usecols = np.load(self.HOME+f'/configs/column_names_{db.replace(" ","")}.npy',allow_pickle=True)
        else:
            self.usecols = self.usecols.split(',')
        if ifoutfile == '<server>':
            output_table = pd.read_csv(save_path, names=table_names)
            output_table.to_csv(save_path, index=False)
            self.db_processbox.insert('insert', '>>query succ. Data has been put on "%s"\n'%save_path)
        elif ifoutfile == 'no':
            self.db_processbox.insert('insert', '>>query succ. Check button can be used.')
        elif ifoutfile == 'local':
            save_path = self.HOME+'/output_data/'+time.strftime('%Y%m%d_%H:%M:%S',time.localtime())+'.csv'
            self.infoback = self.mycursor.fetchall()
            self.infoback = pd.DataFrame(self.infoback,columns=self.usecols)
            self.infoback.to_csv(save_path, index=False)
            self.db_processbox.insert('insert', '>>query succ. Data has been put on "%s".\n'%save_path)
            self.db_processbox.insert('insert', '>>query succ. Check button can be used.')
        
    def db_hpixmap(self):
        db = self.db_listv.get()
        if db == 'gaia dr3':
            db = 'gaia3'
        map = tk.Toplevel()
        map.title = 'hpixmap'
        map.geometry("850x540")
        hpixmap = tk.PhotoImage(file=self.HOME+f'/picture/{db}_hpix_density.png')
        tk.Label(map, image=hpixmap).pack()
        map.mainloop()
    
    def db_stat(self):
        db = self.db_listv.get()
        stat = tk.Toplevel()
        stat.geometry("200x150")
        statbox = tk.Text(stat, bg='white', fg='black', height=20, width=80)
        statbox.insert('insert', 'database statment\n')
        context_gaia  = 'Data from GaiaDR3 archive, see https://gea.esac.esa.int/archive/\n'
        context_mas   = 'Data from database of QiZhaoxiang, all position parameters are converted to unit of mas.\n'
        context_ucac5 = 'Data provided by FangWenfeng, see https://vizier.cds.unistra.fr/viz-bin/VizieR?-source=I/340&-to=3\n'
        if db == 'gaia dr3':
            statbox.insert('insert', context_gaia)
        elif db == 'ucac5':
            statbox.insert('insert', context_ucac5)
        elif db in self.db_listl:
            statbox.insert('insert', context_mas)
        statbox.pack()
        stat.mainloop()
    
    def db_detail(self):
        db = self.db_listv.get().replace(' ','')
        detail = tk.Toplevel()
        detail.geometry('400x200')
        params = tk.StringVar()
        self.db_parambox = tk.Listbox(detail,bg='white',fg='black',listvariable=params,selectmode='multiple')
        for _i, _name in enumerate(np.load(self.HOME+f'/configs/column_names_{db}.npy', allow_pickle=True)):
            self.db_parambox.insert('end', _name)
        
        tk.Label(detail,text='choose parameters').grid(row=0,column=0)
        self.db_parambox.grid(row=1,column=0)
        
        scrollbar_v = tk.Scrollbar(detail,orient='vertical')
        scrollbar_v.grid(row=1,column=0, sticky='Ens')
        scrollbar_v.config(command=self.db_parambox.yview)
        self.db_parambox.config(yscrollcommand=scrollbar_v.set)
        detail.mainloop()
        
    def db_intro(self):
        self.db_introbox.insert('insert','DATABASE introduction')
        welcome = '\n' + '='*40 + \
                       '\nWelcome to use this module, in which you can check,  \
                        \nquery and get outfile from different catalogs easily.\
                        \nCatalogs contain Gaia DR3, GSC 22-24 and UCAC 1-5.\n'.replace('                       ','') + \
                  '='*40 + '\n'
        context =    '>1 [choose one database]\
                    \n>2 [input spatial information]\
                    \n>3 [click submit]\n'.replace('                    ','')
        detail  = '-'*20 + 'detail' + '-'*20 + '\n' + \
                    '1. database and (ra dec radius) are necessary for circle and polygon query, while ra and dec means dec_min and dec_max for stripe query.\n' + \
                    '2. once a database is chosen, hpix map, statement and advance button can be used.\n' + \
                    '3. hpix map shows the star density in hpix.\n' + \
                    '4. statement shows the details of selected database.\n' + \
                    "5. advance can provide selection for parameters of selected database in v2.0. NOTICE don't close the selection window before submitting.\n" + \
                    '6. if output file is defaulted by no.\n' + \
                    '7. area shape is defaulted by circle.\n' + \
                    '8. unit of (ra dec radius) is deg.\n'
        self.db_introbox.insert('insert', welcome)
        self.db_introbox.insert('insert', context)
        self.db_introbox.insert('insert', detail)
    
    def db_output(self):
        output = tk.Toplevel()
        output.geometry('1080x750')
        outbox = tk.Text(output, bg='white', fg='black', height=40, width=120)
        if type(self.infoback) == 'NoneType':
            self.infoback = self.mycursor.fetchall()
            self.infoback = pd.DataFrame(self.infoback,columns=self.usecols)
        outbox.insert('insert', self.infoback.to_string())
        outbox.grid(sticky='wens')
        output.mainloop()
    
    def __del__(self):
        #if self.mycursor:
            #self.mycursor.close()
            #self.mydb.close()
            pass

if __name__ == "__main__":
    root = Root()
    root.mainloop()
