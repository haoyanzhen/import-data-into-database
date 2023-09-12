import pandas as pd
import numpy as np
import healpy as hp
import os.path as osp
import os
import time
import matplotlib.pyplot as plt


print(time.strftime('%Y.%m.%d %H:%M:%S',time.localtime()),flush=True)
HOME = '/data/Catalogdata/'
db_list = ['gsc22','gsc23','gsc24','ucac','ucac2','ucac3','ucac4','ucac5']
path_list = [HOME+_db+'.csv' for _db in db_list]

nside = 8
npix = hp.nside2npix(nside)
print('npix: ',npix)
for _i,_db in enumerate(db_list):
    if _i == 0 or _i == 1:
        continue
    _sep = ('\t' if _db!= 'ucac5' else ',')
    _cat = pd.read_csv(path_list[_i],sep=_sep)
    print(_db, ' read')
    
    # display(_cat[-10:])
    _names_coor = (['RAmas', 'DECmas']          if _db!='ucac5' else    ['RAJ2000','DEJ2000'])
    _ra         = (_cat[_names_coor[0]]/3.6e6   if _db!='ucac5' else    _cat[_names_coor[0]])
    _dec        = (_cat[_names_coor[1]]/3.6e6   if _db!='ucac5' else    _cat[_names_coor[1]])
    _ra  = _ra.astype(np.float64).to_numpy()
    _dec = _dec.astype(np.float64).to_numpy()
    _cat[f'hpix{nside}'] = hp.ang2pix(nside, _ra, _dec, nest=True, lonlat=True)
    del _ra, _dec
    print(_db, ' convert')
    # display(_cat[-10:])
    
    _lenl = []
    for _ii in range(npix):
        # _save_path = osp.join(osp.split(path_list[_i])[0],(db_list[_i].upper()+'_hpix8/')) + _db +f'_{_ii}.csv'
        _save_path = osp.join('./',(db_list[_i].upper()+'_hpix8/')) + _db +f'_{_ii}.csv'
        if not osp.exists(osp.split(_save_path)[0]):
            print(      'mkdir %s' % osp.split(_save_path)[0])
            os.system(  'mkdir %s' % osp.split(_save_path)[0])
        _cati = _cat[_cat['hpix8']==_ii]
        print('saving file: %s || length:%d' % (_save_path, len(_cati)))
        _cati.to_csv(_save_path, index=False)
        _lenl.append(len(_cati))
        
    _lenl = np.array(_lenl)
    np.save(osp.split(_save_path)[0]+'/hpix_density.npy',_lenl)
    hp.mollview(_lenl,unit="hpix",nest=False,cmap='RdBu_r')
    hp.graticule(local=True)
    plt.savefig(osp.split(_save_path)[0]+'/hpix_density.png')
    print(_db, ' save hp')
    
    # _cat.to_csv( osp.join(osp.split(path_list[_i])[0], '%s_hp.csv'%_db) , index=False)
    _cat.to_csv( './%s_hp.csv'%_db , index=False)
    print(_db, ' save full')