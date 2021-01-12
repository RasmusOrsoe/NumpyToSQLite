import pandas as pd
import numpy as np
import os
import sqlite3
from sklearn.preprocessing import RobustScaler
import pickle
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from sqlalchemy import create_engine
import sqlalchemy
import time
from multiprocessing import Pool
import multiprocessing

def FillStack(n_workers,data,df_size,manager,table,db_path):
    data = data.reset_index(drop = True)
    print('This StackFiller Recieved %s events'%len(pd.unique(data['event_no'])))
    settings = []
    job_list = SplitIndicies(data,n_workers,exclude_initial = True)
    q = manager.Queue() 
    for j in range(len(job_list)):
        print('joblist[%s][0] = %s || joblist[%s][1] = %s'%(j,job_list[j][0],j, job_list[j][1]))
        chunk =  data.loc[job_list[j][0]:job_list[j][1],:]
        
        db_path_tmp = db_path.split('.')[0] + '_tmp-%s.db'%str(j) 
        n_appends  = int(np.ceil(len(chunk['event_no'])/df_size))
        
        #for k in range(0,n_appends):
        #    chunk = chunk.reset_index(drop = True)
        #    print('STACKING %s QUEUE : %s / %s || %s / %s '%(table,k,n_appends,j+1,len(job_list)))
        #    if((k+1)*df_size > len(chunk['event_no'])):
        #        up = len(chunk['event_no'])
        #    else:
        #        up = (k+1)*df_size
        #        data_batch           = chunk.loc[k*df_size:up,:]
        #        q.put(data_batch)
        q.put(chunk)
        settings.append([db_path_tmp,table,q,n_appends,n_workers]) 
    
    return settings  #settings

def MergeTemporaries(path):
    files = os.listdir(path)
    tmp = []
    for file in files:
        if file[-3:] == '.db':
            if 'tmp' in file:
                tmp.append(file)
            else:
                main_db = file

    return [main_db, tmp]

def WorkForeman(workers):
    worker_status = 0
    for i in range(0,len(workers)):
        check = workers[i].is_alive()
        worker_status += check
    if worker_status == 0:
        
        return False
    else:
        return True
        
def GrabGCD(path):
    print('GRABBING GEO-SPATIAL DATA')
    files = os.listdir(path)
    for file in files:
        if file.endswith('.pkl'):
            gcd = pd.read_pickle(path + '//' + file)
    return gcd['geo']

def WriteToDB(settings):
    db_path, table, q,n_appends,n_workers = settings
    engine = sqlalchemy.create_engine(db_path)
    queue_empty = q.empty()
    print(queue_empty)
    chunk_counter  = 1
    while(queue_empty != True):
        print('INSERTING IN %s : %s  / %s '%(table,chunk_counter, n_appends))
        data_batch = q.get() 
        data_batch.to_sql(table,engine,index= False, if_exists = 'append',chunksize = len(data_batch))
        chunk_counter += 1
        queue_empty = q.empty()
        
    engine.dispose() 

    return

def SplitIndicies(data,n_workers,exclude_initial = True):
    if exclude_initial == True:
        n_rows         = np.arange(11,len(data)-1)
    else:
        n_rows         = np.arange(0,len(data)-1)
    chunks         = np.array_split(n_rows,n_workers)
    
    split_indicies = []
    n_events = 0
    for chunk in chunks:
        split_indicies.append([chunk[0],chunk[-1]])
        n_events += len(chunk)
    print('total amount of events in Splits: %s '%n_events)
    return split_indicies
    

def parse_args(description=__doc__):
    """Parse command line args"""
    parser = ArgumentParser(
        description=description,
        formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--array_path', type=str, required=True,
        help='The path in which the data.npy and index.npy is saved. E.g: /home/my_awesome_arrays/SplitInIcePulses',
    )
    parser.add_argument(
        '--key', type=str, required=True,
        help='The key for the array containing the pulse data you want in the database. e.g. SplitInIcePulses ',
    )
    parser.add_argument(
        '--db_name', type=str, required=True,
        help='Name of database - no extension please. E.g: MyNewDataBase',
    )
    parser.add_argument(
        '--gcd_path', metavar='GCD_PATH', dest='gcd_path', type=str,
        required=True,
        help='The Path to the GCD.pkl file. Just provide the path not the actual file. E.g: /home/gcd not /home/gcd/gcd.pkl - the code will search for any .pkl file in the directory - so keep it tight. Having multiple will make the code fail. '
    )
    parser.add_argument(
        '--outdir', type=str, required=True,
        help='Directory into which to save .db file and transform.pkl',
    )
    parser.add_argument(
        '--n_workers', type=int, required=True,
        help='Number of Workers',
    )
    return parser.parse_args()
def CreateDataBase(array_path,db_name,key,gcd_path,outdir,n_workers):
    #array_path            = r'X:\speciale\hep\arrays_from_hep\arrays'
    #gcd_path              = r'X:\speciale\hep\gcd\gcd_array'
    #key                   = 'SplitInIcePulses'
    #outdir                = r'X:\speciale\hep\arrays_from_hep\output'
    #db_name               = 'test-db'
 
    start_time            = time.time()

    path                  = array_path + '/' + key
    print('LOADING %s FEATURE ARRAY...'%key)
    data                  = np.load(path + '/data.npy')
    print('LOADING %s FEATURE INDEX...'%key)
    data_index            = np.load(path + '/index.npy')

    truth_key             = 'MCInIcePrimary'
    print('LOADING %s TRUTH ARRAY'%truth_key) 
    path_truth            = array_path + '/' + truth_key
    truth                 = np.load(path_truth + '/' + 'data.npy')
    
    transformer_dict    = {'input':{}, 'truth':{}}
    
    ####################################
    #                                  #
    #      LEGACY TRUTH VARIABLES      #
    #                                  #
    ####################################
    
    print('EXTRACTING TRUTH VALUES FOR %s EVENTS..'%(len(truth['pdg_encoding']) + 1))        
    truth           = pd.concat([pd.DataFrame(np.arange(1,len(truth['pdg_encoding']) + 1)),
                                 pd.DataFrame(truth['energy']),
                                 pd.DataFrame(truth['time']),
                                 pd.DataFrame(truth['pos']['x']),
                                 pd.DataFrame(truth['pos']['y']),
                                 pd.DataFrame(truth['pos']['z']),
                                 pd.DataFrame(truth['dir']['azimuth']),
                                 pd.DataFrame(truth['dir']['zenith']),
                                 pd.DataFrame(truth['pdg_encoding']),
                                 pd.DataFrame(truth['length'])]
                                ,axis = 1)
    truth.columns   = ['event_no',
                       'energy_log10',
                       'time',
                       'position_x',
                       'position_y',
                       'position_z',
                       'azimuth',
                       'zenith',
                       'pid',
                       'muon_track_length']
    
    #feats = str('event_no,x,y,z,time,charge_log10')
    #truths = str('event_no,energy_log10,time,vertex_x,vertex_y,vertex_z,direction_x,direction_y,direction_z,azimuth,zenith,pid)
    ####################################
    #                                  #
    #     LEGACY FEATURES VARIABLES    #
    #                                  #
    ####################################
    print('EXTRACTING FEATURES ...')
    geo                 = GrabGCD(gcd_path)
    hits_idx            = data_index
    hits                = data
    single_hits         = np.empty(hits.shape + (5,))
    event_no            = np.empty((len(hits),))
    string_idx          = hits['key']['string'] - 1
    om_idx              = hits['key']['om'] - 1
    single_hits[:, 0:3] = geo[string_idx, om_idx]
    single_hits[:, 3]   = hits['pulse']['time']
    single_hits[:, 4]   = hits['pulse']['charge']
    
    print('EXTRACTING GEO-SPATIAL DOM DATA')
    for i in range(len(hits_idx)):
        this_idx = hits_idx[i]
        event_no[this_idx['start'] : this_idx['stop']] = i + 1
    
    features            = pd.concat([pd.DataFrame(event_no),
                                      pd.DataFrame(single_hits)]
                                      ,axis = 1)
    features.columns    = ['event_no',
                            'dom_x',
                            'dom_y',
                            'dom_z',
                            'dom_time',
                            'charge_log10']
    del single_hits
    del event_no
    
    ####################################
    #                                  #
    #          PREPROCESSING           #
    #                                  #
    ####################################    
    truth_keys          = truth.columns
    feature_keys        = features.columns
    feature_keys        = feature_keys[feature_keys != 'event_no']
    
    truth_keys          = truth_keys[truth_keys != 'event_no']
    truth_keys          = truth_keys[truth_keys != 'pid']
    
    
    
    for key in feature_keys:
        print('FITTING %s TRANSFORMER' %key)
        scaler          = RobustScaler()
        scaler          = scaler.fit(np.array(features[key]).reshape(-1,1))
        features[key]   = scaler.transform(np.array(features[key]).reshape(-1,1))
        transformer_dict['input'][key] = scaler
    
    for key in truth_keys:
        print('FITTING %s TRANSFORMER' %key)
        scaler          = RobustScaler()
        scaler          = scaler.fit(np.array(truth[key]).reshape(-1,1))
        truth[key]      = scaler.transform(np.array(truth[key]).reshape(-1,1))
        transformer_dict['truth'][key] = scaler
    
    ####################################
    #                                  #
    #      CREATE SQLite DATABASE      #
    #                                  #
    ####################################
    print('SAVING TRANSFORMERS..')
    
    transformer_path = outdir + '/' + '/%s/'%db_name + 'meta'
    db_path          = outdir + '/' + '/%s/'%db_name + 'data'
    os.makedirs(db_path)
    os.makedirs(transformer_path)
    tmp = open(transformer_path + '/transformers.pkl', 'wb')
    pickle.dump(transformer_dict, tmp)
    tmp.close()
    
    # 
    #  INITIAL TRUTH COMMIT
    #  Creates and commits 10 rows of data in 'truth' table in SQLite DataBase 
    #

    df_size    = 100000  # This sets the size of each commit of the workers. Keep this low to maintain low memory usage
    print('MAKING INTIAL TRUTH COMMIT')
    truth_initial           = truth.loc[0:10,:]
    engine = sqlalchemy.create_engine('sqlite:///'+db_path + '/%s.db'%db_name)
    truth_initial.to_sql('truth',engine,index= False, if_exists = 'append')
    engine.dispose()  
    
    #
    # INITIAL FEATURES COMMIT
    # Creates and commits 10 rows of data in 'features' table in SQLite DataBase
    #
    print('MAKING INITIAL FEATURES COMMIT')
    features_initial      = features.loc[0:10,:]
    engine = sqlalchemy.create_engine('sqlite:///'+db_path + '/%s.db'%db_name)
    features_initial.to_sql('features',engine,index= False, if_exists = 'append')
    engine.dispose()
    
    #
    #  MULTI-PROCESSING 
    # 'FillStack' provides a multiprocessing.Queue() full of pd.DataFrames() with df_size rows for each worker.  
    #  Each worker then fills a temporary database with these pd.DataFrames() using pd.to_sql()
    #
    
        

    manager = multiprocessing.Manager()
    db_path = 'sqlite:///'+db_path + '/%s.db'%db_name

    settings_features = FillStack(n_workers, features, df_size, manager, 'features', db_path) # Arguments for FillStack
    settings_truth    = FillStack(n_workers, truth,df_size,manager,'truth',db_path)
    
    del truth   # To keep memory usage low. The data is now stored in chunks in multiprocessing.Queue()'s via FillStack(), so it's fine
    del features#
        
    
    p = Pool(processes = n_workers)
    p.map(WriteToDB, settings_truth)   # This fills the rest of the table 'truth' using n_workers
    p.map(WriteToDB, settings_features)# This fills the rest of the table 'features' using n_workers when  the line above is done
    p.close()
    p.join()
    
    print('Temporary Databases created! Merging...')
    
    #
    #
    # MERGING
    # Here the temporary databases are merged using a single core
    # 
    path = outdir + '/' + db_name + '/data'
   
    main_db, tmp = MergeTemporaries(path)
    tables       = ['truth', 'features']
    engine_main = sqlalchemy.create_engine('sqlite:///' + path + '/' + main_db)
    for j in range(0,len(tmp)):
        for k in range(0,len(tables)):
            with sqlite3.connect(path + '/' + tmp[j] ) as con:
                query      = 'select * from %s'%tables[k]
                data_batch = pd.read_sql(query, con)
            engine_tmp  = sqlalchemy.create_engine('sqlite:///' + path + '/' + tmp[j])    
            data_batch.to_sql(tables[k],engine_main,index= False, if_exists = 'append',chunksize = len(data_batch))
        print('MERGING %s / %s'%(j,len(tmp)))
    engine_main.dispose() 
   
    print('DONE!')
    print('Time Elapsed: %s min'%((time.time()-start_time)/60))    
    return

if __name__ == '__main__':
    CreateDataBase(**vars(parse_args()))

