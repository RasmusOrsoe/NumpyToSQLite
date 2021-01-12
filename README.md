# NumpyToSQLite


 <h2> Writing Numpy Arrays to SQLite databases (NumpyToSQLite/CreateDatabasev2.py) </h2>
  In CreateDatabasev2.py you specify which pulse information in the numpy array you want as a database file. This convertion is then done by writing multiple temporary databases to disk in parallel, that are then merged to one large database in the end. The pulse information is transformed using sklearn.preprocessing.RobustScaler before saved in a .db file. This step can be removed from code or replaced with your own transforms. Please note that writing .db files is memory intensive. You can decrease the memory usage by decreasing df_size in CreateDatabasev2.py (which is set to 100.000) but this will also increase run time. For reference: It takes around 2 hours to write 4.4 million events to a .db file at n_workers  = 4. 

CreateDatabasev2.py takes arguments: \
  -- array_path: The path to numpy arrays from the I3-to-Numpy Pipeline I3Cols. E.g: /home/my_awesome_arrays 
  
  -- key       : The field/key of pulse information you want to add to the database. Multiple keys not supported. E.g : 'SplitInIcePulses'
  
  -- db_name   : The name of your database. E.g: 'myfirstdatabase' 
  
  -- gcd_path  : The path to the gcd.pkl file containing spatial information. This file can be produced via /I3ToNumpy/create_geoarray.py if you don't have it.  
  
  -- outdir    : The Location in which you wish to save the database and the transformers. The script will save the database in yourpath/data and the pickled transformers in yourpath/meta. The transformers can be read using pandas.read_pickle()  
  
  -- n_workers : The number of workers 
  
  Example:    
  python CreateDatabsesv2.py --array_path ~/numpy_arrays --key 'SplitInIcePulses' --db_name 'ADataBase' -- gcd_path ~/gcd/gcdfile.pkl --outdir ~/MyDatabases --n_workers 4 
  
 <h2>  </h2> 
  
  
