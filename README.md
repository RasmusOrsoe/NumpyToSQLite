# NumpyToSQLite




NumpyToSQLite/CreateDatabasev2.py takes arguments: \
  -- array_path: The path to numpy arrays from the I3-to-Numpy Pipeline I3Cols. E.g: /home/my_awesome_arrays 
  
  -- key       : The field/key of pulse information you want to add to the database. Multiple keys not supported. E.g : 'SplitInIcePulses'
  
  -- db_name   : The name of your database. E.g: 'myfirstdatabase' 
  
  -- gcd_path  : The path to the gcd.pkl file containing spatial information. This file can be produced via /I3ToNumpy/create_geoarray.py if you don't have it.  
  
  -- outdir    : The Location in which you wish to save the database and the transformers.  
  
  -- n_workers : The number of workers 
  
  Example:    
  python CreateDatabsesv2.py --array_path ~/numpy_arrays --key 'SplitInIcePulses' --db_name 'ADataBase' -- gcd_path ~/gcd/gcdfile.pkl --outdir ~/MyDatabases --n_workers 4 \
  
