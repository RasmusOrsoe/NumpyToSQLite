# NumpyToSQLite


 <h2> Writing Numpy Arrays to SQLite databases (NumpyToSQLite/CreateDatabasev2.py) </h2>
  In CreateDatabasev2.py you specify which pulse information in the numpy array you want as a database file. This convertion is then done by writing multiple temporary databases to disk in parallel, that are then merged to one large database in the end. The pulse information is transformed using sklearn.preprocessing.RobustScaler before saved in a .db file. This step can be removed from code or replaced with your own transforms. The code assigns an <strong> event number<\strong> to each event, that will facilitate extraction from the database. Event numbers in this code ranges from 0 to the number of events in the numpy array. \ 
 Please note that writing .db files is memory intensive. You can decrease the memory usage by decreasing df_size in CreateDatabasev2.py (which is set to 100.000) but this will also increase run time. For reference: It takes around 2 hours to write 4.4 million events to a .db file at n_workers  = 4. 

<strong>CreateDatabasev2.py takes arguments: </strong>\
  <strong>--array_path</strong>: The path to numpy arrays from the I3-to-Numpy Pipeline I3Cols. E.g: /home/my_awesome_arrays 
  
  <strong>--key</strong>       : The field/key of pulse information you want to add to the database. Multiple keys not supported. E.g : 'SplitInIcePulses'
  
  <strong>--db_name</strong>   : The name of your database. E.g: 'myfirstdatabase' 
  
 <strong> --gcd_path</strong>  : The path to the gcd.pkl file containing spatial information. This file can be produced via /I3ToNumpy/create_geo_array.py if you don't have it.</p>  
  
 <strong> --outdir</strong>    : The Location in which you wish to save the database and the transformers. The script will save the database in yourpath/data and the pickled transformers in yourpath/meta. The transformers can be read using pandas.read_pickle()  
  
  <strong>--n_workers </strong>: The number of workers 
  
  <strong>Example:</strong>
  ```html
  python CreateDatabsesv2.py --array_path ~/numpy_arrays --key 'SplitInIcePulses' --db_name 'ADataBase' -- gcd_path ~/gcd --outdir ~/MyDatabases --n_workers 4 
  ```
  Suppose we now wanted to extract event number 1001, one could do so by
  
 ```html
 import pandas as pd
import sqlite3

db_file = mydbfile.db
desired_event = 1001
with sqlite3.connect(db_file) as con:
   truth_query   = 'select * from truth where  event_no == %s'%desired_event
   truth         = pd.read_sql(truth_query, con)
   
   feature_query = 'select * from features where  event_no == %s'%desired_event
   features      = pd.read_sql(feature_query, con)
 ```
  
  <strong>Notes:</strong> \
  This is effectively a Lite version of https://github.com/ehrhorn/cubedb, a more feature rich pipe-line. 
 <h2> Writing I3-Files to Numpy Arrays </h2>
 Run the scripts in I3ToNumpy in the following order:
 
 ```html
  ./load_cvmfs.sh
 ```
Among many things, this loads <strong> IceTray </strong>, IceCube software required to read I3-files. Now you can write your I3-files to numpy arrays using I3Cols:

 ```html
  ./makearray.sh
 ```
In I3ToNumpy/makearray.sh you can change the path and keys you which to extract from the I3-files. To create the gcd.pkl file, you can then run:

 ```html
  ./create_geo_array.sh
 ```

<strong> Notes : </strong> \
I3ToNumpy/create_geo_array.sh was NOT made by me. \
If your cvmfs environment doesn't contain i3Cols or other external packages, you can install these on user level using

 ```html
  pip install --user yourpackage
 ```




  
