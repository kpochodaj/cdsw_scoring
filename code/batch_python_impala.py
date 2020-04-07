# https://docs.ibis-project.org/impala.html
# https://docs.ibis-project.org/sql.html
# the alternative is impyla but it has issues with python 3

# !pip3 install --upgrade --force-reinstall  ibis-framework[impala]

import ibis
import os
ibis.options.interactive = True
ibis.options.verbose = True

# Connection to Impala
# Secure clusters will require additional parameters to connect to Impala.
# Recommended: Specify IMPALA_HOST as an environment variable in your project settings

# change IP to your Impala host
IMPALA_HOST = os.getenv('IMPALA_HOST', '10.0.1.254')
con = ibis.impala.connect(host=IMPALA_HOST, port=21050, database='default')

# EXPLORE
# list tables in database
con.list_tables()

# select table
all = con.table('telco_churn')

# display metadata
all.schema()
all.metadata()

# display selected data
print(all['customerid', 'churn'])
print(all[all.churn == 'Yes'])

# read into dataframe
import pandas as pd

delta = all[all]

# load serialized model using joblib/pickle/...
# loaded_model = joblib.load(filename)
# loaded_model = pickle.load(open(filename, 'rb'))


# predict / score data
# loaded_model.predict(delta)

# add churn column (to prefabricate model's prediction) 
mutated = delta.mutate(churn="Yes")
delta = mutated['customerid', 'churn']

# print(ibis.impala.compile(delta))

# create table from panda
con.create_table('scored_delta', delta)

# confirm that data has been loaded properly
delta_impala = con.table('scored_delta')
print(ibis.impala.compile(delta_impala[delta_impala]))
delta_impala.execute()

