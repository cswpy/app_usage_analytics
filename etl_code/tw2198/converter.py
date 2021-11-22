#convert the json file to csv

import pandas

source = pandas.read_json(r'./istas.json')
source.to_csv(r'./istas.csv', sep='~')