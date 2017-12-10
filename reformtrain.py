import os
import pandas
import json
data = {}
try:  
    source_root = '/home/hadoop/Bigdata/project/data/enron' 
    target_file = open('/home/hadoop/Bigdata/project/trainsort', 'w+')
    for i in range(1,7):
        source_dir = source_root + str(i) + '/ham/'
        print source_dir
        for root, sub_dirs, files in os.walk(source_dir):  
            for file in files:  
                source = open(source_dir + file)
                tmp = source.read()
                tmp = tmp.replace('\n', ' ')
                tmp = tmp.replace('\r', ' ')
                tmp = tmp.replace('\t', ' ')
                data[tmp] = '0'
                # if file[:3]=="ham":
                #     data[tmp] = '0'
                # else:
                #     data[tmp] = '1'
        source_dir = source_root + str(i) + '/spam/'
        for root, sub_dirs, files in os.walk(source_dir):  
            for file in files:  
                source = open(source_dir + file)
                tmp = source.read()
                tmp = tmp.replace('\n', ' ')
                tmp = tmp.replace('\r', ' ')
                tmp = tmp.replace('\t', ' ')
                data[tmp] = '1'
                # if file[:3]=="ham":
                #     data[tmp] = '0'
                # else:
                #     data[tmp] = '1'
except Exception as e:  
    print "error" 

for mail in data:
    target_file.write(data[mail] + '\t' + mail+'\r')
