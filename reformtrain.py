import os
import pandas
import json
data = {}
try:  
    source_dir = '/home/hadoop/Bigdata/project/train/'  
    target_file = open('/home/hadoop/Bigdata/project/trainsort', 'w+')  
    for root, sub_dirs, files in os.walk(source_dir):  
        for file in files:  
            # target_file.write(file + '\t')
            source = open(source_dir + file)
            tmp = source.read()
            tmp = tmp.replace('\n', ' ')
            tmp = tmp.replace('\r', ' ')
            tmp = tmp.replace('\t', ' ')
            # data.setdefault("mail", []).append(tmp)
            # data.setdefault("label", []).append(file[:3])
            # data = {}
            # data[file[:3]] = tmp
            # p = json.dumps(data)
            # target_file.write(p+'\r')
            data[tmp] = file[:3]
            # for line in source:
            #target_file.write(line,)
            # break	
except Exception as e:  
    print "error" 

# p = json.dumps(data)
# target_file.write(p)

for mail in data:
    target_file.write(data[mail] + '\t' + mail+'\r')
