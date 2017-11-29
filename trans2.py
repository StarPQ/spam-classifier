source = open('/home/hadoop/Bigdata/project/result.txt')
path = '/home/hadoop/Bigdata/project/resource.txt'
# i=0
# j=0
target = open(path, 'w+')
for line in source:
    # if j%981==0:
    #     target = open(path+str(i), 'w+')
    #     i=i+1
    #     j=0
    target.write("2" + '\t' + line)
    # j=j+1