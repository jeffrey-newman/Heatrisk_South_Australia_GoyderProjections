str = r"ok23-1.set10.csiro.mk36.r85.23704.001.txt"
gcms = ["miroc.esm", "miroc5", "access10", "csiro.mk36", ]
if str.endswith(".txt"):
    for gcm_name in gcms:
        p1 = str.find(gcm_name)
        if str.find(gcm_name) != -1:
            break
p6 = str.rfind('.')
p5 = str.rfind('.', 0, p6)
p4 = str.rfind('.', 0, p5)
p3 = str.rfind('.', 0, p4)
format = str[p6+1:]
replicate = str[p5+1:p6]
station = str[p4+1:p5]
climate = str[p3+1:p4]
gcm = str[p1:p3]
zone = str[:p1-1]

print (zone, gcm, climate, station, replicate, format, sep=' ')
