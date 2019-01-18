import pyspark
import re

sc = pyspark.SparkContext()

def is_wallet(line):
    wallet_id  = "{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}"
    try:
        fields = line.split(",")
        if fields[3] == wallet_id and len(fields)==4:
            return True
        else:
            return False
    except:
        return False

#retrieve data from HDFS
lines = sc.textFile("/data/bitcoin/vout.csv")
#return features only if wallet_id is the one specified
filter_data = lines.filter(is_wallet)

#format it to (K,V)=> (hash,1)
vout_features = filter_data.map(lambda l: ((l.split(',')[0]),1)


#retrieve data from HDFS
vin = sc.textFile("/data/bitcoin/vin.csv")
#format it to (k,v) => (tx_id,(tx_hash,vout))
vin_features = vin.map(lambda l: ((l.split(',')[0]),(l.split(',')[1],l.split(',')[2])))


join_data = vin_features.join(vout_features)
#join data as (K,V) => ((tx_id),(tx_hash, vout),(1))

from_join = join_data.map(lambda t: ((t[1][0][0],t[1][0][1]),1))
#from_join as (K,V)=> ((tx_hash, vout),(1))

#Save file to HDFS
from_join.repartition(1).saveAsTextFile("first_join")


#######After done with this part comment lines and continue below#####

#retrieve the file from HDFS saved from before
first_join = sc.textFile("first_join/f_join.txt")

#format again the file
join = first_join.map(lambda l: l.split("'"))
f_join = join.map(lambda l: ((l[1],l[3]),1))
#f_join => ((tx_hash, vout)(1))


#filter method
def clean_vout(line):
    try:
        fields = line.split(',')
        if len(fields)!= 4:
            return False

        float(fields[1])
        return True
    except:
        return False



#convet value to float and check if fields in each line are 4
vout_filter = lines.filter(clean_vout).map(lambda l: l.split(","))

#set vout  as (K,V)=> ((hash,n),(value,publicKey))
vout = vout_filter.map(lambda l: (((l[0],l[2]),(float(l[1]),l[3]))))


#join vout and from_join
final_join = vout.join(f_join)
#final_join format => ((hash,n),((value, publicKey),(1)))


final = final_join.map(lambda l: (l[1][0][1],float(l[1][0][0])))

#final format = (publicKey, value)


#use takeOrdered to get first 10 based on value(value)
top10 = final.takeOrdered(10, key = lambda x: -x[1])
#print each line of top10
for record in top10:
    print("{}: {}".format(record[0],record[1]))
