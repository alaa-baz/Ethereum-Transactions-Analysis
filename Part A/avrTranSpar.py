import pyspark
import time
sc = pyspark.SparkContext()

def good_line(line):
        try:
            	fields = line.split(',')
                if len(fields) != 7:
                        return False
                float(fields[6])
                return True
        except:
               	return False

line = sc.textFile("/data/ethereum/transactions") #read line
clean_line = line.filter(good_line) #filter the line
year_month = clean_line.map( lambda t: (time.strftime("%Y/%m", time.gmtime(float(t.split(',')[6]))), 1 ))
feature = year_month.reduceByKey(lambda a,b: a+b)
reducer1 = feature.sortBy(lambda l : l[0]) #sore by date
for record in reducer1.collect():
        print(" {}  {}".format(record[0],record[1]))
