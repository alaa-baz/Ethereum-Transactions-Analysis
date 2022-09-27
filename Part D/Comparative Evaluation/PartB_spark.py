import pyspark

sc = pyspark.SparkContext()

def is_good_line_trans(line):
        try:
            	fields = line.split(',')
                if len(fields) == 7:
                        float(fields[3])
                        return True
                else:
                     	return False
        except:
               	return False

def is_good_line_contr(line):
        try:
            	fields = line.split(',')
                if len(fields) == 5:
                        return True
                else:
                     	return False

        except:
               	return False




lines_trans = sc.textFile("/data/ethereum/transactions")
lines_contr = sc.textFile("/data/ethereum/contracts")

cleanlines_trans = lines_trans.filter(is_good_line_trans)
cleanlines_contr = lines_contr.filter(is_good_line_contr)

features_trans  = cleanlines_trans.map(lambda t: (t.split(',')[2], float(t.split(',')[3])))
features_contr = cleanlines_contr.map(lambda c: (c.split(',')[0], 0))

#checkpoint
cache_trans = features_trans.cache()
cache_contr = features_contr.cache()

#Job 1
sum = cache_trans.reduceByKey(lambda a,b: (a+b))
inmem = sum.persist()

#Job2 (Join)

joinB = inmem.join(cache_contr)
result = joinB.filter(lambda x: (x[1][1] != None))

#Job3 (Top10)

top_10 = result.takeOrdered(10, key=lambda t: (-t[1][0]))

for value in top_10:
       print(value[0], value[1][0])
