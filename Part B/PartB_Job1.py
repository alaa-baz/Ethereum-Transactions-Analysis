from mrjob.job import MRJob
class PartB_1(MRJob):

        def mapper(self,_,line):
                try:
                    	fields = line.split(',')
                        if len(fields) == 7:
                                addre = fields[2]
                                value = float(fields[3])
                                yield (addre, value)

                except:
                       	pass

        def reducer(self,key,value):
                list = []
                for i in value:
                        list.append(i)
                total = sum(list)
                yield (key, total)

if __name__=='__main__':
        PartB_1.run()
