from mrjob.job import MRJob

class PartC1(MRJob):

        def mapper(self,_,line):
                try:
                    	fields = line.split(',')
                        if len(fields) == 9:
                                miner = fields[2]
                                size = float(fields[4])
                                yield (miner, size)
                except:
                       	pass

        def combiner(self,key,value):
                total = sum(value)
                yield (key, total)

        def reducer(self,key,value):
                total = sum(value)
                yield (key,total)


if __name__=='__main__':
        PartC1.run()
