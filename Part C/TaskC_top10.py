
from mrjob.job import MRJob

class PartC_top10(MRJob):

        def mapper(self,_,line):
                try:
                    	fields = line.split('\t')
                        if len(fields) == 2:
                                addre = fields[0].strip('"\"\\\"')
                                sum = float(fields[1])
                                yield (None,(addre, sum))
                except:
                       	pass

        def combiner(self, key, values):
                sorted_values = sorted(values,reverse=True, key=lambda t:t[1])
                i=0
                for value in sorted_values:
                        if i < 10:
                                yield ("Top_ten:",value)
                                i += 1
                        else:
                             	break

        def reducer(self, key, values):
                sorted_values = sorted(values,reverse=True, key=lambda t:t[1])
                i=0
                for value in sorted_values:
                        if i < 10:
                                yield (value[0], value[1])
                                i += 1
                        else:
                             	break


if __name__=='__main__':
        PartC_top10.run()
