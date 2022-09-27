from mrjob.job import MRJob

class PartB_top10(MRJob):

        def mapper(self,_,line):
                try:
                    	fields = line.split('\t')
                        if len(fields) == 2:
                                address = fields[0].strip('"\"\\\"')
                                aggregation = float(fields[1].strip('"\"\\\"'))
                                yield (None, (address, aggregation))
                except:
                       	pass


        def combiner(self,key,values):
                sorted_values = sorted(values,reverse=True, key=lambda t:t[1])
                i=0
                for value in sorted_values:
                        if i <10:
                                yield ('Top-ten: ',value)
                        else:
                             	break
                        i+=1

        def reducer(self,key,values):
                sorted_values = sorted(values,reverse=True, key=lambda t:t[1])
                i=0
                for value in sorted_values:
                        if i <10:
                                yield ((value[0], value[1]))
                        else:
                             	break
                        i+=1


if __name__=='__main__':
        PartB_top10.run()
