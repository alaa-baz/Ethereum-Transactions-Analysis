from mrjob.job import MRJob

class PartB_join(MRJob):

        def mapper(self,_,line):
                try:
                    	if (len(line.split('\t'))==2):
                                fields = line.split('\t')
                                join_key = fields[0].strip('"')
                                join_val = fields[1]
                                yield (join_key,(join_val,1))


                        elif(len(line.split(','))==5):
                                fields = line.split(',')
                                join_key = fields[0].strip('"')
                                join_value = fields[3]
                                yield (join_key, (join_value, 2))

                except:
                       	pass

        def reducer(self, key, values):
                val =[]
                new_val = None

                for value in values:
                        if value[1]==1:
                                new_val=value[0]
                        elif value[1]==2:
                                val.append(value[0])
                if new_val != None and len(val)!=0:
                        yield (key, new_val)


if __name__=='__main__':
        PartB_join.run()
