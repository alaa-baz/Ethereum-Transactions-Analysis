from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class Gas_used(MRJob):
#Join of two tables
        def mapper_join(self, _, line):
                try:
                    	if len(line.split(',')) == 5:
                                fields=line.split(',')
                                block_num = int(fields[3])
                                yield(block_num,(1, 1,1))

                        elif len(line.split(',')) == 9:
                                fields=line.split(',')
                                block_num = int(fields[0])
                                Gas_used = float(fields[6]) #Gas used
                                epoch_time = float(fields[7])
                                trans_time = time.strftime('%m/%Y', time.gmtime(epoch_time))
                                yield(block_num, (str(trans_time),Gas_used, 2))
                except:
                       	pass

        def reducer_join(self, block_num, values):
                Gas_used = 0
                year = []
                x= None

                for value in values:
                        if value[2] == 1:
                                x = value[0]
                        elif value[2] == 2:
                                year.append(value[0])
                                Gas_used= value[1]

                if x != None and len(year) != 0:
                        yield(year, Gas_used)


                        
#calculate the average of gaz used
        def mapper_avg(self, year, Gas_used):
                yield(year, (Gas_used, 1))

        def reducer_avg(self, year, values):
                Gas_used = []
                count = []
                for value in values:
                        Gas_used.append(value[0])
                        count.append(value[1])
                total_value = sum(Gas_used)
                total_count = sum(count)
                avr = total_value/total_count
                yield(year, avr)

        def steps(self):
                return [MRStep(mapper= self.mapper_join, reducer = self.reducer_join),
                        MRStep(mapper= self.mapper_avg, reducer = self.reducer_avg)]

if __name__ == "__main__":
        Gas_used.run()
