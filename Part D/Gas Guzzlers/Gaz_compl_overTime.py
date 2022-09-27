from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class GasCompli(MRJob):


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
                                complic = float(fields[3])
                                epoch_time = float(fields[7])
                                trans_time = time.strftime('%m/%Y', time.gmtime(epoch_time))
                                yield(block_num, (str(trans_time),complic, 2))
                except:
                       	pass

        def reducer_join(self, block_num, values):
                complic = 0
                year = []
                x= None

                for value in values:
                        if value[2] == 1:
                                x = value[0]
                        elif value[2] == 2:
                                year.append(value[0])
                                complic= value[1]

                if x != None and len(year) != 0:
                        yield(year, complic)


#calculate the average of difficulty
        def mapper_avg(self, year, compli):
                yield(year, (compli, 1))

        def reducer_avg(self, year, values):
                compli_value = []
                count = []
                for value in values:
                        compli_value.append(value[0])
                        count.append(value[1])
                total_value = sum(compli_value)
                total_count = sum(count)
                avr = total_value/total_count
                yield(year, avr)

        def steps(self):
                return [MRStep(mapper= self.mapper_join, reducer = self.reducer_join),
                        MRStep(mapper= self.mapper_avg, reducer = self.reducer_avg)]

if __name__ == "__main__":
        GasCompli.run()
