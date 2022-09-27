from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class top_cont(MRJob):
#Join of two tables
        def mapper(self, _, line):
                try:
                    	if len(line.split(',')) == 7:
                                fields=line.split(',')
                                block_num = int(fields[0])
                                adrr = fields[2]
                                if adrr == "0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444":
                                        yield(adrr,(block_num))
                except:
                       	pass


        def reducer(self, addr, block_num):
                for val in block_num:
                        yield(addr, val)



if __name__ == "__main__":
        top_cont.run()
