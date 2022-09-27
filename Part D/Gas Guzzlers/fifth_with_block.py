from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class top_cont(MRJob):
        def mapper(self, _, line):
                try:
                    	if len(line.split(',')) == 7:
                                fields=line.split(',')
                                block_num = int(fields[0])
                                adrr = fields[2]
                                if adrr == "0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8":
                                        yield(adrr,(block_num))
                except:
                       	pass


        def reducer(self, addr, block_num):
                for val in block_num:
                        yield(addr, val)



if __name__ == "__main__":
        top_cont.run()
