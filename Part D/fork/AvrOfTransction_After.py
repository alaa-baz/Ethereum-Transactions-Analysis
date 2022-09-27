from mrjob.job import MRJob
import time

class AvrOfNumTransacti_after(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7:
                epoch_time = int(fields[6])
                trans_time = time.strftime('%m/%Y', time.gmtime(epoch_time))
                if epoch_time > 1508131331: #After fork
                  yield(str(trans_time), 1)
        except:
            pass

    def combiner(self, year, value):
        total = sum(value)
        yield(year, total)

    def reducer(self, year, value):
        total = sum(value)
        yield(year, total)

if __name__ == "__main__":
    AvrOfNumTransacti_after.run()
