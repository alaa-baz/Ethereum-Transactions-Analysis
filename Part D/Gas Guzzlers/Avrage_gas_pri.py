from mrjob.job import MRJob
import time

class AvrGasPrice(MRJob):
    def mapper(self, _, line):
        fields = line.split(',')
        try:
            if len(fields) == 7:
                gas = int(fields[5])
                epoch_time = int(fields[6])
                time_format = time.strftime('%m-%Y', time.gmtime(epoch_time))
                yield(str(time_format), (gas, 1))
        except:
            pass

    def combiner(self, date, values):
        count = 0
        total = 0
        for value in values:
            count += value[0]
            total += value[1]
        yield(date, (count, total))

    def reducer(self, date, values):
        count = 0
        total = 0
        for value in values:
            count += value[0]
            total += value[1]
        yield(date, count/total)

if __name__ == "__main__":
    AvrGasPrice.run()
