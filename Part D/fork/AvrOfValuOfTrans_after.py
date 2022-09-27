from mrjob.job import MRJob
import time

class NumberOfValuOfTran_after(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7:
                epoch_time = int(fields[6])
                values = int(fields[3])
                tran_time = time.strftime('%m/%Y', time.gmtime(epoch_time))
                if epoch_time >= 1508131331: #after
                    yield(str(tran_time), (values, 1))
        except:
            pass

    def combiner(self, year, values):
        val = []
        count = []
        for value in values:
            val.append(value[0])
            count.append(value[1])
        val_sum= sum(val)
        count_sum = sum(count)
        yield(year, (val_sum, count_sum))

    def reducer(self, year, values):
        val = []
        count = []
        for value in values:
            val.append(value[0])
            count.append(value[1])
        val_sum= sum(val)
        count_sum = sum(count)
        avr=val_sum/count_sum
        yield(year, avr)


if __name__ == "__main__":
    NumberOfValuOfTran_after.run()
