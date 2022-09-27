from mrjob.job import MRJob
from mrjob.step import MRStep
class mostProfit(MRJob):

        def mapper_sum(self, _, line):
           try:
               	fields = line.split(',')
                if len(fields) == 7:
                      addr = fields[2]
                      values = int(fields[3])
                      epoch_time = int(fields[6])
                      if epoch_time >= 1508131331:
                           yield(addr, values)
          except:
                 pass


        def combiner_sum(self, addr, value):
                total = sum(value)
                yield(addr, total)

        def reducer_sum(self, addr, value):
                total = sum(value)
                yield(addr, total)


        def mapper_top10(self,addr,values):
                yield (None,(addr, values))


        def combiner_top10(self, key, values):
               sorted_values = sorted(values,reverse=True, key=lambda t:t[1])
               i=0
               for value in sorted_values:
                      if i < 10:
                          yield ("Top:",value)
                          i += 1
                      else:
                          break

                          break

       def reducer_top10(self, key, values):
              sorted_values = sorted(values,reverse=True, key=lambda t:t[1])
              i=0
              for value in sorted_values:
                      if i < 10:
                              yield (value[0], value[1])
                              i += 1
                      else:
                              break


      def steps(self):
             return [MRStep(mapper = self.mapper_sum, combiner = self.combiner_sum, reducer = self.reducer_sum),
                     MRStep(mapper = self.mapper_top10, combiner = self.combiner_top10, reducer = self.reducer_top10)]


if __name__=='__main__':
     mostProfit.run()
