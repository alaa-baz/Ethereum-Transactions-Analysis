from mrjob.job import MRJob
from mrjob.step import MRStep


class lucrative(MRJob):

        def mapper_join(self, _, line):
            try:
                 if len(line.split("/")) == 3:
                        fields = line.split("/")
                        addr = fields[0]
                        cat = fields[1]
                        yield(addr, (cat, 1))
                 elif len(line.split(',')) == 7:
                        fields = line.split(',')
                        addr = fields[2]
                        values = float(fields[3])
                        yield(addr, (values, 2))
            except:
           	 pass

        def reducer_join(self, key, values):
                val = 0
                cat = None
                for value in values:
                       if value[1]==1:
                              cat=value[0]
                       elif value[1]==2:
                              val+= value[0]
                if cat != None and val !=0:
                       yield(cat, val)

        def mapper_sum(self, cat, values):
                yield(cat, values)

        def reducer_sum(self, cat, values):
                yield(cat, sum(values))



        def mapper_top_10(self, cat, values):
                        yield(None, (cat, values))



        def reducer_top_10(self, _, values):
               sorted_values = sorted(values, reverse=True, key=lambda l : l[1])
               for val in sorted_values:
                       yield(val[0], val[1])


        def steps(self):
                return [
                        MRStep(mapper = self.mapper_join, reducer = self.reducer_join),
                        MRStep(mapper = self.mapper_sum, reducer = self.reducer_sum),
                        MRStep(mapper = self.mapper_top_10, reducer = self.reducer_top_10)]


if __name__ == "__main__":
        lucrative.run()
