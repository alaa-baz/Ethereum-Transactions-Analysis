from mrjob.job import MRJob
from mrjob.step import MRStep


class correlate(MRJob):

        def mapper_join(self, _, line):
            try:
                 if len(line.split("/")) == 3:
                        fields = line.split("/")
                        addr = fields[0]
                        cat = fields[1]
                        stat = fields[2]
                        cat_sta = cat + "-" + stat
                        yield(addr, (cat_sta ,1))
                 elif len(line.split(',')) == 7:
                        fields = line.split(',')
                        addr = fields[2]
                        values = float(fields[3])
                        yield(addr, (values,2))
            except:
                 pass

        def reducer_join(self, addr, values):
                val = []
                new_cat_stat = None
                for value in values:
                    if value[1]==1:
                        new_cat_stat=value[0]
                    elif value[1]==2:
                        val.append(value[0])

                if (len(val)!=0) and (new_cat_stat != None):
                    yield(new_cat_stat,sum(val))


        def mapper_sum(self, cat, value):
                yield(cat, (value))

        def reducer_sum(self, cat, values):
                new_value = []
                for val in values:
                     new_value.append(val)

                if len(new_value)!= 0:
                     yield(cat, sum(new_value))

        def mapper_order(self, cat, value):
                yield(None, (cat, value))

        def reducer_order(self, _, values):
               sorted_values = sorted(values, reverse=True, key=lambda l : l[1])
               for val in sorted_values:
                       yield(val[0], val[1])


        def steps(self):
                return [
                        MRStep(mapper = self.mapper_join, reducer = self.reducer_join),
                        MRStep(mapper = self.mapper_sum ,reducer = self.reducer_sum),
                        MRStep(mapper = self.mapper_order ,reducer = self.reducer_order)]


if __name__ == "__main__":
        correlate.run()
