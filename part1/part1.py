from mrjob.job import MRJob
from datetime import datetime
class part1(MRJob):

    #get the time feature from dataset and yield it as key, and set value as 1(1 transaction)
    def mapper(self,_,line):
        fields=line.split(",")
        try:
            #check if all fields are there
            if len(fields) ==5:
                #cast it to integer
                get_time=int(fields[2])
                #change time format from timestamp to Y-M-D
                date = datetime.fromtimestamp(get_time).strftime('%Y-%m-%B')
                time_fields = date.split("-")
                year = time_fields[0]
                month = time_fields[1]+"-"+time_fields[2]
                yield((year,month),1)
        except:
            pass

    #sort based on year and month and sum the value
    def reducer(self,key,value):
        month = key[1].split('-')
        month = month[1]
        year = key[0]
        yield("{} {} {}".format(year,month,sum(value)),None)




if __name__ == '__main__':
    part1.JOBCONF= { "mapreduce.job.reduces": "1" }
    part1.run()
