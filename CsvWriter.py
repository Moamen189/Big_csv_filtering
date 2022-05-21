from threading import *
import csv
import time



class CsvWriter(Thread):
    def __init__(self,q, filename, SENTINEL, sharedDict):
        print("Construct")
        super().__init__()
        self.sharedDict=sharedDict
        self.q=q
        self.filename=filename
        self.SENTINEL = SENTINEL
    def write(self):
        write_times=[]        
        while(True):
            record = self.q.get()
            #print("after record")
            if(record is self.SENTINEL):
                sum_time = sum(write_times)
                if('write' in self.sharedDict):
                    self.sharedDict["write"]+=sum_time
                else:
                    self.sharedDict["write"] = sum_time
                print("sentinel")
                return 0
    # start = time.time()
            #print("write")
            """ csv_out = csv.writer(out)
            csv_out.writerow(record) """
            start = time.time()
            record.to_csv(self.filename, mode="a",header=False, index=False)
            end = time.time()
            write_time = end-start
            write_times.append(write_time)
    # end = time.time()
            # writeTime = end - start
            # print(f'Finished Write within  {writeTime}')



    def run(self):
        self.write()