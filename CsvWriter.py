from threading import *
import csv



class CsvWriter(Thread):
    def __init__(self,q, filename, SENTINEL):
        print("Construct")
        super().__init__()
        self.q=q
        self.filename=filename
        self.SENTINEL = SENTINEL
    def write(self):
        while(True):
            record = self.q.get()
            #print("after record")
            if(record is self.SENTINEL):
                print("sentinel")
                return 0
    # start = time.time()
            #print("write")
            """ csv_out = csv.writer(out)
            csv_out.writerow(record) """
            print("writing")
            record.to_csv(self.filename, mode="a",header=False, index=False)
            print("end writing")
    # end = time.time()
            # writeTime = end - start
            # print(f'Finished Write within  {writeTime}')



    def run(self):
        self.write()