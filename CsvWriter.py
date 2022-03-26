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
        with open(self.filename, 'a', newline="") as out:
            while(True):
                record = self.q.get()
                #print("after record")
                if(record is self.SENTINEL):
                    print("sentinel")
                    out.close()
                    return 0
        # start = time.time()
                #print("write")
                csv_out = csv.writer(out)
                csv_out.writerow(record)
        # end = time.time()
                # writeTime = end - start
                # print(f'Finished Write within  {writeTime}')

        out.close()


    def run(self):
        self.write()