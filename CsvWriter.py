from threading import *
import csv
class CsvWriter(Thread):
    def __init__(self,q, filename, SENTINEL):
        super().__init__()
        self.q=q
        self.filename=filename
        self.SENTINEL = SENTINEL
    def write(self):
        with open(self.filename, 'a', newline="") as out:
            while(True):
                record = self.q.get()
                if(record is self.SENTINEL):
                    out.close()
                    return 0
                csv_out = csv.writer(out)
                csv_out.writerow(record)
        out.close()
    def run(self):
        self.write()