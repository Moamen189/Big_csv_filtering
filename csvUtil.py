from queue import Queue
from threading import *
import pandas as pd 

class CsvReaderAsync(Thread):
    def __init__(self, q, filename, SENTINEL):
        super().__init__()
        self.q = q
        self.filename=filename
        self.SENTINEL = SENTINEL
    def read(self):
        chunksize=10**3
        with pd.read_csv(self.filename, on_bad_lines='skip', chunksize=chunksize, encoding="ISO-8859-1") as reader:
            for chunk in reader:
                self.q.put(chunk)
        self.q.put(self.SENTINEL)
        

    def run(self):
        self.read()

def main():
    SENTINEL = object()
    q =  Queue()
    csv_reader =  CsvReaderAsync(q,"./ofile.csv", SENTINEL)
    csv_reader.start()
    while(True):
        if(q.not_empty):
            data = q.get()
            if(data is SENTINEL):
                break
    print("done")
if __name__ == '__main__':
    main()
