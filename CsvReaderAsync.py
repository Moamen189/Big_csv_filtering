import string
from threading import *
import pandas as pd
import time



class CsvReaderAsync(Thread):
    def __init__(self, onReadChunk, onFinishReading, filename, nrows,chunkSize, sharedDict):
        super().__init__()
        self.sharedDict=sharedDict
        self.nrows= nrows
        self.chunkSize = chunkSize
        self.onReadChunk = onReadChunk
        self.onFinishReading=onFinishReading
        self.filename=filename
    def read(self):

        ##chunksize=(10**4)
       
        with pd.read_csv(self.filename,na_filter=False, on_bad_lines='skip', chunksize=self.chunkSize,nrows=self.nrows, encoding="ISO-8859-1",
         dtype='unicode', low_memory = False ) as reader:
            start = time.time()
            for chunk in reader:
                # end = time.time()
                print("first")
                self.onReadChunk(chunk)
                # start = time.time()
            end = time.time()
            read_time = end-start
            self.sharedDict["read"] = read_time
            print('read time', read_time)
            self.onFinishReading()
            return




    def run(self):
        self.read()