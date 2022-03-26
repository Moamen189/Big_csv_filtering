from threading import *
import pandas as pd
import time


class CsvReaderAsync(Thread):
    def __init__(self, onReadChunk, onFinishReading, filename):
        super().__init__()
        self.onReadChunk = onReadChunk
        self.onFinishReading=onFinishReading
        self.filename=filename
    def read(self):

        chunksize=10**3*5
        start = time.time()
        with pd.read_csv(self.filename,na_filter=False, on_bad_lines='skip', chunksize=chunksize, encoding="ISO-8859-1", nrows=10000 ) as reader:
            start = time.time()

            for chunk in reader:

                # end = time.time()
                self.onReadChunk(chunk)

                # start = time.time()
            end = time.time()
            readTime = end - start
            print(f'Finished Read within  {readTime}')
            self.onFinishReading()




    def run(self):
        self.read()