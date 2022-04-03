import string
from threading import *
import pandas as pd
import time

from pyparsing import Char


class CsvReaderAsync(Thread):
    def __init__(self, onReadChunk, onFinishReading, filename):
        super().__init__()
        self.onReadChunk = onReadChunk
        self.onFinishReading=onFinishReading
        self.filename=filename
    def read(self):

        chunksize=(10**4)
        start = time.time()
        with pd.read_csv(self.filename,na_filter=False, on_bad_lines='skip', chunksize=chunksize, encoding="ISO-8859-1",
         dtype='unicode', low_memory = False ) as reader:

            for chunk in reader:
                # end = time.time()
                print("first")
                self.onReadChunk(chunk)
                # start = time.time()
            self.onFinishReading()




    def run(self):
        self.read()