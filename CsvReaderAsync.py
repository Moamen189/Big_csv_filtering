from threading import *
import pandas as pd 

class CsvReaderAsync(Thread):
    def __init__(self, onReadChunk, onFinishReading, filename, SENTINEL):
        super().__init__()
        self.onReadChunk = onReadChunk
        self.onFinishReading=onFinishReading
        self.filename=filename
        self.SENTINEL = SENTINEL
    def read(self):
        chunksize=10**3
        with pd.read_csv(self.filename,na_filter=False, on_bad_lines='skip', chunksize=chunksize, encoding="ISO-8859-1") as reader:
            for chunk in reader:
                self.onReadChunk(chunk)
        self.onFinishReading()

    def run(self):
        self.read()