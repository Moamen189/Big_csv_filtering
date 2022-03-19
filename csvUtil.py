from queue import Queue
from threading import *
from CsvReaderAsync import CsvReaderAsync
from CsvFilter import CsvFilter
import pandas as pd

q =  Queue()
def onReadChunk(chunk):
    q.put(chunk)

def onFinishReading():
    print("done")

def onFilterMatch(record):
    print(record[1].iloc[0], "unhealthy")
def onFilterFailure(record):
    print (record, "healthy")


def main():
    SENTINEL = object()
    badwords = pd.read_csv("./badWords.csv",header=None)
    csv_reader =  CsvReaderAsync(onReadChunk,onFinishReading,"./Hussien1.csv", SENTINEL)
    csv_reader.start()
    csv_filter = CsvFilter(onFilterMatch,onFilterFailure, badwords)
    while(True):
        data = q.get()
        csv_filter.FilterWords(data)
        if(data is SENTINEL):
            break
    print("done")
if __name__ == '__main__':
    main()
