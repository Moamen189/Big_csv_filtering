from queue import Queue
from CsvReaderAsync import CsvReaderAsync
from CsvWriter import CsvWriter
from CsvFilter import CsvFilter
from CsvConsumer import CsvConsumer
import pandas as pd

SENTINEL = object()
SENTINELWriter = object()
ReaderQueue =  Queue(10)
WriterHealthyQueue = Queue(10)
WriterUnhealthyQueue = Queue(10)
badwords = pd.read_csv("./badWords.csv", header=None)

############################################################
def onReadChunk(chunk):
    ReaderQueue.put(chunk)
def onFinishReading():
    print("done")
    ReaderQueue.put(SENTINEL)
def onFilterMatch(record):
    #print("UNHEALTHY")
    if(type(record)!=object):
        list(map(WriterUnhealthyQueue.put,record))
    else:
        WriterUnhealthyQueue.put(record)
def onFilterFailure(record):
    #print (record)
    if(type(record)!=object):
        list(map(WriterHealthyQueue.put,record))
    else:
        WriterHealthyQueue.put(record)
    #print("after healthy record")

csv_filter = CsvFilter(onFilterMatch, onFilterFailure, badwords)

def consume(data):
    csv_filter.FilterWords(data)

def main():

    csv_reader =  CsvReaderAsync(onReadChunk,onFinishReading, "./Hussien1.csv")
    csv_reader.start()
    csv_healthy_writer = CsvWriter(WriterHealthyQueue,"./healthy.csv",SENTINEL)
    csv_unhealthy_writer = CsvWriter(WriterUnhealthyQueue,"./unhealthy.csv",SENTINEL)
    csv_unhealthy_writer.start()
    csv_healthy_writer.start()
    csv_consumer = CsvConsumer(ReaderQueue, consume, SENTINEL)
    csv_consumer.start()

    #print("all done")
if __name__ == '__main__':
    main()
