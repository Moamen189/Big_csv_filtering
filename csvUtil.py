from queue import Queue
from CsvReaderAsync import CsvReaderAsync
from CsvWriter import CsvWriter
from CsvFilter import CsvFilter
from CsvConsumer import CsvConsumer
import pandas as pd

SENTINEL = object()
SENTINELWriter = object()
ReaderQueue =  Queue()
WriterHealthyQueue = Queue()
WriterUnhealthyQueue = Queue()
badwords = pd.read_csv("./badWords.csv", header=None)


def onReadChunk(chunk):
    ReaderQueue.put(chunk)

def onFinishReading():
    print("done")
    ReaderQueue.put(SENTINEL)
    WriterHealthyQueue.put(SENTINEL)
    WriterUnhealthyQueue.put(SENTINEL)
def onFilterMatch(record):
    print(record)
    WriterUnhealthyQueue.put(record)
def onFilterFailure(record):
    WriterHealthyQueue.put(record)

def consume(data):
    csv_filter = CsvFilter(onFilterMatch, onFilterFailure, badwords)
    csv_filter.FilterWords(data)

def main():
    csv_reader =  CsvReaderAsync(onReadChunk,onFinishReading,"./2.csv")
    csv_reader.start()
    csv_healthy_writer = CsvWriter(WriterHealthyQueue,"./healthy.csv",SENTINEL)
    csv_unhealthy_writer = CsvWriter(WriterUnhealthyQueue,"./unhealthy.csv",SENTINEL)
    csv_unhealthy_writer.start()
    csv_healthy_writer.start()
    csv_consumer = CsvConsumer(ReaderQueue, consume, SENTINEL)
    csv_consumer.start()

    print("done")
if __name__ == '__main__':
    main()
