from queue import Queue
from CsvReaderAsync import CsvReaderAsync
from CsvWriter import CsvWriter
from CsvFilter import CsvFilter
from CsvConsumer import CsvConsumer
import os
import pandas as pd
from enum import Enum
import time
from pathlib import Path




SENTINEL = object()
SENTINELWriter = object()
ReaderQueue =  Queue(10)
WriterHealthyQueue = Queue(10)
WriterUnhealthyQueue = Queue(10)
path = Path(os.getcwd())
badwords = pd.read_csv(os.join(path.parent.absolute(),"badWords.csv"), header=None)
####################################################################################
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

def consume(data,queue_size):
    csv_filter.FilterWords(data,queue_size)



def main(i, chunk_size, rows_per_queue):
    sharedDict = {}
    print(i)
    csv_reader =  CsvReaderAsync(onReadChunk,onFinishReading, os.join(path.parent.absolute(),"2.csv"), chunk_size, sharedDict)
    csv_reader.start()
    csv_healthy_writer = CsvWriter(WriterHealthyQueue,os.join(os.getcwd("healthy.csv")),SENTINEL, sharedDict)
    csv_unhealthy_writer = CsvWriter(WriterUnhealthyQueue,os.join(os.getcwd("unhealthy.csv")),SENTINEL, sharedDict)
    csv_unhealthy_writer.start()
    csv_healthy_writer.start()
    csv_consumer = CsvConsumer(ReaderQueue, consume, SENTINEL, sharedDict,rows_per_queue)
    csv_consumer.start()
    ##logToXLSX({'type':'read','value':3.5},2)
    csv_reader.join()
    csv_unhealthy_writer.join()
    csv_healthy_writer.join()
    csv_consumer.join()
    
    print('read_time ',sharedDict['read'])
    print('write_time ', sharedDict['write'])
    print("process", sharedDict['process'])
    return (sharedDict['read'], sharedDict['write'], sharedDict['process'])
    sharedDict={}
    ReaderQueue.queue.clear()
    WriterHealthyQueue.queue.clear()
    WriterUnhealthyQueue.queue.clear()
