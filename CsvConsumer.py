from threading import *
import time

class CsvConsumer(Thread):
    def __init__(self,q,process, SENTINEL, sharedDict,queue_size):
        super().__init__()
        self.queue_size = queue_size
        self.sharedDict = sharedDict
        self.q=q
        self.process=process
        self.SENTINEL = SENTINEL
    def consume(self):
        consume_times = []
        start1 =time.time()
        while (True):
            data = self.q.get()
            start = time.time()
            self.process(data,self.queue_size)
            end = time.time()
            filter_time = end-start
            consume_times.append(filter_time)
            if (data is self.SENTINEL):
                end1 = time.time()
                total_time1 = end1-start1
                print("alternative total", total_time1)
                total_time = sum(consume_times)
                self.sharedDict["process"]=total_time
                ##print('filter time',total_time)
                return
    def run(self):
        self.consume()