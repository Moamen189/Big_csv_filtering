from threading import *
class CsvConsumer(Thread):
    def __init__(self,q,process, SENTINEL):
        super().__init__()
        self.q=q
        self.process=process
        self.SENTINEL = SENTINEL
    def consume(self):
        while (True):
            data = self.q.get()
            print("after cosume redord")
            self.process(data)
            if (data is self.SENTINEL):
                return 0
    def run(self):
        self.consume()