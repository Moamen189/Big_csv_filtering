from queue import Queue
from threading import *
import pandas as pd


class CsvReaderAsync(Thread):
    def __init__(self, q, filename, SENTINEL):
        super().__init__()
        self.q = q
        self.filename = filename
        self.SENTINEL = SENTINEL

    def read(self):
        chunksize = 10 ** 3
        with pd.read_csv(self.filename, on_bad_lines='skip', chunksize=chunksize, encoding="ISO-8859-1") as reader:
            for chunk in reader:
                self.q.put(chunk)
        self.q.put(self.SENTINEL)

    def run(self):
        self.read()

class filter(Thread):
    def __init__(self, q,  SENTINEL):
        super().__init__()
        self.q = q
        self.SENTINEL = SENTINEL

    def FilterBadWords(chunk, badwords):

        for record in chunk.iterrows():
            recordunhealthy = False
            for badword in badwords.iterrows():
                if badword[1].header in record[1].EMAIL:
                    recordunhealthy = True
                    break
            if recordunhealthy:
                print(record[1].EMAIL, "            Unhealthy")
            else:
                print(record[1].EMAIL, "            Healthy")

    def run(self):
        while (True):
            chunk = self.q.get()
            badwords = pd.read_csv("./badWords.csv")
            self.FilterBadWords(chunk, badwords)
            if(chunk is self.SENTINEL ):
                break








def main():
    SENTINEL = object()
    # read bad words csv
    # badwords = pd.read_csv("./badWords.csv")
    q = Queue()
    csv_reader = CsvReaderAsync(q, "./ofile.csv", SENTINEL)
    csv_reader.start()
    filterThread = filter(q , SENTINEL)
    filterThread.start()
    # while (True):
    #     if (q.not_empty):
    #         data = q.get()
    #         FilterBadWords(data,badwords)
    #         if (data is SENTINEL):
    #             break
    print("done")


if __name__ == '__main__':
    main()