import re
import time
import pandas as pd
import numpy as np

class CsvFilter:

    def __init__(self, onMatch, onFailure, word_list):
        self.onMatch=onMatch
        self.onFailure=onFailure
        self.word_list=word_list
        badWordsList= self.word_list.values.tolist()
        self.badWordsRegex = re.compile('|'.join(re.escape(x[0]) for x in badWordsList),re.IGNORECASE)
        #print(self.badWordsRegex)

    def FilterWords(self,chunk):
        if(type(chunk)==(object)):
            self.onMatch(chunk)
            self.onFailure(chunk)
            return 0
        print("start")
        start = time.time()
        n=1000
        df = chunk[chunk.apply(lambda record:self.badWordsRegex.search(record[0] + record[2] +record[4]) != None, raw = True, axis=1)]
        list_df = [df[i:i+n] for i in range(0,df.shape[0],n)]
        chunk= (pd.merge(chunk,df, indicator=True, how='outer')
            .query('_merge=="left_only"')
            .drop('_merge', axis=1))
        list_df1 = [chunk[i:i+n] for i in range(0,chunk.shape[0],n)]
        #print(df)
        end = time.time()
        ##print(end-start)
        self.onMatch(list_df)
        self.onFailure(list_df1)
        
                
                
        """ for index,record in chunk.iterrows():
            recordunhealthy=False
            if self.badWordsRegex.search(record[0]) != None or self.badWordsRegex.search(record[2]) != None or self.badWordsRegex.search(record[4]) != None:
                recordunhealthy=True

                
            if recordunhealthy:
                self.onMatch(record)
            else:
                self.onFailure(record)
        end = time.time()
        readTime = end - start
        print(f'Finished Read within  {readTime}') """