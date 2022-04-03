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
        df = chunk[chunk.apply(lambda record:self.badWordsRegex.search(record[1]) != None or self.badWordsRegex.search(record[3]) != None or self.badWordsRegex.search(record[5]) != None, raw = True, axis=1)]

        chunk= (pd.merge(chunk,df, indicator=True, how='outer')
            .query('_merge=="left_only"')
            .drop('_merge', axis=1))
        self.onMatch(df)
        self.onFailure(chunk)
        
                
                
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