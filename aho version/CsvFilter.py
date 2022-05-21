import re
import time
import pandas as pd
import numpy as np
import ahocorasick

class CsvFilter:

    def __init__(self, onMatch, onFailure, word_list):
        self.onMatch=onMatch
        self.onFailure=onFailure
        self.word_list=word_list
        badWordsList= self.word_list.values.tolist()
        ##self.badWordsRegex = re.compile('|'.join(re.escape(x[0]) for x in badWordsList),re.IGNORECASE)
        #print(self.badWordsRegex)
        self.auto = ahocorasick.Automaton()
        for badword in badWordsList:
            self.auto.add_word(badword[0].lower(), badword[0].lower())
        self.auto.make_automaton()
        
    def FilterWords(self,chunk,n):
        if(type(chunk)==(object)):
            self.onMatch(chunk)
            self.onFailure(chunk)
            return 0
        print("start")
        df = chunk[
            chunk.apply(lambda record: len(list(self.auto.iter((record[0] + record[2] + record[4]).lower()))) != 0, raw=True,
                        axis=1)]
        list_df = [df[i:i + n] for i in range(0, df.shape[0], n)]
        chunk= (pd.merge(chunk,df, indicator=True, how='outer')
            .query('_merge=="left_only"')
            .drop('_merge', axis=1))
        list_df1 = [chunk[i:i+n] for i in range(0,chunk.shape[0],n)]
        #print(df)
        ##end = time.time()
        ##print(end-start)
        self.onMatch(list_df)
        self.onFailure(list_df1)
        
                
   