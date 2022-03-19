import re

class CsvFilter:

    def __init__(self, onMatch, onFailure, word_list):
        self.onMatch=onMatch
        self.onFailure=onFailure
        self.word_list=word_list
        badWordsList= self.word_list.values.tolist()
        self.badWordsRegex = re.compile('|'.join(re.escape(x[0]) for x in badWordsList))
        #print(self.badWordsRegex)
    def FilterWords(self,chunk):
        if(type(chunk)==(object)):
            return 0

        for record in chunk.iterrows():
            recordunhealthy=False
            #print(record[1])
            #print(record[1].iloc[2])
            if re.search(self.badWordsRegex , record[1].iloc[0]) != None or re.match(self.badWordsRegex , record[1].iloc[2]) != None or re.match(self.badWordsRegex , record[1].iloc[4]) != None:
                recordunhealthy=True
            else:
                self.onFailure(record)
                
            if recordunhealthy:
                self.onMatch(record)

           
