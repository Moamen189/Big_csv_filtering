import re

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
            return 0

        for index,record in chunk.iterrows():
            recordunhealthy=False
            if re.search(self.badWordsRegex , record[0]) != None or re.search(self.badWordsRegex , record[2]) != None or re.search(self.badWordsRegex , record[4]) != None:
                recordunhealthy=True

                
            if recordunhealthy:
                self.onMatch(record)
            else:
                self.onFailure(record)
           
