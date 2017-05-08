#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 16:36:03 2017

@author: salman
"""

from numpy import unique
from pandas import DataFrame, merge
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk import word_tokenize
from string import punctuation
from pickle import load
from nltk.stem.snowball import EnglishStemmer

path = "/support/"
    
import jnius_config
jnius_config.set_classpath(".", path + "stemmer.jar")
from jnius import autoclass
BanglaStemmer = autoclass("RuleFileParser")



class PreProcessor:
    


    def __init__(self, lang, auto = True):
        
        self.lang = lang
        self.auto = auto
            


    def clean(self, x):
                
        if self.lang == "bn":
            
            x = x.apply(lambda x: x.decode("utf-8"))
            
            bn_num = u'\u09e6\u09e7\u09e8\u09e9\u09ea\u09eb\u09ec\u09ed\u09ee\u09ef'
            symbol = u"♥☺★②④↓→™€…৷।৳♦♣✌★≠♩♻↑←⊙♠•—–à¦®¾°§¨¿¤²¬ª¹¯¸¥·‘’“”„৲"
            
            punc = punctuation
            
            for num in bn_num:
                x = x.str.replace(num, " ")
                
            for p in punc:
                x = x.str.replace(p, " ")
            
            for sym in symbol:
                x = x.str.replace(sym, " ")
            
            x = x.str.replace("\d", " ")
            x = x.str.replace("\s{2,}", " ")
            x = x.str.strip()
        
        else:
        
            x = x.str.replace("[^a-zA-Z]", " ")
            x = x.str.replace("\s{2,}", " ")
            x = x.str.strip()
            x = x.str.lower()

                    
        return x
    

    
    def process_tag(self, questions):

        tags_dict = {}
        
        for i, j in zip(questions.iloc[:,0], questions.iloc[:,2]):
            if i not in tags_dict:
                tags_dict[i] = [j]
            else:
                temp = tags_dict[i]
                temp.append(j)
                tags_dict[i] = temp
            
        qt = DataFrame({"q_id": tags_dict.keys(), "tags": tags_dict.values()})
        
        questions = merge(questions, qt, left_on = "question_id",
                             right_on = "q_id", how = "inner")
    
        questions = questions[["question_id", "body", "tags"]]
        questions = questions.drop_duplicates("question_id", keep = "first")
        questions = questions.drop_duplicates("body", keep = "first")
        
        questions["tags"] = questions["tags"].apply(lambda x: unique(x))
        
        return questions
    


    def stopwords(self):
        
        if self.auto:
            
            if self.lang == "en":
                sw = path + "sw_en.pkl"
            elif self.lang == "bn":
                sw = path + "sw_bn.pkl"
            else:
                sw = path + "sw_ban.pkl"

            stopwords = load(open(sw, "r"))
            
        else:

            if self.lang == "en":
                stopwords = "english"
            elif self.lang == "bn":
                stopwords = load(open(path + "stopwords_bn.pkl", "r"))
            else:
                stopwords = None

        return stopwords



    def tokenize(self, doc):
        
        token = []
        
        if self.lang == "en":
            
            stemmer = EnglishStemmer(ignore_stopwords = True)
            token = [stemmer.stem(w) for w in word_tokenize(doc)]    
        
        elif self.lang == "bn":
            
            stemmer = BanglaStemmer()        
            token = [stemmer.stemOfWord(w) for w in word_tokenize(doc)]
        
        else:
            
            token = word_tokenize(doc)
            
        return token
    

    
    def extract_features(self, data, labels, unique_tags, test_per, seed,
                         lsa = False, components = 2000):
        
        train_data, test_data, train_target, test_target = train_test_split(data,
                                    labels, test_size = test_per, random_state = seed)
        
        mlb = MultiLabelBinarizer(classes = unique_tags)
        train_mlb = mlb.fit_transform(train_target)
        test_mlb = mlb.transform(test_target)
        
        tfidf_vect = TfidfVectorizer(analyzer = "word", stop_words = self.stopwords(),
                             tokenizer = self.tokenize, lowercase = False) 
        train_dtm = tfidf_vect.fit_transform(train_data)
        test_dtm = tfidf_vect.transform(test_data)
        
        if lsa:
            
            from sklearn.decomposition import TruncatedSVD
            
            lsa = TruncatedSVD(n_components = components)
            train_lsa = lsa.fit_transform(train_dtm)
            test_lsa = lsa.transform(test_dtm)
            
            return train_lsa, test_lsa, train_mlb, test_mlb
        
        else:
            
            return train_dtm, test_dtm, train_mlb, test_mlb

        