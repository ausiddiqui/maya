#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 16:32:32 2017

@author: salman
"""

from __future__ import division
from sklearn.multiclass import OneVsRestClassifier
from sklearn.ensemble import VotingClassifier
from sklearn.linear_model import SGDClassifier, LogisticRegression
from numpy import zeros, unique
from pandas import DataFrame, Series



class PostProcessor:
    

    
    def model_fit(self, train, target, seed):
        
        clf1 = SGDClassifier(loss = "log", penalty = "l1", random_state = seed))
        
        clf2 = LogisticRegression(class_weight = "balanced", solver = "newton-cg",
                                  random_state = seed))
        
        clf = OneVsRestClassifier(VotingClassifier(estimators = [('sgd', clf1),
                                        ('logreg', clf2),], voting = 'soft'))
        clf = clf.fit(train, target)
        
        self.model = clf
        
        return clf



    def predict(self, test):
        
        top = 2
        
        threshold = 0.5
        
        model = self.model
        
        pred_prob = model.predict_proba(test)
        
        prediction = zeros(pred_prob.shape)
        
        for i in range(len(prediction)):
            
            prediction[i,:] = map(lambda x: 1 if x >= threshold else 0, pred_prob[i,:])
            
            if prediction[i,:].max() < 1:
            
                t0 = pred_prob[i,:]
                t2 = set(sorted(t0)[-top:])
                prediction[i,:] = map(lambda x: 1 if x in t2 else 0, t0)
        
        return prediction
    
    
    
    def accuracy(self, actual, predicted):
        
        s = 0
        
        for p, q in zip(actual, predicted):
            
            p = set(p.nonzero()[0])
            q = set(q.nonzero()[0])
            
            acc = len(p.intersection(q)) / len(p.union(q))
            s += acc
            
        accuracy = s / len(actual)
                          
        return accuracy
    
    
    