# -*- coding: utf-8 -*-

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
#from sklearn.naive_bayes import MultinomialNB
from sklearn.svm import LinearSVC

class datasets:
    def __init__(self):
        self.data = list()
        self.target = list()
        self.target_names = set()

def load_datasets(x_file, y_file) :
    ds = datasets()
    f = open(x_file)
    for line in f.readlines():
        line=line.strip('\n')
        ds.data.append(line)
    f.close()

    f = open(y_file)
    for line in f.readlines():
        line=line.split(":")[1].strip('\n')
        ds.target.append(line)
        ds.target_names.add(line)
    f.close()

    return ds

import os
x_file = os.path.join(os.getcwd(), 'utils/x.txt')
y_file = os.path.join(os.getcwd(), 'utils/y.txt')
data_train = load_datasets(x_file, y_file)
#categories = list(data_train.target_names)

vectorizer = TfidfVectorizer(sublinear_tf=True, max_df=0.5)
x_train = vectorizer.fit_transform(data_train.data)
y_train = np.array(data_train.target)

clf = LinearSVC(loss='l2', penalty='l2', C=1000, dual=False, tol=1e-3)
#clf = MultinomialNB(alpha=.01)
clf.fit(x_train, y_train)

def classify(keywords_list):
    global vectorizer, clf
    x_test = vectorizer.transform(keywords_list)
    pred = clf.predict(x_test)
    return pred

def reload_train_datasets(keyword_category):
    global data_train, vectorizer, x_train, y_train
    for keywords, category in keyword_category.items():
        data_train.data.append(keywords)
        data_train.target.append(category)
        data_train.target_names.add(category)

    x_train = vectorizer.fit_transform(data_train.data)
    y_train = np.array(data_train.target)
    clf.fit(x_train, y_train)
