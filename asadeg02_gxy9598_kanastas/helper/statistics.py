import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from math import sqrt
from random import shuffle
import time



class statistics():
    

    @staticmethod
    def permute(x):
        shuffled = [xi for xi in x]
        shuffle(shuffled)
        return shuffled

    @staticmethod
    def avg(x):  # Average
        return sum(x) / len(x)

    @staticmethod
    def stddev(x):  # Standard deviation.
        m = statistics.avg(x)
        return sqrt(sum([(xi - m) ** 2 for xi in x]) / len(x))

    @staticmethod
    def cov(x, y):  # Covariance.
        return sum([(xi - statistics.avg(x)) * (yi - statistics.avg(y)) for (xi, yi) in zip(x, y)]) / len(x)

    @staticmethod
    def corr(x, y):  # Correlation coefficient.
        if statistics.stddev(x) * statistics.stddev(y) != 0:
            return statistics.cov(x, y) / (statistics.stddev(x) * statistics.stddev(y))

    @staticmethod            
    def p(x, y):
        c0 = statistics.corr(x, y)
        corrs = []
        for k in range(0, 2000):
            y_permuted = statistics.permute(y)
            corrs.append(statistics.corr(x, y_permuted))
        return len([c for c in corrs if abs(c) >= abs(c0)])/len(corrs)   
