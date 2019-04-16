import scipy.stats

def Correlation(x,y):
	print(scipy.stats.pearsonr(x, y))