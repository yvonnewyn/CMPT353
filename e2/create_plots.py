import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

filename1 = sys.argv[1] 
filename2 = sys.argv[2]

file1 = pd.read_csv(filename1, sep=' ', header=None, index_col=1,
        names=['lang', 'page', 'views', 'bytes'])
file2 = pd.read_csv(filename2, sep=' ', header=None, index_col=1,
        names=['lang', 'page', 'views', 'bytes'])

file1 = file1.sort_values(by="views", ascending=False)
file2['views2']=file1['views']
file2 = file2.sort_values(by='views', ascending=False)

plt.figure(figsize=(10,5))
plt.subplot(1, 2, 1)
plt.plot(file1['views'].values)
plt.title('Popularity Distribution')
plt.xlabel('Rank')
plt.ylabel('Views')
plt.subplot(1, 2, 2)
plt.plot(file2['views'].values, file2['views2'].values, 'b.', alpha=0.5)
plt.xscale('log')
plt.yscale('log')
plt.title('Hourly Correlation')
plt.xlabel("Hour 12 views")
plt.ylabel("Hour 13 views")
plt.savefig('wikipedia.png')