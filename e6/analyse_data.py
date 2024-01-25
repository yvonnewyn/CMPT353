from scipy import stats
import numpy as np
import pandas as pd
from statsmodels.stats.multicomp import pairwise_tukeyhsd

def main():
    data = pd.read_csv('data.csv')

    anova = stats.f_oneway(data['qs1'], data['qs2'], data['qs3'], data['qs4'], data['qs5'], data['merge1'], data['partition_sort'])
    print("ANOVA p-value: ", anova.pvalue)

    x_data = pd.DataFrame({'qs1': data['qs1'],
                        'qs2': data['qs2'],
                        'qs3': data['qs3'],
                        'qs4': data['qs4'],
                        'qs5': data['qs5'],
                        'merge1': data['merge1'],
                        'partition_sort': data['partition_sort']})
    x_melt = pd.melt(x_data)
    posthoc = pairwise_tukeyhsd(x_melt['value'], x_melt['variable'], alpha=0.05)
    print(posthoc)

    fig = posthoc.plot_simultaneous()
    fig.savefig("fig.png")
    print("Means of each sorting algorithm: ")
    print(data['qs1'].mean(), data['qs2'].mean(), 
        data['qs3'].mean(), data['qs4'].mean(), 
        data['qs5'].mean(), data['merge1'].mean(), 
        data['partition_sort'].mean())
    

if __name__ == '__main__':
    main()