import numpy as np
import pandas as pd
import time
import random
from implementations import all_implementations

def main():

    random_array = np.random.randint(-5000, 5000, 5000)
    datas = []
    for i in range(400):
        row = []
        for sort in all_implementations:
            st = time.time()
            res = sort(random_array)
            en = time.time()
            row.append(en-st)
        datas.append(row)

    data = pd.DataFrame(datas, columns=['qs1', 'qs2', 'qs3', 'qs4', 'qs5', 'merge1', 'partition_sort'])
    data.to_csv('data.csv', index=False)

if __name__ == '__main__':
    main()