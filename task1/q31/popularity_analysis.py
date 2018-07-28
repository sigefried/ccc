#! /usr/bin/python3
import pandas as pd
import sys
import numpy as np
import matplotlib.pyplot as plt


def main():
    input_path = sys.argv[1]
    content = pd.read_csv(input_path, names=['airport','popularity'], delim_whitespace=True)
    content = content.sort_values(by=['popularity'], ascending=False)
    first_count = content.popularity.iloc[0]
    popularity_series = content.popularity
    
    popularity_series = popularity_series.reset_index(drop=True)
    size = popularity_series.size
    print(size)
    zipfs = np.arange(1,size+1)
    zipfs = 1 / zipfs
    zipfs = zipfs * first_count

    plt.figure(figsize=(16,10))
    popularity_series.plot(marker='x', label="real distribution")
    plt.plot(zipfs, marker='_', label="zipf's distribution")
    plt.ylabel("number of flights")
    plt.xlabel("rank")
    plt.legend()
    plt.title("airport popularity")
    plt.show()



if __name__ == '__main__':
    plt.style.use("ggplot")
    main()
