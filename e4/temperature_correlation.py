import os
import pathlib
import sys
import numpy as np
import pandas as pd
from math import cos, asin, sqrt, pi
import matplotlib.pyplot as plt

def distance(city, stations):
    get_distance_func = np.vectorize(get_distance)
    distances = get_distance_func(city['latitude'], city['longitude'], stations['latitude'], stations['longitude'])
    return distances
    
def best_tmax(city, stations):
    distances = distance(city, stations)
    index = np.argmin(distances)
    return stations._get_value(index, 'avg_tmax')
    
# Haversine Code
# Author: Salvador Dali
# Date: Feb 7, 2014
# URL: https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/21623206
def get_distance(lat1, lon1, lat2, lon2):
    p = pi/180
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p) * cos(lat2*p) * (1-cos((lon2-lon1)*p))/2
    return 1000 * 12742 * asin(sqrt(a)) #2*R*asin...

def main():
    stations_file = sys.argv[1]
    city_file = sys.argv[2]
    plot_filename = sys.argv[3]

    stations = pd.read_json(stations_file, lines=True)
    city = pd.read_csv(city_file)

    stations['avg_tmax'] = stations['avg_tmax']/10
    city = city.dropna()
    city['area'] = city['area']/1000000
    city = city[city['area']<=10000]
    city['population_density'] = city['population']/(city['area'])

    city['best_tmax'] = city.apply(lambda x: best_tmax(x, stations), axis=1)

    plt.plot(city['best_tmax'], city['population_density'], 'b.', alpha=0.5)
    plt.title('Temperature vs Population Density')
    plt.xlabel("Avg Max Temperature (\u00b0C)")
    plt.ylabel("Population Density (people/km\u00b2)")
    plt.savefig(plot_filename)





main()