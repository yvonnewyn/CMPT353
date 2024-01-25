import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.nonparametric.smoothers_lowess import lowess
from datetime import datetime
from pykalman import KalmanFilter
import xml.etree.ElementTree as ET
from math import cos, asin, sqrt, pi

def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation
    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.7f' % (pt['lat']))
        trkpt.setAttribute('lon', '%.7f' % (pt['lon']))
        trkseg.appendChild(trkpt)
    
    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)
    
    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)
    
    with open(output_filename, 'w') as fh:
        doc.writexml(fh, indent=' ')

def element_to_data(elem):
    datetime = elem[0].text
    lat = float(elem.get('lat'))
    lon = float(elem.get('lon'))
    return datetime, lat, lon
    

def get_data(gpx_file):
    parse_result = ET.parse(gpx_file)
    elements = parse_result.iter('{http://www.topografix.com/GPX/1/0}trkpt')
    data= pd.DataFrame(list(map(element_to_data, elements)), columns=['datetime', 'lat', 'lon'])
    data['datetime'] = pd.to_datetime(data['datetime'], utc=True)
    return data
    
    
def distance(points):
    shifted = points.shift(periods=-1)
    get_distance_func = np.vectorize(get_distance)
    distance = get_distance_func(points['lat'], points['lon'], shifted['lat'], shifted['lon'])
    distance = distance[0:-1]
    total = sum(distance)
    return total

# Haversine Code
# Author: Salvador Dali
# Date: Feb 7, 2014
# URL: https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/21623206
def get_distance(lat1, lon1, lat2, lon2):
    p = pi/180
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p) * cos(lat2*p) * (1-cos((lon2-lon1)*p))/2
    return 1000 * 12742 * asin(sqrt(a)) #2*R*asin...

def smooth(points):
    initial_state = points.iloc[0]
    observation_covariance = np.diag([5, 5, 1, 1]) ** 2 
    transition_covariance = np.diag([1, 1, 5, 5]) ** 2 
    transition = [[1,0,6*10**-7,29*10**-7], [0,1,-43*10**-7,12*10**-7], [0,0,1,0], [0,0,0,1]] 

    kf = KalmanFilter(
        initial_state_mean=initial_state,
        initial_state_covariance=observation_covariance,
        observation_covariance=observation_covariance,
        transition_covariance=transition_covariance,
        transition_matrices=transition
    )
    kalman_smoothed, _ = kf.smooth(points)
    kalman_points = pd.DataFrame(kalman_smoothed, columns=['lat', 'lon', 'Bx', 'By'])
    return kalman_points




def main():
    input_gpx = sys.argv[1]
    input_csv = sys.argv[2]
    
    points = get_data(input_gpx).set_index('datetime')
    sensor_data = pd.read_csv(input_csv, parse_dates=['datetime']).set_index('datetime')
    points['Bx'] = sensor_data['Bx']
    points['By'] = sensor_data['By']

    dist = distance(points)
    print(f'Unfiltered distance: {dist:.2f}')

    smoothed_points = smooth(points)
    smoothed_dist = distance(smoothed_points)
    print(f'Filtered distance: {smoothed_dist:.2f}')

    output_gpx(smoothed_points, 'out.gpx')


if __name__ == '__main__':
    main()