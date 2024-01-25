import os
import pathlib
import sys
import numpy as np
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET


def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation, parse
    xmlns = 'http://www.topografix.com/GPX/1/0'
    
    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.10f' % (pt['lat']))
        trkpt.setAttribute('lon', '%.10f' % (pt['lon']))
        time = doc.createElement('time')
        time.appendChild(doc.createTextNode(pt['datetime'].strftime("%Y-%m-%dT%H:%M:%SZ")))
        trkpt.appendChild(time)
        trkseg.appendChild(trkpt)

    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)

    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)

    doc.documentElement.setAttribute('xmlns', xmlns)

    with open(output_filename, 'w') as fh:
        fh.write(doc.toprettyxml(indent='  '))


def element_to_data(elem):
    datetime = elem[1].text
    lat = float(elem.get('lat'))
    lon = float(elem.get('lon'))
    return datetime, lat, lon

def get_data(gpx_file):
    parse_result = ET.parse(gpx_file)
    elements = parse_result.iter('{http://www.topografix.com/GPX/1/0}trkpt')
    data= pd.DataFrame(list(map(element_to_data, elements)), columns=['datetime', 'lat', 'lon'])
    data['datetime'] = pd.to_datetime(data['datetime'], utc=True)
    return data


def main():
    input_directory = pathlib.Path(sys.argv[1])
    output_directory = pathlib.Path(sys.argv[2])
    
    accl = pd.read_json(input_directory / 'accl.ndjson.gz', lines=True, convert_dates=['timestamp'])[['timestamp', 'x']]
    gps = get_data(input_directory / 'gopro.gpx')
    phone = pd.read_csv(input_directory / 'phone.csv.gz')[['time', 'gFx', 'Bx', 'By']]

    first_time = accl['timestamp'].min()
    
    # TODO: create "combined" as described in the exercise

    first_time = accl['timestamp'].min()
    phone['timestamp'] = first_time + pd.to_timedelta(phone['time'], unit='sec')
    phone['timestamp'] = phone['timestamp'].dt.round(freq='4S')
    phone = phone.groupby('timestamp').mean()

    gps['datetime'] = gps['datetime'].dt.round(freq='4S')
    gps = gps.groupby('datetime').mean()

    accl['timestamp'] = accl['timestamp'].dt.round(freq='4S')
    accl = accl.groupby('timestamp').mean()

    best_offset = -5
    largest_cross_corr = 0
    accl2 = pd.read_json(input_directory / 'accl.ndjson.gz', lines=True, convert_dates=['timestamp'])[['timestamp', 'x']]
    first_time = accl2['timestamp'].min()


    for offset in np.linspace(-5.0, 5.0, 101):
        phone2 = pd.read_csv(input_directory / 'phone.csv.gz')[['time', 'gFx', 'Bx', 'By']]
        phone2['timestamp'] = first_time + pd.to_timedelta(phone2['time'], unit='sec')
        phone2['timestamp'] = phone2['timestamp'] + pd.to_timedelta(offset, unit='S')
        phone2['timestamp'] = phone2['timestamp'].dt.round(freq='4S')
        phone2 = phone2.groupby('timestamp').mean()
        phone2 = phone2.join(accl)
        phone2 = phone2.dropna()
        
        cross_corr = phone2['gFx']*phone2['x']
        if cross_corr.sum() > largest_cross_corr:
            largest_cross_corr = cross_corr.sum()
            best_offset = offset

    combined = phone2.join(gps)
    combined['datetime'] = combined.index

    print(f'Best time offset: {best_offset:.1f}')
    os.makedirs(output_directory, exist_ok=True)
    output_gpx(combined[['datetime', 'lat', 'lon']], output_directory / 'walk.gpx')
    combined[['datetime', 'Bx', 'By']].to_csv(output_directory / 'walk.csv', index=False)
    
    


main()
