import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.nonparametric.smoothers_lowess import lowess
from datetime import datetime
from pykalman import KalmanFilter

def to_timestamp(date):
    return date.timestamp()

def to_date(string):
    return datetime.strptime(string, '%Y-%m-%d %H:%M:%S.%f')


filename = sys.argv[1] 
cpu_data = pd.read_csv(filename)

cpu_data['timestamp'] = cpu_data['timestamp'].apply(to_date)

loess_smoothed = lowess(cpu_data['temperature'], cpu_data['timestamp'], frac=0.025)

kalman_data = cpu_data[['temperature', 'cpu_percent', 'sys_load_1', 'fan_rpm']]

initial_state = kalman_data.iloc[0]
observation_covariance = np.diag([0.5, 0.3, 0.3, 5]) ** 2 
transition_covariance = np.diag([0.1, 0.1, 0.1, 5]) ** 2 
transition = [[0.96,0.5,0.2,-0.001], [0.1,0.4,2.3,0], [0,0,0.96,0], [0,0,0,1]] 

kf = KalmanFilter(
    initial_state_mean=initial_state,
    initial_state_covariance=observation_covariance,
    observation_covariance=observation_covariance,
    transition_covariance=transition_covariance,
    transition_matrices=transition
)
kalman_smoothed, _ = kf.smooth(kalman_data)
plt.figure(figsize=(24, 8))
plt.xticks(rotation=25)
plt.plot(cpu_data['timestamp'], kalman_smoothed[:, 0], 'g-')
plt.plot(cpu_data['timestamp'], loess_smoothed[:,1], 'r-')
plt.plot(cpu_data['timestamp'], cpu_data['temperature'], 'b.', alpha=0.5)
plt.legend(["Kalman-smoothed", 'LOESS-smoothed', 'Data points'])
plt.savefig('cpu.svg')
