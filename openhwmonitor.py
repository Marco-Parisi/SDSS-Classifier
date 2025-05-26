"""OpenHWMonitor COM Monitoring Library

This library provides a Python interface to monitor hardware sensors using the OpenHardwareMonitor COM interface.
It leverages the wmi package to access sensor data and provides functionality to collect CPU and GPU
power, usage, and temperature readings over time.

Author: Marco Parisi
License: MIT License

Dependencies:
    - wmi
    - psutil
    - pythoncom

Usage:
    1. Instantiate the OpenHWMonitor class.
    2. Start monitoring with start_monitoring(interval=1).  The interval specifies the measurement frequency in seconds.
    3. Stop monitoring with stop_monitoring().
    4. Retrieve statistics with get_stats().

Example:
    import OpenHWMonitor
    ohwm = OpenHWMonitor.OpenHWMonitor()
    ohwm.start_monitoring(interval=2)
    time.sleep(10)  # Monitor for 10 seconds every 2 seconds (interval)
    ohwm.stop_monitoring()
    stats = ohwm.get_stats()
    print(stats)
"""

import wmi
import time
import psutil
import pythoncom
import traceback
from multiprocessing import Manager, Value, Process

class OpenHWMonitor_stats:
    def __init__(self):
        manager = Manager()
        self.cpu_power = manager.list()
        self.gpu_power = manager.list()
        self.cpu_usage = manager.list()
        self.gpu_usage = manager.list()
        self.cpu_temp = manager.list()
        self.gpu_temp = manager.list()
        self.elapsed_time = manager.list()

    def to_dict(self):
        return {
            'CPU_Power': list(self.cpu_power),
            'CPU_Usage': list(self.cpu_usage),
            'CPU_Temp': list(self.cpu_temp),
            'GPU_Power': list(self.gpu_power),
            'GPU_Usage': list(self.gpu_usage),
            'GPU_Temp': list(self.gpu_temp),
            'Elapsed_Time': list(self.elapsed_time)
        }

class OpenHWMonitor:
    def __init__(self):
        self.is_monitoring = Value('b', True)
        self.verbose = False

    @staticmethod
    def _measure(sensors, ohwm_stats):
        status=True
        
        try:
            for sensor in sensors:
                sensValue = round(sensor.Value, 1)
                if "CPU Package" in sensor.Name or "CPU Total" in sensor.Name:
                    if sensor.SensorType == u'Power':
                        ohwm_stats.cpu_power.append(sensValue)
                    elif sensor.SensorType == u'Load':
                        ohwm_stats.cpu_usage.append(int(sensValue))
                    elif sensor.SensorType == u'Temperature':
                        ohwm_stats.cpu_temp.append(sensValue)
                elif "GPU" in sensor.Name:
                    if sensor.SensorType == u'Power':
                        ohwm_stats.gpu_power.append(sensValue)
                    elif sensor.SensorType == u'Load' and "GPU Core" in sensor.Name:
                        ohwm_stats.gpu_usage.append(int(sensValue))
                    elif sensor.SensorType == u'Temperature':
                        ohwm_stats.gpu_temp.append(sensValue)
        except Exception:
            print('\033[41m' + f"--- OpenHardwareMonitor COM monitoring exception\n{traceback.format_exc()}" + '\033[0m')
            status = False
            
        return status
    
    @staticmethod
    def monitoring(interval, is_monitoring, ohwm_stats):      
        parent = psutil.Process()
        parent.nice(psutil.REALTIME_PRIORITY_CLASS)
        for child in parent.children():
            child.nice(psutil.REALTIME_PRIORITY_CLASS)
        print('\033[42m' + f"--- OpenHardwareMonitor COM monitoring process:" + '\033[0m')
        print('\033[42m' + f"--- name:{parent.name()}  -  pid:{parent.pid}" + '\033[0m')
    
        pythoncom.CoInitialize()
        w = wmi.WMI(namespace=r"root\OpenHardwareMonitor") 
        t0 = time.time()
        dt = interval
        last_measure_num = 1
        
        print('\033[42m' + "--- OpenHardwareMonitor COM monitoring started" + '\033[0m')
        
        while is_monitoring.value | last_measure_num > 0:
            if dt >= interval: 
                if OpenHWMonitor._measure(w.Sensor(), ohwm_stats):     
                    if len(ohwm_stats.elapsed_time) > 0:
                        ohwm_stats.elapsed_time.append(ohwm_stats.elapsed_time[-1] + dt)
                    else:
                        ohwm_stats.elapsed_time.append(dt)                           
                t0 = time.time()
            dt = round(time.time() - t0, 1) 
            if is_monitoring.value == False:
                last_measure_num -= 1
            
        print('\033[42m' + "--- OpenHardwareMonitor COM monitoring finished" + '\033[0m')

    def start_monitoring(self, interval=1, verbose=False):
        self.verbose = verbose
        self.interval = interval
        self.is_monitoring.value = True
        self.ohwm_stats = OpenHWMonitor_stats()
        self.process = Process(
            target = OpenHWMonitor.monitoring,
            args = (interval, self.is_monitoring, self.ohwm_stats),
            daemon = True
        )     
        self.process.start()
        
        if self.verbose:
            print('\033[42m' + "--- OpenHardwareMonitor process started" + '\033[0m')

    def stop_monitoring(self):
        self.is_monitoring.value = False
        self.process.join(self.interval + 1)
        
        if self.verbose:
            print('\033[42m' + "--- OpenHardwareMonitor process closed" + '\033[0m')

    def get_stats(self):
        return self.ohwm_stats.to_dict()
