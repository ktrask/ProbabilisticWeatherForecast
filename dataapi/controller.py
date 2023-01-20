import ecmwf.data as ecdata
import os
import re
import numpy as np
import xarray as xr
import pandas as pd
from datetime import datetime
from ecmwf.opendata import Client
from time import sleep

downloadSteps = list(range(0,36,6))
#downloadSteps = list(range(0,361,6))

gribData = {
  'windspeed': {
    'filename': 'windspeed-probability.grib',
    'parameters': ['10v', '10u'],
    'grib_parameters': ['v10', 'u10'],
  },
  'temperature': {
    'filename': 'temperature-probability.grib',
    'parameters': ['2t'],
    'grib_parameters': ['t2m'],
  },
  'precipitation': {
    'filename': 'precipitation-probability.grib',
    'parameters': ['tp'],
    'grib_parameters': ['tp'],
  },
  'watervapor': {
    'filename': 'watervapor-probability.grib',
    'parameters': ['tcwv'],
    'grib_parameters': ['tcwv'],
  },
}


def downloadGribFiles(parameters, filename):
    client = Client("ecmwf", beta=True)
    result = client.retrieve(
        # date=0,
        # time=0,
        step=downloadSteps,
        stream="enfo",
        type=['cf', 'pf'],
        levtype="sfc",
        param=parameters,
        target=filename
    )

def downloadData():
    downloadedFiles = []
    for type in gribData:
       v = gribData[type]
       shallDownload = False
       if not os.path.exists(v['filename']):
           shallDownload = True
       else:
           fileCreated = datetime.fromtimestamp(os.path.getmtime(v['filename']))
           if (fileCreated-datetime.utcnow()).total_seconds() > 43200:
               shallDownload = True
       if shallDownload:
           filename = re.sub('\.grib$', '_new.grib',  v['filename'])
           downloadGribFiles(v['parameters'], filename)
           downloadedFiles.append({
             'newFile':filename,
             'filename':v['filename'],
           })
    if downloadedFiles:
        for file in downloadedFiles:
            os.rename(file['newFile'], file['filename'])


def getIndex(lat, lon):
    "lan goes from 90 to -90"
    "lon goes from -180 to 180"
    lat_center = 225 #451 values in total
    lon_center = 450 # 900 in total
    grid_size = 0.4
    lat_max = 451
    lon_max = 900
    lon_index = lon_center + round(lon/grid_size)
    lat_index = (lat_center-round(lat/0.4))
    if lat_index < 0:
        lat_index = 0
    elif lat_index > lat_max:
        lat_index = lat_max
    if lon_index < 0:
        lon_index = 0
    elif lon_index > lon_max:
        lon_index = lon_max
    return lat_index, lon_index

    
def openData():
    for dataset in gribData:
        v = gribData[dataset]
        if 'dataset' in v:
            v['dataset'].close()
        v['dataset'] = xr.open_dataset(v['filename'], engine='cfgrib', filter_by_keys={'dataType': 'pf'})
        v['data'] = { param:v['dataset'][param].data for param in v['grib_parameters']}

def createTemperatureData(lat_index, lon_index, data):
    local_data = data[:,:,lat_index, lon_index]
    result = {}
    for step in range(0,len(local_data[0])):
        myTemps = local_data[0:50,step]
        result[step] = np.quantile(myTemps, [0,0.1,0.25,0.5,0.75,0.9,1])
    return(result)

def createTccData(lat_index, lon_index, data):
    local_data = data[:,:,lat_index, lon_index]
    result = {}
    for step in range(0,len(local_data[0])):
        tcc = local_data[0:50,step]/24
        tcc[tcc > 1] = 1
        result[step] = np.quantile(tcc, [0,0.1,0.25,0.5,0.75,0.9,1]).tolist()
    return(result)

def createWindSpeedData(lat_index, lon_index, wind_v, wind_u):
    local_v = wind_v[:,:,lat_index, lon_index]
    local_u = wind_u[:,:,lat_index, lon_index]
    result = {}
    for step in range(0,len(local_v[0])):
        myUs = local_v[0:50,step]
        myVs = local_u[0:50,step]
        speeds = np.sqrt(myUs**2+myVs**2)
        result[step] = np.quantile(speeds, [0,0.1,0.25,0.5,0.75,0.9,1]).tolist()
    return(result)

def createPrecipitationData(lat_index, lon_index, data):
    local_data = data[:,:,lat_index, lon_index]
    precipitation = np.diff(local_data,prepend=0)
    result = {}
    for step in range(0,len(local_data[0])):
        myTemps = precipitation[0:50,step]
        result[step] = np.quantile(myTemps, [0,0.1,0.25,0.5,0.75,0.9,1]).tolist()
    return result

def createECMWFAPIjson(myDict, param):    
    res = {param: {
        "min": [],
        "max": [],
        "median": [],
        "ninety": [],
        "seventy_five": [],
        "ten": [],
        "twenty_five": [],
        "steps": [],
    }}

    i = 0
    for step in myDict:
        #print(prec[step])
        res[param]['min'].append(myDict[step][0])
        res[param]['ten'].append(myDict[step][1])
        res[param]['twenty_five'].append(myDict[step][2])
        res[param]['median'].append(myDict[step][3])
        res[param]['seventy_five'].append(myDict[step][4])
        res[param]['ninety'].append(myDict[step][5])
        res[param]['max'].append(myDict[step][6])
        res[param]['steps'].append(str(i))
        i += 6
    return res

def makeMeteogram(lat, lon):
    lat_index, lon_index = getIndex(lat,lon)
    t2m = createTemperatureData(lat_index,lon_index,gribData['temperature']['data']['t2m'])
    tcc = createTccData(lat_index,lon_index,gribData['watervapor']['data']['tcwv'])
    ws = createWindSpeedData(lat_index, lon_index, gribData['windspeed']['data']['v10'],gribData['windspeed']['data']['u10'])
    prec = createPrecipitationData(lat_index,lon_index,gribData['precipitation']['data']['tp'])
    result = {
        'prec': createECMWFAPIjson(prec,'prec'),
        't2m': createECMWFAPIjson(t2m,'t2m'),
        'tcc': createECMWFAPIjson(tcc,'tcc'),
        'ws': createECMWFAPIjson(ws,'ws')
    }
    return result


if __name__ == "__main__":
    downloadData()
    openData()
    lat = 52.27
    lon = 10.52
    print(makeMeteogram(lat, lon))
    sleep(10)

