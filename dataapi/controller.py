import ecmwf.data as ecdata
import os
import re
import numpy as np
import xarray as xr
import pandas as pd
import json
import copy
import logging
from datetime import datetime
from ecmwf.opendata import Client
from time import sleep

downloadHours = int(os.environ.get("HOURS", default='36'))
print(downloadHours)
downloadSteps = list(range(0,downloadHours+1,6))
#downloadSteps = list(range(0,361,6))

gribDataTemplate = {
  'windspeed': {
    'filename': 'data/windspeed-probability.grib',
    'parameters': ['10v', '10u'],
    'grib_parameters': ['v10', 'u10'],
  },
  'temperature': {
    'filename': 'data/temperature-probability.grib',
    'parameters': ['2t'],
    'grib_parameters': ['t2m'],
  },
  'precipitation': {
    'filename': 'data/precipitation-probability.grib',
    'parameters': ['tp'],
    'grib_parameters': ['tp'],
  },
  'watervapor': {
    'filename': 'data/watervapor-probability.grib',
    'parameters': ['tcwv'],
    'grib_parameters': ['tcwv'],
  },
}

global gribData 
gribData = copy.deepcopy(gribDataTemplate)

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
    return result

def downloadAndLoad():
    if downloadData():
        openData()

def downloadData():
    downloadedFiles = []
    for type in gribDataTemplate:
       v = gribDataTemplate[type]
       shallDownload = False
       if not os.path.exists(v['filename']):
           shallDownload = True
       else:
           fileCreated = datetime.fromtimestamp(os.path.getmtime(v['filename']))
           if (datetime.utcnow()-fileCreated).total_seconds() > 43200:
               shallDownload = True
       if shallDownload:
           filename = re.sub('\.grib$', '_new.grib',  v['filename'])
           logging.info(f"Downloading of file {filename} started")
           result = downloadGribFiles(v['parameters'], filename)
           with open(f"{filename}.json", "w") as fp:
               json.dump(
                   {
                       'index_meta': result.for_index,
                       'meta': result.for_urls,
                   },
                   fp
               )
           downloadedFiles.append({
             'newFile':filename,
             'filename':v['filename'],
           })
           downloadedFiles.append({
             'newFile':f"{filename}.json",
             'filename':f"{v['filename']}.json",
           })
    if downloadedFiles:
        for file in downloadedFiles:
            os.rename(file['newFile'], file['filename'])
        return True
    return False


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
    logging.info("Start reading new data")
    myGribData = copy.deepcopy(gribDataTemplate)
    for dataset in myGribData:
        v = myGribData[dataset]
        if 'dataset' in v:
            v['dataset'].close()
        logging.info(f"Open data from {v['filename']}")
        v['dataset'] = xr.open_dataset(v['filename'], engine='cfgrib', filter_by_keys={'dataType': 'pf'})
        v['data'] = { param:v['dataset'][param].data for param in v['grib_parameters']}
        with open(f"{v['filename']}.json") as fp:
            v['metadata'] = json.load(fp)
    oldGribData = gribData
    gribData = myGribData
    logging.info("New data loaded")
    for dataset in oldGribData:
        if 'dataset' in v:
            v['dataset'].close()

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

def createECMWFAPIjson(myDict, param, metadata):    
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
        res[param]['steps'].append(metadata['meta']['step'][i])
        i += 1
        res['date'] = re.sub("-", "",  metadata['meta']['date'][0][0:10] )
        res['time'] = re.sub(":", "", metadata['meta']['date'][0][11:16])
    return res

def makeMeteogram(lat, lon):
    lat_index, lon_index = getIndex(lat,lon)
    t2m = createTemperatureData(lat_index,lon_index,gribData['temperature']['data']['t2m'])
    tcc = createTccData(lat_index,lon_index,gribData['watervapor']['data']['tcwv'])
    ws = createWindSpeedData(lat_index, lon_index, gribData['windspeed']['data']['v10'],gribData['windspeed']['data']['u10'])
    prec = createPrecipitationData(lat_index,lon_index,gribData['precipitation']['data']['tp'])
    result = {
        'tp': createECMWFAPIjson(prec,'tp', gribData['precipitation']['metadata']),
        '2t': createECMWFAPIjson(t2m,'2t', gribData['temperature']['metadata']),
        'tcc': createECMWFAPIjson(tcc,'tcc', gribData['watervapor']['metadata']),
        'ws': createECMWFAPIjson(ws,'ws', gribData['windspeed']['metadata'])
    }
    return result


if __name__ == "__main__":
    downloadAndLoad()
    lat = 52.27
    lon = 10.52
    print(makeMeteogram(lat, lon))
    sleep(10)

