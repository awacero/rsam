
'''
Created on 11/06/2015

@author: wacero
'''

from obspy.signal import filter
from datetime import timedelta,datetime
import numpy as np
import os
import logging

from obspy.core import UTCDateTime
from obspy.clients.arclink import Client as clientArclink
from obspy.clients.seedlink import Client as clientSeedlink
from obspy.clients.filesystem.sds import Client as clientArchive
#import rsamBase
import json


def getRMS(datosFlujo):
    """
    Calculate rms of a time series
    
    The :func: `rsam.getRMS` calculates 
    
    """
    
    datosCuadrado=[]
    sumatorioDatos=np.float64()
    mediaDatos=np.float64()
    
    arrayNP64=np.array(datosFlujo,dtype='float64')
    arrayNP64SQR=np.square(arrayNP64)    
    
    for datos in arrayNP64SQR:
        sumatorioDatos=sumatorioDatos + datos       

    mediaDatos=sumatorioDatos/float(len(datosFlujo))
    print("media: %s" %mediaDatos)
    rms=mediaDatos**(0.5)
    return rms


def insertarVacio(estacion,canal,diaUTC):
    rmsData['rsam_estacion']=estacion
    rmsData['rsam_canal']=canal
    rmsData['rsam_fecha_proceso']=str(diaUTC.datetime)
    rmsData['rms']=0
    rmsData['rsam_banda1']=0
    rmsData['rsam_banda2']=0
    rmsData['rsam_banda3']=0
    rmsData['rsam_banda4']=0   
        
def fillRMS(strSta):
    
    rmsData['rsam_estacion']=strSta[0].stats['station']
    rmsData['rsam_canal']=strSta[0].stats['channel']
    rmsData['rsam_fecha_proceso']=str(diaUTC.datetime)
    
    rmsData['rms']=getRMS(strSta[0].data)

    
    dataFilt1=filter.bandpass(strSta[0].data,0.05,0.125,strSta[0].stats['sampling_rate'])
    rmsData['rsam_banda1']=getRMS(dataFilt1)
    
    dataFilt2=filter.bandpass(strSta[0].data,2,8,strSta[0].stats['sampling_rate'])
    rmsData['rsam_banda2']=getRMS(dataFilt2)

    dataFilt3=filter.bandpass(strSta[0].data,0.25,2,strSta[0].stats['sampling_rate'])
    rmsData['rsam_banda3']=getRMS(dataFilt3)

    dataFilt4=filter.highpass(strSta[0].data,10.0,strSta[0].stats['sampling_rate'],corners=1,zerophase=True)
    rmsData['rsam_banda4']=getRMS(dataFilt4)


def chooseService(parSrv):

    if parSrv['name'] == 'ARCLINK':
        try:
            print("Trying  Arclink : %s %s %s" %(parSrv['user'],parSrv['serverIP'],parSrv['port']) )
            return clientArclink(parSrv['user'],parSrv['serverIP'],int(parSrv['port']))
        except Exception as e:
            print("Error Arclink : %s -- %s %s %s" %(str(e), parSrv['user'],parSrv['serverIP'],parSrv['port']) )
            return -1
    elif parSrv['name'] == 'SEEDLINK':
        try:
            print("Trying  Seedlink : %s %s" %(parSrv['serverIP'],parSrv['port']) )
            return clientSeedlink(parSrv['serverIP'],int(parSrv['port']) )
        except Exception as e:
            print("Error Seedlink :%s -- %s %s" %(str(e),parSrv['serverIP'],parSrv['port']) )
            return -1
    elif parSrv['name'] == 'ARCHIVE':
        try:
            print("Trying  Archive : %s " %(parSrv['archivePath']) )
            return clientArchive(parSrv['archivePath'])
        except Exception as e:
            print("Error Archive : %s -- %s" %(str(e),parSrv['archivePath']))
            return -1

def readConfigFile(jsonFile):
    
    try:
        with open(jsonFile) as json_data_files:
            return json.load(json_data_files)
    except Exception as e:
        print("Error readConfigFile(): %s" %str(e))
        return -1

def getStore(service,net,cod,loc,cha,dUTC):
    print('Starting station: %s %s %s %s' %(net,cod,loc,cha))
    if service=='ARCLINK':
        try:
            streamStat=conexion.get_waveforms(net,cod,loc,cha,dUTC,dUTC + 60,route=False,compressed=False)
        except Exception as e:
            print("Error getting streamStat: %s" %str(e))
    else:
        try:
            streamStat=conexion.get_waveforms(net,cod,loc,cha,dUTC,dUTC + 60)
        except Exception as e:
            print("Error getting streamStat: %s" %str(e))
            
    if len(streamStat)>0:
        print("Starting calculateRSAM()")
        print(streamStat)
        fillRMS(streamStat)
    else:
        print("insertar 0")
        insertarVacio(sta['cod'], cha, diaUTC)  
    
    print(rmsData) #This will be replaced by DB inserts 

    
execDir=os.path.realpath(os.path.dirname(__file__))
logging.basicConfig(filename=os.path.join(execDir,"rsam.log"),level=logging.DEBUG)

configurationFile='rsamConfiguration.json'
stationsFile="stations.json"
rmsData={'rsam_estacion':'','rsam_canal':'','rsam_fecha_proceso':'','rms':'','rsam_banda1':'','rsam_banda2':'','rsam_banda3':'','rsam_banda4':''}
servicio='ARCHIVE'

diaString=datetime.now().strftime('%Y-%m-%d %H:%M:00')
diaUTC=UTCDateTime(diaString) - 60
#diaUTC=UTCDateTime('2016-12-16 19:08:00')
diaUTC=UTCDateTime('2016-12-21 15:27:00')


serviceParam=readConfigFile(configurationFile)
if  serviceParam == -1:
    print("Error reading configuration file: %s" %configurationFile)
    exit(-1)
    
conexion=chooseService(serviceParam[servicio])
if conexion == -1:
    print("Error connecting service: %s" %servicio)
    exit(-1)

stations=readConfigFile(stationsFile)
if statons==-1:
    print("Error reading stations file: %s" %stationsFile)
    exit(-1)



for stname,sta in stations.iteritems():
    print stname
    for loc in sta['loc']:
        for cha in sta['cha']:
            print(sta['net'],sta['cod'],loc,cha)
            getStore(servicio,sta['net'],sta['cod'],loc,cha,diaUTC)
      
