
'''
Created on 11/06/2015

@author: wacero
'''
#

#from obspy.signal.filter import highpass
#import obspy.signal.filter


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



'''
def principal(clienteArclink,red, estacion, localizacion, canal, diaUTC, intervalo):
    """
    
    Get the data streamStat of a station, calculates its RSAM and store in a DB
    
    Input: a arclink client, and estation parameters
    Get the station data streamStat
    Call rmsBanda to fill dict datosFila for rsam_banda1,2,3,4
    Fill rms field in dict datosFila
    Fill station,channel,date in dict datosFila
    Store dict datosfila in DB 
    
    """
    flujo=sda.getFlujo(clienteArclink, red, estacion, localizacion, canal, diaUTC, intervalo)
    if isinstance(flujo,streamStat.Stream):
        if len(flujo) > 0:
            rmsBanda(flujo)
            datosFila['rms']=getRMS(flujo[0].data)
            datosFila['rsam_estacion']=estacion
            datosFila['rsam_canal']=canal
            datosFila['rsam_fecha_proceso']=str(diaUTC.datetime)
            logging.info( datosFila)
            try:
                # rsamBase.insertarFila(datosFila)
                print(datosFila)
            except Exception as e:
                logging.info("Error on insertarFila(): %s" %str(e))
        else:
            insertarVacio(estacion,canal,diaUTC)
            try:
                #rsamBase.insertarFila(datosFila)
                print(datosFila)
            except Exception as e:
                logging.info("Error on insertarFila() Vacia: %s" %str(e))

            logging.info( "No hay datos disponibles, Insertando 0s")

    else:
        insertarVacio(estacion,canal,diaUTC)
        logging.info( "ERROR: %s . Insertando 0s" %flujo)

'''

def getRMS(datosFlujo):
    datosCuadrado=pow(datosFlujo,2)
    sumatorioDatos=np.int64()
    
    for datos in datosCuadrado:
        sumatorioDatos=sumatorioDatos + datos
        
    mediaDatos=np.float64()
    mediaDatos=sumatorioDatos/float(len(datosFlujo))
    rms=mediaDatos**(0.5)
    return rms


def insertarVacio(estacion,canal,diaUTC):
    datosFila['rsam_estacion']=estacion
    datosFila['rsam_canal']=canal
    datosFila['rsam_fecha_proceso']=str(diaUTC.datetime)
    datosFila['rms']=0
    datosFila['rsam_banda1']=0
    datosFila['rsam_banda2']=0
    datosFila['rsam_banda3']=0
    datosFila['rsam_banda4']=0   
        
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

    dataFilt4=filter.highpass(strSta[0].data,10.0,strSta[0].stats['sampling_rate'])
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

def readConfigFile():
    
    try:
        with open(configurationFile) as json_data_files:
            return json.load(json_data_files)
    except Exception as e:
        print("Error readConfigFile(): %s" %str(e))
        return -1
    
    
rmsData={'rsam_estacion':'','rsam_canal':'','rsam_fecha_proceso':'','rms':'','rsam_banda1':'','rsam_banda2':'','rsam_banda3':'','rsam_banda4':''}
execDir=os.path.realpath(os.path.dirname(__file__))
logging.basicConfig(filename=os.path.join(execDir,"RsamLog.log"),level=logging.DEBUG)
configurationFile='rsamConfiguration.json'



intervalo=60
#2015.6.1, MINUTO 10, 2015-06-01T00:01:00.000000Z
diaString=datetime.now().strftime('%Y-%m-%d %H:%M:00')
diaUTC=UTCDateTime(diaString) - 300
diaUTC=UTCDateTime('2016-12-16 19:08:00')
logging.info( diaUTC)
servicio='ARCLINK'
REVS_BD={'net':'EC','cod':'REVS','loc':('01','02'),'cha':('BDF',)}
REVS_HH={'net':'EC','cod':'REVS','loc':('',),'cha':('HHX','HHE')}
BREF={'net':'EC','cod':'BREF','loc':('',),'cha':('BHZ',)}
stations=(BREF,)


serviceParam=readConfigFile()
if  serviceParam == -1:
    print("Error reading configuration file: %s" %configurationFile)
    exit(-1)
    
conexion=chooseService(serviceParam[servicio])
if conexion == -1:
    print("Error connecting service: %s" %servicio)
    exit(-1)

#Parametros lista diccionario estacion,conexion,tipoServicio, dia-hora, intervalo

for sta in stations:
    for loc in sta['loc']:
        for cha in sta['cha']:
            print "%s, %s,%s, %s" %(sta['net'],sta['cod'],loc,cha)
            
            if servicio=='ARCLINK':
                try:
                    streamStat=conexion.get_waveforms(sta['net'],sta['cod'],loc,cha,diaUTC,diaUTC + 60,route=False,compressed=False)
                    print streamStat
                    #Call Rellensar dictRSAM
                except Exception as e:
                    print("Error getting streamStat: %s" %str(e))
                    continue
            else:
                try:
                    streamStat=conexion.get_waveforms(sta['net'],sta['cod'],loc,cha,diaUTC,diaUTC + 60)
                    print streamStat
                    #Call Rellensar dictRSAM
                except Exception as e:
                    print("Error getting streamStat: %s" %str(e))
                    continue
            if len(streamStat)>0:
                print("Starting calculateRSAM()")
                print fillRMS(streamStat)
                print rmsData
            else:
                print("insertar 0")
                insertarVacio(sta['cod'], cha, diaUTC)
                

                
