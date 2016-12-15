
'''
Created on 11/06/2015

@author: wacero
'''
import obspy.signal
#import rsamBase
import requestDataArclink as sda
from obspy.core import UTCDateTime
from obspy.clients.arclink import Client
from obspy.core import stream
from scipy.signal import butter, lfilter
from datetime import timedelta,datetime
import numpy as np
import os
import logging

from obspy.signal.filter import highpass
execDir=os.path.realpath(os.path.dirname(__file__))
logging.basicConfig(filename=os.path.join(execDir,"RsamLog.log"),level=logging.DEBUG)

'''
#Eliminar 
perdelta() se usaba
para generar varios dias de datos 
def perdelta(start,end,delta):
    current=start
    while current < end:
        yield current
        current +=delta
'''

'''
for dia in perdelta(datetime(2015,6,1,0,0),datetime(2015,6,1,0,10),timedelta(minutes=1)):
    diaUTC=UTCDateTime(dia)
    print diaUTC
    #principal(clienteArclink,red,estacion,localizacion,canal,diaUTC,intervalo)
'''


'''
#Eliminar
#Se usaba en rsamBanda()
def rangoFreq(datosFlujo,tasa, frecuenciaBaja, frecuenciaAlta):

    deltaFrecuencia=tasa/len(datosFlujo)
    vectorFrecuencia=np.linspace(0,(len(datosFlujo)-1)*deltaFrecuencia,len(datosFlujo))
    rangoFrecuencia=np.where((frecuenciaBaja < vectorFrecuencia) & ( frecuenciaAlta > vectorFrecuencia))
    return rangoFrecuencia
'''

'''
#Eliminar
#Se usaba en rsamBanda()
 
def amplitudFrecuencia(datosFlujo,tasa):
    """
    Get amplitud in frequence domain
    
    Use the fft to get the spectra of a time series
    gets the absolute value, divide to the sample rate
    and return this value
    """

    from numpy import fft
    espectro=fft.fft(datosFlujo)
    absEspectro=abs(espectro)
    #amplitud=absEspectro/tasa
    amplitud=absEspectro
    return amplitud
'''


def getRMS(datosFlujo):
    datosCuadrado=pow(datosFlujo,2)
    sumatorioDatos=np.int64()
    for datos in datosCuadrado:
        sumatorioDatos=sumatorioDatos + datos

    mediaDatos=np.float64()
    mediaDatos=sumatorioDatos/float(len(datosFlujo))
    logging.info( "MEDIA : %s" %mediaDatos)
    rms=mediaDatos**(0.5)
    return rms

def butter_bandpass(lowcut, highcut, fs, order=5):
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    b, a = butter(order, [low, high], btype='band')
    return b, a

def butter_bandpass_filter(data, lowcut, highcut, fs, order=5):
    b, a = butter_bandpass(lowcut, highcut, fs, order=order)
    y = lfilter(b, a, data)
    return y

def pasoAlto(traza):
    trazaFiltrado=traza.copy()
    
    #trazaFiltrado.data=obspy.signal.highpass(trazaFiltrado.data,10.0,corners=1,zerophase=True,df=traza.stats.sampling_rate)
    trazaFiltrado.data=highpass(trazaFiltrado.data, 10.0,df=traza.stats.sampling_rate, corners=1, zerophase=True)
    return trazaFiltrado


def rmsBanda(flujo):
    tasa=flujo[0].stats['sampling_rate']
    datos=flujo[0].data
    '''
    #amplitudFreq=amplitudFrecuencia(datos,tasa)
    #bandas={'rsam_banda1':(0.1,7),'rsam_banda2':(2,8), 'rsam_banda3':(5,10),'rsam_banda4':(7,20)}
    #bandas={'rsam_banda1':(0.05,0.125),'rsam_banda2':(2,8), 'rsam_banda3':(0.25,2),'rsam_banda4':(7,20)}
    '''
    bandas={'rsam_banda1':(0.05,0.125),'rsam_banda2':(2,8), 'rsam_banda3':(0.25,2)}

    for banda,freqs in bandas.iteritems():
        '''
        rango=rangoFreq(datos,tasa,freqs[0],freqs[1])
        amplitudFreqRango=amplitudFreq[rango]
        amplitudFreqRangoCuadrado=pow(amplitudFreqRango,2)
        amplitudFreqRangoCuadradoSumatorio=amplitudFreqRangoCuadrado.sum()
        rmsBanda=amplitudFreqRangoCuadradoSumatorio**(0.5)/len(amplitudFreqRango)
        datosFila[banda]=rmsBanda
        '''
        logging.info( "banda %s,%s " %(freqs[0],freqs [1]))
        datosFiltrado=butter_bandpass_filter(datos,freqs[0],freqs[1],tasa,4)
        datosFila[banda]=getRMS(datosFiltrado)

    datosFiltradoHigh=pasoAlto(flujo[0])
    datosFila['rsam_banda4']=getRMS(datosFiltradoHigh.data)


def insertarVacio(estacion,canal,diaUTC):
    datosFila['rsam_estacion']=estacion
    datosFila['rsam_canal']=canal
    datosFila['rsam_fecha_proceso']=str(diaUTC.datetime)
    datosFila['rms']=0
    datosFila['rsam_banda1']=0
    datosFila['rsam_banda2']=0
    datosFila['rsam_banda3']=0
    datosFila['rsam_banda4']=0

def principal(clienteArclink,red, estacion, localizacion, canal, diaUTC, intervalo):
    flujo=sda.getFlujo(clienteArclink, red, estacion, localizacion, canal, diaUTC, intervalo)
    if isinstance(flujo,stream.Stream):
        if len(flujo) > 0:
            datos=flujo[0].data
            rmsBanda(flujo)
            datosFila['rms']=getRMS(datos)
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
        #exit(1)


#datosFila={'rsam_estacion':[],'rsam_canal':[],'rsam_fecha_ingreso':[],'rsam_fecha_proceso':[],'rsam_banda1':[]}
#valoresBanda={'banda 1':[],'banda 2':[],'banda 3':[],'banda 4':[] }
#valoresBanda={'banda 1':'','banda 2':'','banda 3':[],'banda 4':[] }
datosFila={'rsam_estacion':'','rsam_canal':'','rsam_fecha_proceso':'','rms':'','rsam_banda1':'','rsam_banda2':'','rsam_banda3':'','rsam_banda4':''}


servidor='192.168.1.163'
puerto='18001'

clienteArclink=sda.ClienteArclink('rsam',servidor,puerto)
if isinstance(clienteArclink,Client):
    logging.info( "conexion Arclink OK")
else:
    logging.info( "ERROR: %s" %clienteArclink)
    exit(1)


red='EC'
#Estaciones BHZ
#estaciones=('BMAS','BREF','BTAM','BNAS','BRRN','TOMA','SRAM','SUCR','BVC2')
estaciones=('BMAS','BREF')
localizacion=''
canal='BHZ'
intervalo=60

#2015.6.1, MINUTO 10, 2015-06-01T00:01:00.000000Z
diaString=datetime.now().strftime('%Y-%m-%d %H:%M:00')
diaUTC=UTCDateTime(diaString) - 300
logging.info( diaUTC)


for estacion in estaciones:
    principal(clienteArclink,red,estacion,localizacion,canal,diaUTC,intervalo)


'''
##Estaciones HHZ
estaciones=('VCES','SLOR','ANGU','CAYA','BRTU','POND','SAGA','ANTS','FER2','PULU','IMBA','COTA','CUIC','BTER','REVN','REVS')
localizacion=''
canal='HHZ'
intervalo=60

#2015.6.1, MINUTO 10, 2015-06-01T00:01:00.000000Z
diaString=datetime.now().strftime('%Y-%m-%d %H:%M:00')
diaUTC=UTCDateTime(diaString) - 300
logging.info( diaUTC)

for estacion in estaciones:
    principal(clienteArclink,red,estacion,localizacion,canal,diaUTC,intervalo)

##ESTACIONES SHZ
estaciones=('RETU','PINO','CAYR','YANA','ECEN','CONE')
localizacion=''
canal='SHZ'
intervalo=60

#2015.6.1, MINUTO 10, 2015-06-01T00:01:00.000000Z
diaString=datetime.now().strftime('%Y-%m-%d %H:%M:00')
diaUTC=UTCDateTime(diaString) - 420
logging.info( diaUTC)

for estacion in estaciones:
    logging.info( estacion)
    principal(clienteArclink,red,estacion,localizacion,canal,diaUTC,intervalo)



#    rsamBase.insertarFila(datosFila)
'''