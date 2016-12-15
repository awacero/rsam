'''
Created on 11/06/2015

@author: wacero
'''
from obspy.clients.arclink import Client as ClienteArclink
from socket import error as SocketError

def conexion(USER,SERVIDOR,PUERTO):
    try:
        cliente=ClienteArclink(USER,SERVIDOR,PUERTO,debug=False)
        return cliente
    except SocketError as e:
        error="Error en conexion (args) : " + str(e)
        return error

def getFlujo(cliente,red,estacion,localizacion,canal,diaUTC,intervalo):
    try:
        flujo=cliente.get_waveforms('%s' %red, '%s' %estacion, localizacion, '%s' %canal, diaUTC,diaUTC + intervalo, compressed=False, route=False)
        return flujo
    except Exception as e:
        return "Error en getFlujo(args) : " + str(e)
