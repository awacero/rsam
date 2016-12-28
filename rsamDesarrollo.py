
import os
import logging

from obspy.core import UTCDateTime
import rsam
import DBConexion

execDir=os.path.realpath(os.path.dirname(__file__))
logging.basicConfig(filename=os.path.join(execDir,"rsam.log"),level=logging.DEBUG)

configurationFile='/home/seiscomp/git/rsam/serverConfiguration.json'
stationsFile="/home/seiscomp/git/rsam/stations.json"
server='SEEDLINK'

DB="DEV"
conexionFile='/home/seiscomp/git/rsam/DBConfiguration.json'

prmDB=rsam.readConfigFile(conexionFile)
if prmDB==-1:
    print("Error reading configuration file: %s" %conexionFile)
    exit(-1)

serviceParam=rsam.readConfigFile(configurationFile)
if  serviceParam == -1:
    print("Error reading configuration file: %s" %configurationFile)
    exit(-1)
    
conSrv=rsam.chooseService(serviceParam[server])
if conSrv == -1:
    print("Error connecting service: %s" %server)
    exit(-1)

stations=rsam.readConfigFile(stationsFile)
if stations==-1:
    print("Error reading stations file: %s" %stationsFile)
    exit(-1)

conDB=DBConexion.createConexionDB(prmDB[DB]['host'], prmDB[DB]['port'], prmDB[DB]['user'], prmDB[DB]['pass'], prmDB[DB]['DBName'])
if conDB==-1:
    print("Error in createConexionDB %s" %(prmDB[DB]))
    exit(-1)


#diaString=datetime.now().strftime('%Y-%m-%d %H:%M:00')
#diaUTC=UTCDateTime(diaString) - 60
#diaUTC=UTCDateTime('2016-12-16 19:08:00')
#diaUTC=UTCDateTime(UTCDateTime('2016-12-28 10:29:00'))


diaUTC=UTCDateTime(UTCDateTime.now().strftime("%Y-%m-%d %H:%M:00")) - 120

rsam.rsamUpdate(stations,diaUTC,server,conSrv,conDB)