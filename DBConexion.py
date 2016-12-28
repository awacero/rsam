
import pypyodbc
import json




def readConfigFile(jsonFile):
    try:
        with open(jsonFile) as json_data_files:
            return json.load(json_data_files)
    except Exception as e:
        print("Error readConfigFile(): %s" %str(e))
        return -1
    
def createConexionDB(DBHost,port,user,pwd,DBName):
    try:
        conexion=pypyodbc.connect("Driver=FreeTDS;SERVER=%s;port=%s;UID=%s;PWD=%s;DATABASE=%s" %(DBHost,port,user,pwd,DBName))
        return conexion
    except Exception as e:
        print("Error in createConexionDB: %s" %str(e))
        return -1
        
def closeConexion(conexion):
    conexion.close()
    
def queryStationID(conexion,statName):
    try:
        print("Query station ID of : %s" %statName)
        query=conexion.cursor()
        query.execute('''SELECT esta_id FROM Estacion WHERE esta_code=?;''',(statName,))
        row=query.fetchone()
        if row:
            return(row[0])
        else:
            print("QueryStationID: No station %s" %(str(statName)))
            return -1
    except Exception as e:
        print("Failed queryStationID : %s. %s" %(statName, str(e)))
        return -1
        
def insertRow(conexion,row):
    statID=queryStationID(conexion, row['rsam_estacion'])
    
   
    if statID !=-1:
        try:
            
            queryStr="INSERT INTO RSAM (esta_id_estacion,rsam_canal,rsam_fecha_utc,rsam_banda1,rsam_banda2,rsam_banda3,rsam_banda4,rsam_rms) VALUES (?,?,?,?,?,?,?,?)"
            query=conexion.cursor()
            query.execute(queryStr,(statID,row['rsam_canal'],row['rsam_fecha_proceso'],row['rsam_banda1'],row['rsam_banda2'],row['rsam_banda3'],row['rsam_banda4'],row['rms']))
            query.commit()
            print("insertRow Ok: %s" %(row))
        except Exception as e:
            print("Error in insertRow(): %s" %(str(e)))
            return -1
    else:
        print("No station %s,  no insertRow()" %(row['rsam_estacion']))

def updateRow(conexion,row):
    statID=queryStationID(conexion, row['rsam_estacion'])
    if statID != -1:
        try:
            query=conexion.cursor()
            query.execute("""UPDATE RSAM SET rsam_banda1='%s', rsam_banda2='%s', rsam_banda3='%s', rsam_banda4='%s', rsam_rms='%s' WHERE rsam_fecha_utc='%s' and esta_id_estacion='%s' and rsam_canal='%s'""" 
                          %(row['rsam_banda1'],row['rsam_banda2'],row['rsam_banda3'],row['rsam_banda4'],row['rms'],row['rsam_fecha_proceso'],statID,row['rsam_canal']))
            print("updateRow() ok: %s" %(row))
        except Exception as e:
            print("Error in updateRow(): %s" %(str(e)))
            return -1
    else:
        print("No station %s,  no updateRow()" %(row['rsam_estacion']))

