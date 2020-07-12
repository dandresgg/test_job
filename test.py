import http.client
import json
#PANDAS
import pandas as pd
#TIMETASK
from time import time
#SHA1 CODING
import hashlib
#SQLITE3
import sqlite3
from sqlite3 import Error

'''Este fragmento de codigo esta la web proporcionada
    https://rapidapi.com/apilayernet/api/rest-countries-v1?endpoint=53aa5a0be4b0f2c975470d6b,
    para codigo python
'''

conn = http.client.HTTPSConnection("restcountries-v1.p.rapidapi.com")#Conexion establecida

headers = {
    'x-rapidapi-host': "restcountries-v1.p.rapidapi.com",
    'x-rapidapi-key': "9c22f6a8f6mshf0fe210cd5c578dp16c979jsn9fb4bf4a753f"
    }

conn.request("GET", "/all", headers=headers) #Todas la regiones
# conn.request("GET", "/region/africa", headers=headers) # Solo una region en especifico
# conn.request("GET", "/name/norge", headers=headers) # Un pais especifico

res = conn.getresponse() #Obtengo la informacion
data_t = res.read() #Formato facil de leer

'''la informacion es proporcionada en formato json'''
countries = json.loads(data_t) # Convertir en diccionaro

'''listas para almacenar informacion de parametros deseados'''
region_list = [] # region
countries_list = [] # Nombre de paises
languaje_list = [] # Lenguaje de dicho pais
time_list = [] # Tiempo de ejecucion de tarea por cada pais


def create_lists():
    '''Llenado listas'''
    for country in countries:
        region_list.append(country['region'])
        countries_list.append(country['name'])
        language = coding(country['languages'])#Pasar a sha1
        languaje_list.append(language)
        start_time = time()
        elapsed_time = time() - start_time
        time_list.append(elapsed_time*1000000)


def table_dataframe():
    '''dataframe con pandas'''
    df = pd.DataFrame({ 'Region':region_list,
                        'Country':countries_list,
                        'Language':languaje_list,
                        'time(uS)':time_list,})

    '''valores estadisticos'''
    total_time = df['time(uS)'].sum()
    average_time = df['time(uS)'].mean() 
    max_time = df['time(uS)'].max() 
    min_time = df['time(uS)'].min() 

    '''resultados'''
    print(df)
    print(f'El tiempo total fue de {total_time} us')
    print(f'El tiempo promedio fue de {average_time} us')
    print(f'El tiempo maximo fue de {max_time} us')
    print(f'El tiempo minimo fue de {min_time} us')

    return df

#Codificando lenguaje por pais
def coding(language): 
    '''mini list es porque hay paises que hablan mas de un idioma'''
    mini_list = []  
    for code_sha1 in language:
        h = hashlib.sha1(code_sha1.encode()).hexdigest()
        mini_list.append(h)
    return mini_list


def sql_connection():
    '''Crear base de datos y establecer conexion'''
    try:
        con = sqlite3.connect('mydatabase.db')
        return con
    except Error:
        print(Error)


def sql_table(con):
    '''Creacion de tabla basada en el dataframe hecho antes'''
    cursorObj = con.cursor()    
    try:
        cursorObj.execute("CREATE TABLE test(id integer PRIMARY KEY, region text, country blob, language text , time float)")
        con.commit()
    #Si la tabla ya esta creada se optiene el sqlite3.OperationalError error
    #Creo la excepsion para que el programa no pare su ejecucion
    except sqlite3.OperationalError:
        pass


def sql_insert(con, df):
    try:
        cursorObj = con.cursor()
        region = df['Region']
        country = df['Country']
        languages = df['Language']    
        time = df['time(uS)']
        for row in range(0,len(country)):  
            language = list_to_string(languages[row]) #Convertir list to string    
            entities = (row, region[row], country[row], language, time[row])
            cursorObj.execute('INSERT INTO test(id, region, country, language, time) VALUES(?, ?, ?, ?, ?)', entities)
        con.commit()
    #Si tabla ya esta llena se crea sqlite3.IntegrityError
    except sqlite3.IntegrityError:
        pass


def list_to_string(lista):
    '''sqlite3 no acepta listas como valor valido
    las convierto a tipo string para que puedan ser almacenadas'''
    new = "".join(lista)
    return new


def get_sql_to_json( con, json_str = False):
    con.row_factory = sqlite3.Row # Permite acceso por colomnas: row['column_name'] 
    db = con.cursor()

    rows = db.execute('''
    SELECT * from test
    ''').fetchall() #Consulta BD toda la tabla test

    con.commit()
    con.close()

    if json_str:
        with open('data.json', 'w') as file:
            json.dump([dict(ix) for ix in rows], file, indent=4)#Archivo creado
        return json.dumps( [dict(ix) for ix in rows] ) #JSON creado

    return rows


'''Inicio del programa'''
if __name__ == '__main__':

    create_lists()

    df = table_dataframe()#Dataframe

    '''para crear una DB se crea primero una conexion'''
    con = sql_connection()

    sql_table(con)# Crear tabla

    sql_insert(con, df)#Llenar tabla creada con valores de dataframe

    '''Crear archivo data.json'''
    get_sql_to_json(con, json_str = True)