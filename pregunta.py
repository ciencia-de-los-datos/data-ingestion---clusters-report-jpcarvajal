"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import re
import pandas as pd


def ingest_data():

    filename="clusters_report.txt"

    t = open(filename, "r").readlines()

    #Columnas
    for i in range(2):
        t[i]=re.split(r"\s{2,}|\n", t[i])

    columnas = [a+" "+b for a,b in zip(t[0],t[1])]

    columnas = [x.lower().strip().replace(" ","_") for x in columnas]

    por_borrar=[]
    #Filas
    for i in range (4,len(t)):
        #Se separa a groso modo los datos por columnas
        t[i] = re.split(r"\A\s{3}|\s{4,5}|\s{13,14}|\s{10}|\s*$",t[i])
        t[i] = [a for a in t[i] if a]
        #se organizan las filas
        if len(t[i])>0:
            #Se quitan los porcentajes
            t[i]=[re.sub("\s%|","", x) for x in t[i]]
            #Se verifica si se trata de una fila "inicial" o de una complementaria
            if not t[i][0].isdigit():
                if t[i][0][:3]=="   ":
                    t[i]=[re.sub("\s{2,}"," ", x) for x in t[i]]
                    t[i]=[re.sub(".$","", x) for x in t[i]]
            #Se realiza el procesamiento para los números de las filas "iniciales"
            else:
                for x in range(len(t[i])):
                    if t[i][x].isdigit():
                        t[i][x]=int(t[i][x])
                    #Para la columna que tiene float
                    if x == 2:
                        t[i][x]=(t[i][x]).replace(",",".")
                        t[i][x]=float(t[i][x])
        else:
            #de esta forma para que las filas se borren de adelante hacia atrás y no haya que reindexar
            por_borrar.insert(0,i)

    for i in por_borrar:
        t.pop(i)

    filas = t.copy()
    por_borrar=[]

    for i in range(4,len(t)):
        if type(t[i][0]) is int:
            #En caso de que haya strings conflictivos en la fila inicial
            while len(t[i])>4:
                t[i][-2]+=" "+t[i][-1]
                t[i].pop()
            cont=1
            complemento=filas[i+cont]
            while type(complemento[0]) is not int and cont+i < len(t):
                por_borrar.insert(0,i+cont)
                #Para las strings conflictivas en filas complementarias
                if len(complemento)>1:
                    complemento=" ".join(str(x) for x in complemento)
                filas[i][3]+=(complemento)[0]
                cont+=1
                if i+cont<len(t):
                    complemento=filas[i+cont]

    for i in por_borrar:
        filas.pop(i)

    filas=filas[4:]
    for i in range(len(filas)):
        filas[i][3]=re.sub("\s{2,}"," ",filas[i][3])
        filas[i][3]=re.sub("\A\s","",filas[i][3])
    
    """for i in filas:
        print(i) """

    df = pd.DataFrame(filas, columns=columnas)
    return df

print(ingest_data().principales_palabras_clave.to_list()[0])