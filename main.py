import boto3
import pandas as pd
from decouple import config

def uploadDynamo(data):
    #Configurar las credenciales AWS
    session = boto3.Session(
        aws_access_key_id = config('Aws_access_key_id'),
        aws_secret_access_key = config('Aws_secret_access_key'),
        region_name = config('Region_name')
    )
    #Crear Instancia del cliente DynamoDB
    dynamodb = session.resource('dynamodb')
    #Nombre de la tabla
    table_name = 'python_dynamo'
    #Crear objeto de la tabla
    table = dynamodb.Table(table_name)
    #Subir los datos a Dynamo
    for item in data:
        id_tabla = item['id_tabla']
        grado = item['grado']
        nombre = item['nombre']
        apellido = item['apellido']
        edad = item['edad']

        table.put_item(
            Item = {
                'id_tabla': id_tabla,
                'grado': grado,
                'nombre': nombre,
                'apellido': apellido,
                'edad': edad
            }
        )
    print("Los datos se han subido a DynamoDB")

data = [
    {
        'id_tabla': "4",
        'grado': 'Cuarto',
        'nombre': 'Carlos',
        'apellido': 'Rodríguez',
        'edad': 10
    },
    {
        'id_tabla': "5",
        'grado': 'Quinto',
        'nombre': 'Laura',
        'apellido': 'Hernández',
        'edad': 11
    },
    {
        'id_tabla': "6",
        'grado': 'Sexto',
        'nombre': 'Pedro',
        'apellido': 'Gómez',
        'edad': 11
    },
    {
        'id_tabla': "7",
        'grado': 'Séptimo',
        'nombre': 'Sofía',
        'apellido': 'Pérez',
        'edad': 12
    },
    {
        'id_tabla': "8",
        'grado': 'Octavo',
        'nombre': 'Miguel',
        'apellido': 'Díaz',
        'edad': 13
    },
    {
        'id_tabla': "9",
        'grado': 'Noveno',
        'nombre': 'Ana',
        'apellido': 'González',
        'edad': 14
    },
    {
        'id_tabla': "10",
        'grado': 'Décimo',
        'nombre': 'Javier',
        'apellido': 'Sánchez',
        'edad': 15
    },
    {
        'id_tabla': "11",
        'grado': 'Onceavo',
        'nombre': 'Isabel',
        'apellido': 'Romero',
        'edad': 16
    },
    {
        'id_tabla': "12",
        'grado': 'Doceavo',
        'nombre': 'Diego',
        'apellido': 'Flores',
        'edad': 17
    },
    {
        'id_tabla': "13",
        'grado': 'Primero',
        'nombre': 'Sara',
        'apellido': 'Cruz',
        'edad': 8
    }
]

def readDynamo():
    # configurar las credenciales AWS
    session = boto3.Session(
        aws_access_key_id = config('Aws_access_key_id'),
        aws_secret_access_key = config('Aws_secret_access_key'),
        region_name = config('Region_name')
    )
    # Crear Instancia del cliente DynamoDB
    dynamodb = session.resource('dynamodb')
    # Nombre de la tabla
    table_name = 'python_dynamo'
    # Crear objeto de la tabla
    table = dynamodb.Table(table_name)
    #Escanear completamente
    response = table.scan()
    #Extrear la informacion de la respuesta
    result = response['Items']
    #Convertir datos de la tabla
    pf = pd.DataFrame(result)

    print(pf)

#uploadDynamo(data)
readDynamo()