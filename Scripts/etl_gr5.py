import pandas as pd
import re
from sqlalchemy import create_engine


def insert_into_db(df, table_name, engine):
    """
    Función que inserta un DataFrame en una tabla de una base de datos usando SQLAlchemy.

    Args:
        df (pandas.DataFrame): DataFrame a insertar.
        table_name (str): Nombre de la tabla en la base de datos.
        engine (sqlalchemy.engine.base.Engine): Objeto de conexión a la base de datos.

    Returns:
        None
    """
    df.to_sql(table_name, engine, if_exists='append', index=False)


def main():
    """
    Función principal que ejecuta el proceso de carga de datos en la base de datos.
    """

    # Configurar la conexión a la base de datos
    engine = create_engine(
        'postgresql://postgres:toortoor@192.168.0.3:5433/gr5')

    # Cargar el DataFrame desde el archivo CSV
    df = pd.read_csv('dataset.csv')

    # Dataframe cliente para la dimensión cliente
    dfCliente = df[['num_documento_cliente', 'tipo_documento_cliente']].copy()
    dfCliente['num_documento_cliente'] = dfCliente['num_documento_cliente'].apply(
        lambda x: x if x > 0 else x * -1)
    dfCliente['num_documento_cliente'] = dfCliente['num_documento_cliente'].astype(
        'int64')
    dfCliente.sort_values(by='num_documento_cliente', inplace=True)

    # Contar el número de ocurrencias por cliente y tipo de documento
    count_dfCliente = dfCliente.groupby(
        ['num_documento_cliente', 'tipo_documento_cliente']).size().reset_index(name='count')
    count_dfCliente.sort_values(
        by=['num_documento_cliente', 'tipo_documento_cliente', 'count'], inplace=True)
    count_dfCliente.drop_duplicates(
        subset='num_documento_cliente', inplace=True)

    # Seleccionar las columnas necesarias para la dimensión cliente
    dfCliente = count_dfCliente[[
        'num_documento_cliente', 'tipo_documento_cliente']]

    # Insertar el DataFrame en la tabla dtCliente
    insert_into_db(df=dfCliente, table_name='dtCliente', engine=engine)

    # Dataframe ubicación para la dimensión ubicación
    dfUbicacion = df[['codigo_tienda', 'tipo_tienda', 'id_barrio',
                      'nombre_barrio', 'latitud_tienda', 'longitud_tienda']].copy()
    dfUbicacion.sort_values(
        by=['codigo_tienda', 'tipo_tienda', 'id_barrio'], inplace=True)

    # Contar el número de ocurrencias por ubicación
    count_dfUbicacion = dfUbicacion.groupby(['codigo_tienda', 'tipo_tienda', 'id_barrio',
                                            'nombre_barrio', 'latitud_tienda', 'longitud_tienda']).size().reset_index(name='count')
    count_dfUbicacion.sort_values(by=['codigo_tienda', 'tipo_tienda', 'id_barrio',
                                  'nombre_barrio', 'latitud_tienda', 'longitud_tienda', 'count'], inplace=True)
    count_dfUbicacion.drop_duplicates(subset='codigo_tienda', inplace=True)

    # Seleccionar las columnas necesarias para la dimensión ubicación
    dfUbicacion = count_dfUbicacion[['codigo_tienda', 'tipo_tienda',
                                     'id_barrio', 'nombre_barrio', 'latitud_tienda', 'longitud_tienda']]

    # Insertar el DataFrame en la tabla dtUbicacion
    insert_into_db(df=dfUbicacion, table_name='dtUbicacion', engine=engine)

    # Dateframe tiempo para la dimensión tiempo
    dfTiempo = df[['fecha_compra']].copy()
    dfTiempo['fecha_compra'] = dfTiempo['fecha_compra'].apply(lambda x: x[:10])
    dfTiempo['fecha_id'] = dfTiempo['fecha_compra'].apply(
        lambda x: ''.join(x.split('-')))
    dfTiempo['fecha_compra'] = pd.to_datetime(
        dfTiempo['fecha_compra'], format='%Y-%m-%d')
    dfTiempo['anio'] = dfTiempo['fecha_compra'].dt.year
    dfTiempo['mes'] = dfTiempo['fecha_compra'].dt.month
    dfTiempo['dia'] = dfTiempo['fecha_compra'].dt.day
    dfTiempo.drop_duplicates(subset='fecha_id', inplace=True)
    dfTiempo = dfTiempo[['fecha_id', 'fecha_compra', 'anio', 'mes', 'dia']]
    dfTiempo.sort_values(by='fecha_id', inplace=True)

    # Insertar el DataFrame en la tabla dtTiempo
    insert_into_db(df=dfTiempo, table_name='dtTiempo', engine=engine)

    # Dateframe ventas para la tabla de hechos
    dfVentas = df[['num_documento_cliente', 'codigo_tienda',
                   'fecha_compra', 'total_compra']].copy()
    dfVentas['num_documento_cliente'] = dfVentas['num_documento_cliente'].apply(
        lambda x: x if x > 0 else x * -1)
    dfVentas['num_documento_cliente'] = dfVentas['num_documento_cliente'].astype(
        'int64')
    dfVentas['fecha_id'] = dfVentas['fecha_compra'].apply(
        lambda x: ''.join(re.findall(r'([\d]+)', x[:10])))

    # Seleccionar las columnas necesarias para la tabla de hechos
    dfVentas = dfVentas[['num_documento_cliente',
                         'codigo_tienda', 'fecha_id', 'total_compra']]

    # Insertar el DataFrame en la tabla ftVentas
    insert_into_db(df=dfVentas, table_name='ftVentas', engine=engine)


if __name__ == "__main__":
    main()
