import pandas as pd
from sqlalchemy import create_engine


def get_top_5_barrios(engine):
    """
    Obtiene los 5 barrios con la mayor cantidad de clientes únicos en tiendas tipo 'Tienda Regional'.

    Args:
        engine (sqlalchemy.engine.base.Engine): Objeto de conexión a la base de datos.

    Returns:
        pandas.DataFrame: DataFrame que contiene los 5 barrios con la mayor cantidad de clientes únicos.
    """

    # Consulta SQL para obtener los 5 barrios con la mayor cantidad de clientes únicos
    query = """
    SELECT
        u.nombre_barrio
        ,COUNT(DISTINCT v.num_documento_cliente) AS unique_clients
    FROM "ftVentas" v
        INNER JOIN "dtUbicacion" u ON u.codigo_tienda = v.codigo_tienda
    WHERE u.tipo_tienda = 'Tienda Regional'
        GROUP BY nombre_barrio
        ORDER BY unique_clients DESC
    LIMIT 5
    """

    # Ejecutar la consulta y obtener los resultados en un DataFrame
    df = pd.read_sql(query, engine)

    return df


# Configurar la conexión a la base de datos
engine = create_engine(
    'postgresql://postgres:toortoor@192.168.0.3:5433/gr5')

# Obtener las tiendas con al menos 100 clientes diferentes
top_5_barrios = get_top_5_barrios(engine)

# Imprimir el resultado
print(top_5_barrios)
