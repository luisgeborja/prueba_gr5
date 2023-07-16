import pandas as pd
from sqlalchemy import create_engine


def get_stores_with_100_customers(engine):
    """
    Obtiene las tiendas con al menos 100 clientes diferentes.

    Args:
        engine (sqlalchemy.engine.base.Engine): Objeto de conexión a la base de datos.

    Returns:
        pandas.DataFrame: DataFrame que contiene las tiendas con al menos 100 clientes diferentes.
    """

    # Consulta SQL para obtener las tiendas con al menos 100 clientes diferentes
    query = """
    SELECT
        codigo_tienda
        ,COUNT(DISTINCT num_documento_cliente) cantidad_uniq_cliente
    FROM "ftVentas"
    GROUP BY codigo_tienda
    HAVING COUNT(DISTINCT num_documento_cliente) >= 100
    """

    # Ejecutar la consulta y obtener el resultado como un DataFrame
    df = pd.read_sql(query, engine)
    return df


# Configurar la conexión a la base de datos
engine = create_engine(
    'postgresql://postgres:toortoor@192.168.0.3:5433/gr5')

# Obtener las tiendas con al menos 100 clientes diferentes
result = get_stores_with_100_customers(engine)

# Imprimir el resultado
print(result)
