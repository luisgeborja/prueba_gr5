<h1 align="center"> Prueba Ingeniero de Datos para GR5 </h1>

En los archivos cargados en este reposoitorio podrás detallar el desarrollo de los elementos solicitados para el desarrollo de la prueba de Ingeniero de Datos.

Se desarrollo un modelo estrella pensando más en el diseño de un datamart, depurando la información a través del pipeline creado en Python.

1. Se adjunta el dataset descargado para la ejecución de la prueba.
2. En la primera parte del ejercicio se aprecía el diseño inicial de la base de datos (diseno_inicial_gr5.svg).
   
    ![alt text](Imagenes/diseno_inicial_gr5.svg)
   
   Se aprecia su evolución durante el desarrollo del ejercicio, cuyo resultado es diseno_final_gr5.png
    ![alt text](Imagenes/diseno_final_gr5.png)
   
3. Se envía el código SQL para la creación de las tablas, la base de datos elegida fue PostgreSQL.
   ```sql
   CREATE TABLE "dtCliente" (
     "num_documento_cliente" int8 not null constraint "pkdtCliente" PRIMARY KEY,
     "tipo_documento_cliente" integer
   );
   
   CREATE TABLE "dtUbicacion" (
     "codigo_tienda" int8  not null constraint "pkdtUbicacion" PRIMARY KEY,
     "tipo_tienda" varchar,
     "id_barrio" integer,
     "nombre_barrio" varchar,
     "latitud_tienda" float,
     "longitud_tienda" float
   );
   
   CREATE TABLE "dtTiempo" (
     "fecha_id" integer not null constraint "pkdtTiempo" PRIMARY KEY,
     "fecha_compra" timestamp,
     "anio" integer,
     "mes" integer,
     "dia" integer
   );
   
   CREATE TABLE "ftVentas" (
     "num_documento_cliente" int8,
     "codigo_tienda" int8,
     "fecha_id" integer,
     "total_compra" float
   );
   
   ALTER TABLE "ftVentas" ADD FOREIGN KEY ("num_documento_cliente") REFERENCES "dtCliente" ("num_documento_cliente");
   ALTER TABLE "ftVentas" ADD FOREIGN KEY ("codigo_tienda") REFERENCES "dtUbicacion" ("codigo_tienda");
   ALTER TABLE "ftVentas" ADD FOREIGN KEY ("fecha_id") REFERENCES "dtTiempo" ("fecha_id");
   ```
4. Se adjunta el pipeline o ETL construido para el desarrollo de la prueba, con las siguientes particularidades:
  > - Algunos números de identificación de cliente venian con diferentes tipos de identificación, se opto por contar la mayor apariación del número de identificación según su tipo, tomando el dato con mayores aparaciones y almacenando en la base de datos, lo anterior ya que considere un hipotetico caso de error en digitación del tipo y aterrizando un poco el ejercicio a la practica, un número de identificación es único y no podría tener difentes tipos de identificación (CC, TI, CE, etc).

  > - Existen 2 códigos de tienda (745, 747) que están repetidos con diferentes localizaciones y tipo de tienda, al considerar la cantidad de datos afectados y el total de los datos, se aplica la misma lógica explicada en el punto anterior y se deja un solo registro.

5. Ejecutar scripts (.py) en este orden para obtener el resultado esperado:
   ```python
   # Ejecutar pipeline para obtener información y cargarla en la base de datos
   python3 etl_gr5.py

   # Una vez se cargan los datos se puede ejecutar cada una de las consultas para obtener la información esperada
   # Consulta para obtener las tiendas con al menos 100 clientes
   python3 qry_1_gr5.py

   # Consulta para obtener los 5 barrios con la mayor cantidad de clientes únicos
   python3 qry_2_gr5.py
   ```
   El resultado de la ejecución de los scripts a continuación:

   ![alt text](Imagenes/exec_qry1.png)

   ![alt text](Imagenes/exec_qry2.png)
   
6. Se crean unas gráficas en Power Bi conectandose a la base de datos en la que se almaceno la información. Una aclaración, posiblemente al abrir el archivo se muestra un mensaje, por favor seleccionar la opción "No Gracias" para no afectar la visual correspondiente al mapa.
    
    ![alt text](Imagenes/error-pbi.png)

   El archivo .pbix mostraría así unas visuales como los siguientes:
   
    ![alt text](Imagenes/visual_pbi.png)

