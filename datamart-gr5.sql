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
