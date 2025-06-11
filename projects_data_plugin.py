import json
import logging
import os
from typing import Optional

import asyncpg
import pandas as pd
from semantic_kernel.functions import kernel_function
from terminal_colors import TerminalColors as tc

# Configuración de logs
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# Validación de variables de entorno
required_env_vars = [
    "DB_HOST",
    "DB_PORT",
    "DB_NAME",
    "DB_USER",
    "DB_PASSWORD",
    "DB_TABLE_NAME",
]
missing_vars = [var for var in required_env_vars if var not in os.environ]

if missing_vars:
    raise EnvironmentError(
        f"Las siguientes variables de entorno son requeridas pero no están configuradas: {', '.join(missing_vars)}"
    )

# Configuración de DB
DATABASE_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}
# print(f"{DATABASE_CONFIG=}")

DB_TABLE_NAME = os.getenv("DB_TABLE_NAME")
# print(f"{DB_TABLE_NAME=}")


class ProjectsDataPlugin:
    conn: Optional[asyncpg.Connection]

    def __init__(self) -> None:
        self.conn = None

    async def connect(self) -> None:
        try:
            self.conn = await asyncpg.connect(**DATABASE_CONFIG)
            logger.info("Database connection opened.")
        except asyncpg.PostgresError as e:
            logger.exception("An error occurred", exc_info=e)
            self.conn = None

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()
            logger.debug("Database connection closed.")

    async def _get_table_names(self: "ProjectsDataPlugin") -> list:
        """Return a list of table names."""
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
        """
        rows = await self.conn.fetch(query)
        return [row["table_name"] for row in rows]

    async def _get_column_info(self: "ProjectsDataPlugin", table_name: str) -> list:
        """Return a list of tuples containing column names and their types."""
        query = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = $1 AND table_schema = 'public'
        ORDER BY ordinal_position;
        """
        rows = await self.conn.fetch(query, table_name)
        return [f"{row['column_name']}: {row['data_type']}" for row in rows]

    async def _get_regions(self: "ProjectsDataPlugin") -> list:
        """Return a list of unique regions in the database."""
        query = "SELECT DISTINCT region FROM sales_data;"
        rows = await self.conn.fetch(query)
        return [row["region"] for row in rows]

    async def _get_product_types(self: "ProjectsDataPlugin") -> list:
        """Return a list of unique product types in the database."""
        query = "SELECT DISTINCT product_type FROM sales_data;"
        rows = await self.conn.fetch(query)
        return [row["product_type"] for row in rows]

    async def _get_product_categories(self: "ProjectsDataPlugin") -> list:
        """Return a list of unique product categories in the database."""
        query = "SELECT DISTINCT main_category FROM sales_data;"
        rows = await self.conn.fetch(query)
        return [row["main_category"] for row in rows]

    async def _get_reporting_years(self: "ProjectsDataPlugin") -> list:
        """Return a list of unique reporting years in the database."""
        query = "SELECT DISTINCT year FROM sales_data ORDER BY year;"
        rows = await self.conn.fetch(query)
        return [str(row["year"]) for row in rows]

    def _get_column_descriptions(self) -> dict:
        """Retorna un diccionario con las descripciones de las columnas de la base de datos de inversiones."""
        return {
            "CODIGO_UNICO": "Código Único de la Invesión",
            "CODIGO_SNIP": "Código SNIP de la Inversión",
            "NOMBRE_INVERSION": "Nombre de la Inversión",
            "ESTADO": "Estado de la Inversión: ACTIVO / CERRADO / DESACTIVADO",
            "SITUACION": "Situación de la Inversión:  VIABLE /APROBADO / NO VIABLE / NO APROBADO / EN FORMULACION / EN REGISTRO",
            "MARCO": "Sistema de Inversión Pública: SNIP / INVIERTE",
            "TIPO_FORMATO": "Descripción del formato de inversión: PROYECTO DE INVERSIÓN / IOARR / PROGRAMA DE INVERSIÓN / PROYECTOS ESPECIALES",
            "UNIDAD_FORMULADORA_UF": "UF: Unidad Formuladora de la Inversión",
            "UNIDAD_EJECUTORA_INVERSIONES": "Nombre de  la Unidad Ejecutora de Inversiones en la Fase de Ejecución",
            "OPMI": "Nombre de la Oficina de Programación Multianual de Inversiones (OPMI) ",
            "DEPARTAMENTO_OPMI": "Departamento de la OPMI (Oficina de Programación Multianual de Inversiones)",
            "CODIGO_EJECUTORA_PRESUPUESTAL": "Código de la Unidad Ejecutora Presupuestal (UEP) asignada a la Inversión",
            "NOMBRE_EJECUTORA_PRESUPUESTAL": "Nombre de la Unidad Ejecutora Presupuestal (UEP) asignada a la Inversión",
            "FUNCION": "Nombre de la Función",
            "DIVISION_FUNCIONAL": "Nombre de la División Funcional",
            "GRUPO_FUNCIONAL": "Nombre del Grupo Funcional",
            "BENEFICIARIOS": "Número de beneficiarios de la Inversión",
            "FECHA_REGISTRO": "Fecha de registro de la inversión",
            "FECHA_VIABILIDAD": "Fecha de la VIABILIDAD / APROBACIÓN de la Inversión",
            "COSTO_INVERSION_VIABLE": "Costo de la Inversión con la que fue declarado viable",
            "DEPARTAMENTO_INVERSION": "Departamento donde se ubica la Inversión",
            "TIPOLOGIA_DE_INVERSION": "Tipología registrada  de la Inversión",
            "EXPEDIENTE_TECNICO": "Indicador de registro de Expediente Técnico: SI / NO",
            "TIENE_FORMATO_08": "Indicador de si la Inversión tiene registro en el Formato 08 (Ejecución): SI / NO",
            "ETAPA_FORMATO_08_ACTUAL": "Etapa actual en la cual se encuentra la Inversión: 'APROBACIÓN DE CONSISTENCIA (A)', 'EXPEDIENTE TÉCNICO (B)', 'EJECUCIÓN FÍSICA (C)'",
            "FECHA_INICIO_INVERSION": "Fecha que, según lo registrado en el Formato 08  correspondería al inicio programado de la inversión.",
            "FECHA_FIN_INVERSION":"Fecha que, de acuerdo con lo consignado en el Formato 08 se consideraría como la finalización programada de la inversión.",
            "COSTO_INVERSION_ACTUALIZADO": "Costo Total de la Inversión Actualizado y podría incluir Costo de Control Concurrente",
            "COSTO_CONTROVERSIAS": "Costo de controversias asociadas a la Inversión",
            "MONTO_CARTA_FIANZA": "Monto de la Carta Fianza asociada a la Inversión",
            "COSTO_TOTAL_INV_ACTUALIZADO": "Costo Total de la Inversión Actualizado",
            "DEVENGADO_ACUMULADO": "Monto devengado acumulado de la inversión",
            "PIM_AÑO_ACTUAL": "Monto PIM del año actual y también puede ser Presupuesto asignado",
            "DEVENGADO_AÑO_ATUAL": "Monto DEVENGADO del año actual",
            "AVANCE_FINANCIERO": "Avance financiero de la inversión: Devengado_año_actual/Monto_PIM",
            "REGISTRA_FORMATO_12B": "Indicador de registro del Formato F12B: SI / NO",
            "FECHA_ACTUALIZACION_F12B": "Fecha de actualziación del Formato F12B",
            "AVANCE_FISICO_INVERSION": "Avance físico de la inversión registrado en el Formato F12B",
            "AVANCE_EJECUCION_INVERSION": "Avance de ejecución de la inversión registrado en el Formato F12B",
            "FECHA_REGISTRO_SITUACION": "Fecha actualizada del registro de la situación de la inversión",
            "DESCRIPCION_ULTIMA_SITUACION": "Descripción correspondiente al último estado situacional de la inversión, según el reporte oficial emitido por la Unidad Ejecutora de Inversiones (UEI), el cual se encuentra vinculado a la fecha registrada en el campo FECHA_REGISTRO_SITUACION.",  
            "TIENE_FORMATO_09": "Indicador de si la Inversión tiene registro en el Formato 08 (Cierre): SI / NO",
            "ESTADO_REGISTRO_CIERRE": "Estado registrado en el fase del cierre de la Inversión",
            "FECHA_REGISTRO_CIERRE": "Fecha del registro de cierre de la Inversión",
            "NIVEL": "Nivel de gobierno de la Unidad Formuladora: GN (Gobiern nacional), GR (Gobiernos Regionales), GL (Gobiernos Locales)",
            "SECTOR": "Sector de gobierno de la Unidad Formuladora",
            "PLIEGO": "Pliego Entidad de ula Unidad Formuladora"
        }

    def _get_detailed_fields_query_info(self) -> str:
        """Retorna información sobre los campos detallados para consultas específicas."""
        filter_info = """
CAMPOS DETALLADOS PARA CONSULTAS ESPECÍFICAS DE INVERSIÓN:
Cuando el usuario solicite información detallada sobre una inversión específica usando su código único, mostrar en una tabla:
- CODIGO_UNICO
- NOMBRE_INVERSION
- ESTADO
- SITUACION
- TIPO_FORMATO
- SECTOR
- PLIEGO
- NOMBRE_EJECUTORA_PRESUPUESTAL
- FUNCION
- FECHA_VIABILIDAD
- COSTO_INVERSION_VIABLE
- EXPEDIENTE TECNICO
- COSTO_TOTAL_INV_ACTUALIZADO
- DEVENGADO_ACUMULADO
- AVANCE_FINANCIERO
- PIM_AÑO_ACTUAL
- DEVENGADO_AÑO_ACTUAL
- AVANCE_FISICO_INVERSION
- FECHA_REGISTRO_SITUACION
- DESCRIPCION_ULTIMA_SITUACION
- ENLACE_SSI

No es necesario motrar CODIGO_SNIP si no se solicitan.
        """
        return filter_info.strip()

    def _get_filter_fields_info(self) -> str:
        """Retorna información sobre los campos disponibles para filtrar."""
        filter_info = """
CAMPOS PARA FILTRAR INFORMACIÓN:
- NIVEL: Nivel de gobierno de la Unidad Formuladora: GN (Gobiern nacional), GR (Gobiernos Regionales), GL (Gobiernos Locales)
- SECTOR: Sector de gobierno de la Unidad Formuladora
- PLIEGO: Pliego Entidad de ula Unidad Formuladora
- TIPO_FORMATO: Descripción del formato de inversión: PROYECTO DE INVERSIÓN / IOARR / PROGRAMA DE INVERSIÓN / PROYECTOS ESPECIALES
- UNIDAD_FORMULADORA_UF: UF: Unidad Formuladora de la Inversión
- UNIDAD_EJECUTORA_INVERSIONES: Nombre de  la Unidad Ejecutora de Inversiones en la Fase de Ejecución
- OPMI: Nombre de la Oficina de Programación Multianual de Inversiones (OPMI) 
- DEPARTAMENTO_OPMI: Departamento de la OPMI (Oficina de Programación Multianual de Inversiones)
- NOMBRE_EJECUTORA_PRESUPUESTAL: Nombre de la Unidad Ejecutora Presupuestal (UEP) asignada a la Inversión
- FUNCION: Nombre de la Función
- DEPARTAMENTO_INVERSION: Departamento donde se ubica la Inversión
- TIPOLOGIA_DE_INVERSION: Tipología registrada  de la Inversión
Las regiones se deben convertir a mayúsculas sin tildes, por ejemplo convertir Huánuco a HUANUCO. Además 'Lima Metropolitana' se debe convertir a 'LIMA'.
        """
        return filter_info.strip()

    def _get_mandatory_response_fields(self) -> str:
        """Retorna información sobre los campos que deben incluirse en las respuestas cuando estén disponibles."""
        return """
CAMPOS OBLIGATORIOS A MOSTRAR EN RESPUESTAS (cuando estén disponibles):
- CODIGO_UNICO: Código Único de la Invesión
- CODIGO_SNIP: Código SNIP de la Inversión
- NOMBRE_INVERSION: Nombre de la Inversión
- ESTADO: Estado de la Inversión: ACTIVO / CERRADO / DESACTIVADO
- SITUACION: Situación de la Inversión:  VIABLE /APROBADO / NO VIABLE / NO APROBADO / EN FORMULACION / EN REGISTRO
        """

    async def get_database_info(self: "ProjectsDataPlugin") -> str:
        """Return a string containing the database schema information and common query fields."""
        # Agregar contexto sobre las columnas y campos para filtrar
        database_info = "\n\n" + self._get_mandatory_response_fields()
        database_info += "\n\n" + self._get_filter_fields_info()
        database_info += "\n\n" + self._get_detailed_fields_query_info()
        database_info += f"\n\nDESCRIPCIÓN DE COLUMNAS de la tabla {DB_TABLE_NAME}:\n"

        column_descriptions = self._get_column_descriptions()
        for column, description in column_descriptions.items():
            database_info += f"- {column}: {description}\n"

        return database_info

    @kernel_function(
        name="fetch_sales_data",
        description="Execute a PostgreSQL query and return results as JSON",
    )
    async def async_fetch_sql_data_using_postgres_query(self, sqlite_query: str) -> str:
        """
        This function is used to answer user questions about investment data by executing PostgreSQL queries against the database.

        :param sqlite_query: The input should be a well-formed PostgreSQL query to extract information based on the user's question. The query result will be returned as a JSON object.
        :return: Return data in JSON serializable format.
        :rtype: str
        """
        # Convertir query a PostgreSQL si es necesario
        postgres_query = self._convert_sqlite_to_postgres(sqlite_query)

        # Convertir a mayúsculas pero preservar columnas con ñ
        postgres_query = self._selective_uppercase(postgres_query)

        print(
            f"\n{tc.BLUE}Function Call Tools: async_fetch_sql_data_using_postgres_query{tc.RESET}\n"
        )
        print(f"{tc.BLUE}Executing query: {postgres_query}{tc.RESET}\n")

        try:
            rows = await self.conn.fetch(postgres_query)

            if not rows:
                return json.dumps(
                    "The query returned no results. Try a different question."
                )

            # Convertir asyncpg.Record a diccionarios para pandas
            data_dicts = [dict(row) for row in rows]
            data = pd.DataFrame(data_dicts)
            return data.to_json(index=False, orient="split")

        except Exception as e:
            return json.dumps(
                {"PostgreSQL query failed with error": str(e), "query": postgres_query}
            )

    def _selective_uppercase(self, query: str) -> str:
        """
        Convierte la query a mayúsculas pero preserva los nombres de columnas con ñ
        que deben mantenerse en minúsculas.
        """
        # Columnas que deben mantenerse en minúsculas (contienen ñ)
        preserve_columns = ["pim_año_actual", "devengado_año_actual"]

        # Crear marcadores temporales para preservar estas columnas
        temp_markers = {}
        temp_query = query

        for i, column in enumerate(preserve_columns):
            marker = f"__PRESERVE_COLUMN_{i}__"
            temp_markers[marker] = column
            temp_query = temp_query.replace(column, marker)

        # Convertir a mayúsculas
        temp_query = temp_query.upper()

        # Restaurar las columnas preservadas
        for marker, original_column in temp_markers.items():
            temp_query = temp_query.replace(marker, original_column)

        return temp_query

    def _convert_sqlite_to_postgres(self, sqlite_query: str) -> str:
        """
        Convierte queries de SQLite a PostgreSQL cuando sea necesario.
        Esta función puede expandirse para manejar más conversiones específicas.
        """
        postgres_query = sqlite_query

        # Conversiones básicas de SQLite a PostgreSQL
        conversions = {
            # SQLite usa || para concatenación, PostgreSQL también lo soporta
            # SQLite usa LIMIT, PostgreSQL también lo soporta
            # Conversiones de tipos de datos si es necesario
            "INTEGER": "INTEGER",
            "TEXT": "TEXT",
            "REAL": "REAL",
        }

        # Normalizar nombres de columnas comunes que pueden venir en diferentes formatos
        column_normalizations = {
            "PIM_AÑO_ACTUAL": "pim_año_actual",
            "DEVENGADO_AÑO_ACTUAL": "devengado_año_actual",
        }

        # Aplicar normalizaciones de columnas
        for variant, normalized in column_normalizations.items():
            postgres_query = postgres_query.replace(variant, normalized)

        # Aplicar conversiones básicas
        for sqlite_syntax, postgres_syntax in conversions.items():
            postgres_query = postgres_query.replace(sqlite_syntax, postgres_syntax)

        return postgres_query
