import json
import logging
from typing import Optional

import aiosqlite
import pandas as pd
from semantic_kernel.functions import kernel_function
from terminal_colors import TerminalColors as tc

# DATA_BASE = "database/contoso-sales.db"
DATA_BASE = "database/total_inversiones.db"

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


class ProjectsDataPlugin:
    conn: Optional[aiosqlite.Connection]

    def __init__(self) -> None:
        self.conn = None

    async def connect(self) -> None:
        db_uri = f"file:{DATA_BASE}?mode=ro"
        try:
            self.conn = await aiosqlite.connect(db_uri, uri=True)
            logger.info("Database connection opened.")
        except aiosqlite.Error as e:
            logger.exception("An error occurred", exc_info=e)
            self.conn = None

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()
            logger.debug("Database connection closed.")

    async def _get_table_names(self: "ProjectsDataPlugin") -> list:
        """Return a list of table names."""
        table_names = []
        async with self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ) as tables:
            return [table[0] async for table in tables if table[0] != "sqlite_sequence"]

    async def _get_column_info(self: "ProjectsDataPlugin", table_name: str) -> list:
        """Return a list of tuples containing column names and their types."""
        column_info = []
        async with self.conn.execute(f"PRAGMA table_info('{table_name}');") as columns:
            # col[1] is the column name, col[2] is the column type
            return [f"{col[1]}: {col[2]}" async for col in columns]

    async def _get_regions(self: "ProjectsDataPlugin") -> list:
        """Return a list of unique regions in the database."""
        async with self.conn.execute(
            "SELECT DISTINCT region FROM sales_data;"
        ) as regions:
            result = await regions.fetchall()
        return [region[0] for region in result]

    async def _get_product_types(self: "ProjectsDataPlugin") -> list:
        """Return a list of unique product types in the database."""
        async with self.conn.execute(
            "SELECT DISTINCT product_type FROM sales_data;"
        ) as product_types:
            result = await product_types.fetchall()
        return [product_type[0] for product_type in result]

    async def _get_product_categories(self: "ProjectsDataPlugin") -> list:
        """Return a list of unique product categories in the database."""
        async with self.conn.execute(
            "SELECT DISTINCT main_category FROM sales_data;"
        ) as product_categories:
            result = await product_categories.fetchall()
        return [product_category[0] for product_category in result]

    async def _get_reporting_years(self: "ProjectsDataPlugin") -> list:
        """Return a list of unique reporting years in the database."""
        async with self.conn.execute(
            "SELECT DISTINCT year FROM sales_data ORDER BY year;"
        ) as reporting_years:
            result = await reporting_years.fetchall()
        return [str(reporting_year[0]) for reporting_year in result]

    def _get_column_descriptions(self) -> dict:
        """Retorna un diccionario con las descripciones de las columnas de la base de datos de inversiones."""
        return {
            "CODIGO_UNICO": "Código Único de la Invesión",
            "CODIGO_SNIP": "Código SNIP de la Inversión",
            "NOMBRE_INVERSION": "Nombre de la Inversión",
            "ESTADO": "Estado de la Inversión: ACTIVO / CERRADO / DESACTIVADO",
            "SITUACION": "Situación de la Inversión:  VIABLE /APROBADO / NO VIABLE / NO APROBADO / EN FORMULACIÓN / EN REGISTRO",
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
            "EXPEDIENTE TECNICO": "Indicador de registro de Expediente Técnico: SI / NO",
            "TIENE_FORMATO_08": "Indicador de si la Inversión tiene registro en el Formato 08 (Ejecución): SI / NO",
            "ETAPA_FORMATO_08_ACTUAL": "Etapa actual en la cual se encuentra la Inversión: CONSISTENCIA / EXPEDIENTE TÉCNICO / EJECUCIÓN FÍSICA",
            "FECHA_INICIO_INVERSION": "Fecha de Inicio de la ejecución de la inversión registrado en el Formato 08",
            "FECHA_FIN_INVERSION": "Fecha de Fin de la ejecución de la Inversión registrado en el Format 08",
            "COSTO_INVERSION_ACTUALIZADO": "Costo de la Inversión actualizado",
            "COSTO_CONTROVERSIAS": "Costo de controversias asociadas a la Inversión",
            "MONTO_CARTA_FIANZA": "Monto de la Carta Fianza asociada a la Inversión",
            "COSTO_TOTAL_INV_ACTUALIZADO": "Costo Total de la Inversión Actualizado",
            "DEVENGADO_ACUMULADO": "Monto devengado acumulado de la inversión",
            "PIM_AÑO_ACTUAL": "Monto PIM del año actual",
            "DEVENGADO_AÑO_ATUAL": "Monto DEVENGADO del año actual",
            "AVANCE_FINANCIERO": "Avance financiero de la inversión: Devengado_año_actual/Monto_PIM",
            "REGISTRA_FORMATO_12B": "Indicador de registro del Formato F12B: SI / NO",
            "FECHA_ACTUALIZACION_F12B": "Fecha de actualziación del Formato F12B",
            "AVANCE_FISICO_INVERSION": "Avance físico de la inversión registrado en el Formato F12B",
            "AVANCE_EJECUCION_INVERSION": "Avance de ejecución de la inversión registrado en el Formato F12B",
            "FECHA_REGISTRO_SITUACION": "Fecha actualizada del registro de la situación de la inversión",
            "DESCRIPCION_ULTIMA_SITUACION": "Descripción de la última situación de la Inversión",
            "TIENE_FORMATO_09": "Indicador de si la Inversión tiene registro en el Formato 08 (Cierre): SI / NO",
            "ESTADO_REGISTRO_CIERRE": "Estado registrado en el fase del cierre de la Inversión",
            "FECHA_REGISTRO_CIERRE": "Fecha del registro de cierre de la Inversión",
            "NIVEL": "Nivel de gobierno de la Unidad Formuladora: GN (Gobiern nacional), GR (Gobiernos Regionales), GL (Gobiernos Locales)",
            "SECTOR": "Sector de gobierno de la Unidad Formuladora",
            "PLIEGO": "Pliego 7Entidad de ula Unidad Formuladora",
        }

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
- SITUACION: Situación de la Inversión:  VIABLE /APROBADO / NO VIABLE / NO APROBADO / EN FORMULACIÓN / EN REGISTRO
        """

    async def get_database_info(self: "ProjectsDataPlugin") -> str:
        """Return a string containing the database schema information and common query fields."""
        # table_dicts = []
        # for table_name in await self._get_table_names():
        #     columns_names = await self._get_column_info(table_name)
        #     table_dicts.append(
        #         {"table_name": table_name, "column_names": columns_names}
        #     )

        # database_info = "\n".join(
        #     [
        #         f"Table {table['table_name']} Schema: Columns: {', '.join(table['column_names'])}"
        #         for table in table_dicts
        #     ]
        # )

        # Agregar contexto sobre las columnas y campos para filtrar
        database_info = "\n\n" + self._get_mandatory_response_fields()
        database_info += "\n\n" + self._get_filter_fields_info()
        database_info += "\n\nDESCRIPCIÓN DE COLUMNAS de la tabla total_inversiones:\n"

        column_descriptions = self._get_column_descriptions()
        for column, description in column_descriptions.items():
            database_info += f"- {column}: {description}\n"

        return database_info

    @kernel_function(
        name="fetch_sales_data",
        description="Execute an SQLite query and return results as JSON",
    )
    async def async_fetch_sales_data_using_sqlite_query(self, sqlite_query: str) -> str:
        """
        This function is used to answer user questions about Contoso sales data by executing SQLite queries against the database.

        :param sqlite_query: The input should be a well-formed SQLite query to extract information based on the user's question. The query result will be returned as a JSON object.
        :return: Return data in JSON serializable format.
        :rtype: str
        """
        sqlite_query_upper = sqlite_query.upper()

        print(
            f"\n{tc.BLUE}Function Call Tools: async_fetch_sales_data_using_sqlite_query{tc.RESET}\n"
        )
        print(f"{tc.BLUE}Executing query: {sqlite_query_upper}{tc.RESET}\n")

        try:
            async with self.conn.execute(sqlite_query_upper) as cursor:
                rows = await cursor.fetchall()
                columns = [description[0] for description in cursor.description]

            if not rows:
                return json.dumps(
                    "The query returned no results. Try a different question."
                )
            data = pd.DataFrame(rows, columns=columns)
            return data.to_json(index=False, orient="split")

        except Exception as e:
            return json.dumps(
                {"SQLite query failed with error": str(e), "query": sqlite_query_upper}
            )
