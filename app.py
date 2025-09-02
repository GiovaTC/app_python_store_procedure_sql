import pyodbc

# ========= Configura tu conexión =========
CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=GTAPIERO-POLI;"
    "DATABASE=PruebaPython;"
    "UID=sa;"
    "PWD=tapiero;"
)

def upsert_usuario(nombre: str, edad: int, email: str, id_existente: int | None = None):
    """
    Ejecuta  dbo.usp_Usuarios_Upsert.
    - Si id_existente es None => INSERT
    - Si id_existente tiene valor => UPDATE
    Devuelve: (id_resultante, accion, fila_dict)
    """
    with pyodbc.connect(CONN_STR, autocommit=True) as conn:
        with conn.cursor()_as cur:
            tsql = """
                DECLARE @outId INT = ?; -- recibimos el posible Id inicial
                EXEC dbo.usp_Usuarios_Upsert
                    @Id = @outId OUTPUT,
                    @Nombre = ?,
                    @Edad = ?,
                    @Email = ?;
                -- La SP devuelve la fila afectada y luego el Id final
                SELECT FinalId = @outId;
            """
            cur.execute(tsql, (id_existente, nombre, edad, email))
            # Primer result set: detalles de la acción
            row = cur.fetchone()
            if row is None:
                conn.rollback()
                raise RuntimeError("La SP no devolvió resultados .")
            cols = [d[0] for d in cur.description]
            fila = dict(zip(cols, row))
            # Segundo result set: FinalId
            