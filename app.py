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
        with conn.cursor() as cur:
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
            if cur.nextset():
                row2 = cur.fetchone()
                if row2 is None:
                    conn.rollback()
                    raise RuntimeError("No se pudo leer FinalId .")
                final_id = row2[0]
            else:
                conn.rollback()
                raise RuntimeError("No se encontró el segundo result set .")
            
            conn.commit()

            accion = fila.get("Accion", "DESCONOCIDA")
            fila["Id"] = final_id

            return final_id, accion, fila
if __name__ == "__main__":
    # ---- INSERT ----
    nuevo_id, accion_ins, fila_ins = upsert_usuario(
    nombre="Carlos Pérez",
    edad=28,
    email="carlos.perez@exam.com"
    )
    print(f"[{accion_ins}] Nuevo Id: {nuevo_id} -> {fila_ins}")
    # ---- UPDATE ----
    id_actualizado, accion_upd, fila_upd = update_usuario(
        nombre ="Carlos A. Pérez",
        edad = 29,
        email = "carlos.perez@exam.com",
        id_existente=nuevo_id
    )
    print(f"[{accion_upd}] Id: {id_actualizado} -> {fila_upd}")

