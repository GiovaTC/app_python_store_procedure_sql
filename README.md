# app_python_store_procedure_sql
# 🐍 Python + SQL Server: Ejemplo completo de **Upsert** con Stored Procedure y `pyodbc`

Este ejemplo muestra cómo:

1. Crear una tabla `Usuarios` en SQL Server.  
2. Definir un **Stored Procedure** (`usp_Usuarios_Upsert`) que realiza **INSERT o UPDATE (Upsert)**.  
3. Ejecutar el procedimiento desde **Python** usando `pyodbc`.  

---

## 1️⃣ SQL Server: Esquema + Stored Procedure

Guarda el siguiente script como **`schema.sql`** y ejecútalo en tu instancia (SSMS / Azure Data Studio):

```sql
-- 1. (Opcional) Crear BD de ejemplo
IF DB_ID('PruebaPython') IS NULL
BEGIN
    CREATE DATABASE PruebaPython;
END
GO

USE PruebaPython;
GO

-- 2. Tabla de ejemplo
IF OBJECT_ID('dbo.Usuarios', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Usuarios (
        Id     INT IDENTITY(1,1) PRIMARY KEY,
        Nombre NVARCHAR(100) NOT NULL,
        Edad   INT           NOT NULL,
        Email  NVARCHAR(150) NOT NULL UNIQUE
    );
END
GO

-- 3. Stored Procedure de Upsert
IF OBJECT_ID('dbo.usp_Usuarios_Upsert', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_Usuarios_Upsert;
GO

CREATE PROCEDURE dbo.usp_Usuarios_Upsert
    @Id     INT = NULL OUTPUT,
    @Nombre NVARCHAR(100),
    @Edad   INT,
    @Email  NVARCHAR(150)
AS
BEGIN
    SET NOCOUNT ON;

    IF @Id IS NULL
    BEGIN
        INSERT INTO dbo.Usuarios (Nombre, Edad, Email)
        VALUES (@Nombre, @Edad, @Email);

        SET @Id = SCOPE_IDENTITY();

        SELECT
            Accion = 'INSERT',
            U.Id, U.Nombre, U.Edad, U.Email
        FROM dbo.Usuarios U
        WHERE U.Id = @Id;
    END
    ELSE
    BEGIN
        UPDATE U
        SET U.Nombre = @Nombre,
            U.Edad   = @Edad,
            U.Email  = @Email
        FROM dbo.Usuarios U
        WHERE U.Id = @Id;

        SELECT
            Accion = 'UPDATE',
            U.Id, U.Nombre, U.Edad, U.Email
        FROM dbo.Usuarios U
        WHERE U.Id = @Id;
    END
END
GO
🔹 Notas:

Email es UNIQUE para prevenir duplicados.

Si prefieres que el upsert sea por Email en lugar de Id, se puede ajustar con MERGE o lógica condicional.

2️⃣ Python: Ejecutar el Stored Procedure
Guarda el siguiente script como app.py.
Instala dependencias:

bash
pip install pyodbc
Asegúrate de tener instalado el ODBC Driver 17/18 para SQL Server.

python
import pyodbc

# ========= Configura tu conexión =========
CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"          # Cambia host/puerto si aplica
    "DATABASE=PruebaPython;"
    "Trusted_Connection=Yes;"         # O usa: UID=sa;PWD=TuPassword;
    "TrustServerCertificate=Yes;"     # Útil en entornos locales
)

def upsert_usuario(nombre: str, edad: int, email: str, id_existente: int | None = None):
    """
    Ejecuta dbo.usp_Usuarios_Upsert.
    - Si id_existente es None => INSERT
    - Si id_existente tiene valor => UPDATE
    Devuelve: (id_resultante, accion, fila_dict)
    """
    with pyodbc.connect(CONN_STR, autocommit=False) as conn:
        with conn.cursor() as cur:
            tsql = """
                DECLARE @outId INT = ?;  -- recibimos el posible Id inicial
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
                raise RuntimeError("La SP no devolvió resultados.")

            cols = [d[0] for d in cur.description]
            fila = dict(zip(cols, row))

            # Segundo result set: FinalId
            if cur.nextset():
                row2 = cur.fetchone()
                if row2 is None:
                    conn.rollback()
                    raise RuntimeError("No se pudo leer FinalId.")
                final_id = row2[0]
            else:
                conn.rollback()
                raise RuntimeError("No se encontró el segundo result set.")

            conn.commit()

            accion = fila.get("Accion", "DESCONOCIDA")
            fila["Id"] = final_id

            return final_id, accion, fila


if __name__ == "__main__":
    # ---- INSERT ----
    nuevo_id, accion_ins, fila_ins = upsert_usuario(
        nombre="Carlos Pérez",
        edad=28,
        email="carlos.perez@example.com"
    )
    print(f"[{accion_ins}] Nuevo Id: {nuevo_id} -> {fila_ins}")

    # ---- UPDATE ----
    id_actualizado, accion_upd, fila_upd = upsert_usuario(
        nombre="Carlos A. Pérez",
        edad=29,
        email="carlos.perez@example.com",
        id_existente=nuevo_id
    )
    print(f"[{accion_upd}] Id: {id_actualizado} -> {fila_upd}")

Puntos clave
Usamos DECLARE @outId ... EXEC ... SELECT para capturar el parámetro OUTPUT en pyodbc.

autocommit=False y conn.commit() aseguran control explícito de la transacción.

La SP devuelve dos result sets:

Datos de la acción (INSERT/UPDATE + fila).

FinalId para confirmar el ID final del registro.

🚀 Variantes útiles
Autenticación SQL:

UID=usuario;PWD=secreto;Trusted_Connection=no;

Upsert por Email: si deseas que Email sea la clave, se puede adaptar la SP.

MERGE: una alternativa compacta (aunque con advertencias en versiones nuevas de SQL Server).
