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