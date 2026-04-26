-- =============================================================================
-- Car Manufacturing Supply Control — Source Schema (OLTP)
-- Layer: Bronze / Source
-- Description: Original transactional tables as defined in the source system.
--              Do NOT modify these — they represent the raw operational model.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS CAR_MANUFACTURING_DB;
CREATE SCHEMA IF NOT EXISTS CAR_MANUFACTURING_DB.SOURCE;

USE SCHEMA CAR_MANUFACTURING_DB.SOURCE;

-- -----------------------------------------------------------------------------
-- SUPPLIER
-- Stores supplier company information.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SUPPLIER (
    Proveedor_ID    INTEGER         NOT NULL,
    Nombre          NVARCHAR(100)   NOT NULL,
    Direccion       NVARCHAR(200),
    Ciudad          NVARCHAR(100),
    Provincia       NVARCHAR(100),

    CONSTRAINT PK_SUPPLIER PRIMARY KEY (Proveedor_ID)
);

-- -----------------------------------------------------------------------------
-- FINAL_DELIVERY  (ENTREGA_FINAL)
-- One record per supplier delivery batch.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE FINAL_DELIVERY (
    Entrega_ID      INTEGER         NOT NULL,
    Proveedor_ID    INTEGER         NOT NULL,
    Fecha           DATE            NOT NULL,

    CONSTRAINT PK_FINAL_DELIVERY    PRIMARY KEY (Entrega_ID),
    CONSTRAINT FK_FD_SUPPLIER       FOREIGN KEY (Proveedor_ID) REFERENCES SUPPLIER(Proveedor_ID)
);

-- -----------------------------------------------------------------------------
-- CATEGORY
-- Part categories (e.g. Engine, Bodywork, Electronics).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE CATEGORY (
    Category_ID     INTEGER         NOT NULL,
    Nombre          TEXT            NOT NULL,

    CONSTRAINT PK_CATEGORY PRIMARY KEY (Category_ID)
);

-- -----------------------------------------------------------------------------
-- PARTS  (PIEZAS)
-- Individual part catalogue.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE PARTS (
    Pieza_ID        INTEGER         NOT NULL,
    Nombre          TEXT            NOT NULL,
    Color           TEXT,
    Precio          DECIMAL(10, 2)  NOT NULL,
    Category_ID     INTEGER         NOT NULL,

    CONSTRAINT PK_PARTS         PRIMARY KEY (Pieza_ID),
    CONSTRAINT FK_PARTS_CAT     FOREIGN KEY (Category_ID) REFERENCES CATEGORY(Category_ID)
);

-- -----------------------------------------------------------------------------
-- LOT_DELIVERY  (ENTREGA_LOTE_DE_PIEZAS)
-- Tracks which parts were included in each delivery batch and quantity.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE LOT_DELIVERY (
    Entrega_ID      INTEGER         NOT NULL,
    Pieza_ID        INTEGER         NOT NULL,
    Fecha           DATE            NOT NULL,
    Cantidad        INTEGER         NOT NULL,

    CONSTRAINT PK_LOT_DELIVERY  PRIMARY KEY (Entrega_ID, Pieza_ID),
    CONSTRAINT FK_LD_DELIVERY   FOREIGN KEY (Entrega_ID) REFERENCES FINAL_DELIVERY(Entrega_ID),
    CONSTRAINT FK_LD_PARTS      FOREIGN KEY (Pieza_ID)   REFERENCES PARTS(Pieza_ID)
);

-- -----------------------------------------------------------------------------
-- CARS  (COCHES)
-- Car model catalogue.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE CARS (
    Car_ID          INTEGER         NOT NULL,
    Marca           TEXT            NOT NULL,
    Modelo          TEXT            NOT NULL,
    Ano             INTEGER         NOT NULL,

    CONSTRAINT PK_CARS PRIMARY KEY (Car_ID)
);

-- -----------------------------------------------------------------------------
-- PARTS_CAR  (PIEZAS_COCHE)
-- Bill of materials: which parts (and how many) go into each car model.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE PARTS_CAR (
    Car_ID          INTEGER         NOT NULL,
    Pieza_ID        INTEGER         NOT NULL,
    Cantidad_pieza  INTEGER         NOT NULL,

    CONSTRAINT PK_PARTS_CAR     PRIMARY KEY (Car_ID, Pieza_ID),
    CONSTRAINT FK_PC_CAR        FOREIGN KEY (Car_ID)    REFERENCES CARS(Car_ID),
    CONSTRAINT FK_PC_PARTS      FOREIGN KEY (Pieza_ID)  REFERENCES PARTS(Pieza_ID)
);

-- -----------------------------------------------------------------------------
-- CUSTOMERS  (CLIENTES)
-- End customer information.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE CUSTOMERS (
    Cliente_ID      INTEGER         NOT NULL,
    Nombre          NVARCHAR(100)   NOT NULL,
    Direccion       NVARCHAR(200),
    Ciudad          NVARCHAR(100),
    Provincia       NVARCHAR(100),

    CONSTRAINT PK_CUSTOMERS PRIMARY KEY (Cliente_ID)
);

-- -----------------------------------------------------------------------------
-- SALES  (VENTAS)
-- Transactional sales records — one row per car sold to a customer.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SALES (
    Venta_ID        INTEGER         NOT NULL,
    Cliente_ID      INTEGER         NOT NULL,
    Car_ID          INTEGER         NOT NULL,
    Fecha           DATE            NOT NULL,
    Precio          NUMERIC(12, 2)  NOT NULL,

    CONSTRAINT PK_SALES         PRIMARY KEY (Venta_ID),
    CONSTRAINT FK_SALES_CUST    FOREIGN KEY (Cliente_ID) REFERENCES CUSTOMERS(Cliente_ID),
    CONSTRAINT FK_SALES_CAR     FOREIGN KEY (Car_ID)     REFERENCES CARS(Car_ID)
);
