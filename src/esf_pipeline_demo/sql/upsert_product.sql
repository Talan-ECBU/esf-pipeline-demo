IF OBJECT_ID('tempdb..#ProductsInsert') IS NOT NULL
DROP TABLE #ProductsInsert;

CREATE TABLE #ProductsInsert (
    ProductID       VARCHAR(18)     NOT NULL,
    Marketplace     VARCHAR(32)     NOT NULL,
    ProductGroup    VARCHAR(32)     NOT NULL,
    UploadDate      DATETIME2       NOT NULL,
    Title           NVARCHAR(500)   NOT NULL,
    Description     NVARCHAR(MAX)   NULL,
    Rating          DECIMAL(3,2)    NULL,
    Price           DECIMAL(18,2)   NULL,
    Currency        CHAR(3)         NULL,
    NumImages       INT             NULL,
    SellerID        INT             NULL
);