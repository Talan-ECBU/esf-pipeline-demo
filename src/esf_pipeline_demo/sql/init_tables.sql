-- 1. SELLERS table
IF OBJECT_ID('dbo.Sellers', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.Sellers (
    SellerID        INT             IDENTITY(1,1) PRIMARY KEY,
    Name            NVARCHAR(255)   NOT NULL,
    Marketplace     VARCHAR(32)     NOT NULL,
    URL             NVARCHAR(1000)  NULL
  );
END

IF OBJECT_ID('dbo.Products', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.Products (
    ProductID       VARCHAR(18)    PRIMARY KEY,
    Marketplace     VARCHAR(32)     NOT NULL,
    ProductGroup    VARCHAR(32)     NOT NULL,
    UploadDate      DATETIME2       NOT NULL,
    Title           NVARCHAR(MAX)   NOT NULL,
    Description     NVARCHAR(MAX)   NULL,
    Rating          DECIMAL(3,2)    NULL,
    Price           DECIMAL(18,2)   NULL,
    Currency        CHAR(3)         NULL,
    NumImages       INT             NULL,
    SellerID        INT             NULL,
    CONSTRAINT FK_Products_Sellers FOREIGN KEY (SellerID)
        REFERENCES dbo.Sellers(SellerID)
  );
END

-- 3. IMAGES table
IF OBJECT_ID('dbo.Images', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.Images (
    ImageGUID      UNIQUEIDENTIFIER  PRIMARY KEY,
    ProductID      VARCHAR(18)       NOT NULL
        FOREIGN KEY REFERENCES dbo.Products(ProductID),
    BlobPath       NVARCHAR(260)     NOT NULL,
    Width          INT               NOT NULL,
    Height         INT               NOT NULL,
    Format         VARCHAR(16)       NOT NULL,
    FileSizeBytes  BIGINT            NOT NULL,
    Checksum       VARCHAR(64)       NOT NULL,
    UploadTs       DATETIME2         DEFAULT SYSUTCDATETIME() NOT NULL
  );
END

-- 4. LABELS table
IF OBJECT_ID('dbo.Labels', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.Labels (
    LabelID      INT              IDENTITY(1,1) PRIMARY KEY,
    ImageGUID    UNIQUEIDENTIFIER NOT NULL
        FOREIGN KEY REFERENCES dbo.Images(ImageGUID),
    LabelType    NVARCHAR(100)    NOT NULL,
    LabelValue   NVARCHAR(100)    NOT NULL,
    LabeledBy    NVARCHAR(100)    NULL,
    LabeledTs    DATETIME2        DEFAULT SYSUTCDATETIME() NOT NULL
  );
END

-- 5. REVIEWS table
IF OBJECT_ID('dbo.Reviews', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.Reviews (
    ReviewID    INT            IDENTITY(1,1) PRIMARY KEY,
    ProductID   VARCHAR(18)
        FOREIGN KEY REFERENCES dbo.Products(ProductID),
    ReviewText  NVARCHAR(MAX)  NOT NULL,
    Rating      INT            NOT NULL,
    ReviewTs    DATETIME2      DEFAULT SYSUTCDATETIME() NOT NULL
  );
END
