MERGE dbo.Products AS Target
USING #ProductsInsert AS Source
ON Target.ProductID = Source.ProductID

WHEN MATCHED THEN
    UPDATE SET
        Target.Marketplace   = Source.Marketplace,
        Target.ProductGroup  = Source.ProductGroup,
        Target.UploadDate    = Source.UploadDate,
        Target.Title         = Source.Title,
        Target.Description   = Source.Description,
        Target.Rating        = Source.Rating,
        Target.Price         = Source.Price,
        Target.Currency      = Source.Currency,
        Target.NumImages     = Source.NumImages,
        Target.SellerID      = Source.SellerID

WHEN NOT MATCHED BY TARGET THEN
    INSERT (ProductID, Marketplace, ProductGroup, UploadDate, Title, Description, Rating, Price, Currency, NumImages, SellerID)
    VALUES (Source.ProductID, Source.Marketplace, Source.ProductGroup, Source.UploadDate, Source.Title, Source.Description, Source.Rating, Source.Price, Source.Currency, Source.NumImages, Source.SellerID);