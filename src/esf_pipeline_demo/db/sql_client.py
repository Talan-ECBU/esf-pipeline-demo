# src/esf_pipeline/db/sql_client.py
"""
SQL Client for Azure SQL Database interactions with robust error handling.
Includes functions for inserting and upserting data with detailed logging.
"""

import logging
import traceback

import pandas as pd
import pyodbc

from ..config.config import (
    AZURE_SQL_DATABASE,
    AZURE_SQL_DRIVER,
    AZURE_SQL_PASSWORD,
    AZURE_SQL_SERVER,
    AZURE_SQL_USERNAME,
    setup_logging,
)

setup_logging()
logger = logging.getLogger(__name__)


def get_sql_connection():
    """Establish and return SQL connection with error handling"""
    try:
        conn_str = (
            f"DRIVER={{{AZURE_SQL_DRIVER}}};"
            f"SERVER={AZURE_SQL_SERVER};DATABASE={AZURE_SQL_DATABASE};"
            f"UID={AZURE_SQL_USERNAME};PWD={AZURE_SQL_PASSWORD}"
        )
        conn = pyodbc.connect(conn_str)
        logger.info("Successfully connected to SQL database")
        return conn
    except pyodbc.Error as e:
        logger.error(f"Connection failed: {e}\n{traceback.format_exc()}")
        raise
    except Exception as e:
        logger.error(f"Unexpected connection error: {e}\n{traceback.format_exc()}")
        raise


def insert_dataframe(table_name: str, df: pd.DataFrame, debug: bool = False):
    """Insert DataFrame into SQL table with comprehensive error handling"""
    try:
        if df.empty:
            logger.info(f"Skipped: DataFrame is empty for table {table_name}")
            return

        conn = get_sql_connection()
        cursor = conn.cursor()

        columns = ",".join(df.columns)
        placeholders = ",".join(["?"] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        rows = [tuple(x) for x in df.to_numpy()]
        cursor.fast_executemany = True

        try:
            cursor.executemany(sql, rows)
            conn.commit()
            logger.info(f"Inserted {len(rows)} rows into {table_name}")
        except pyodbc.IntegrityError as e:
            logger.warning(f"Bulk insert failed due to integrity error: {e}")
            logger.info("Falling back to row-by-row insert (skipping invalid rows)...")

            success_count = 0
            for i, row in enumerate(rows):
                try:
                    cursor.execute(sql, row)
                    success_count += 1
                except pyodbc.IntegrityError as err:
                    if debug:
                        logger.info(f"Row {i} skipped due to: {err}")
                    continue
                except Exception as err:
                    logger.error(
                        f"Unexpected row error: {err}\n{traceback.format_exc()}"
                    )
                    continue

            conn.commit()
            logger.info(f"Inserted {success_count} of {len(rows)} rows after filtering")
        except pyodbc.Error as e:
            logger.error(f"SQL error during insert: {e}\n{traceback.format_exc()}")
            conn.rollback()
        except Exception as e:
            logger.error(
                f"Unexpected error during insert: {e}\n{traceback.format_exc()}"
            )
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(
            f"General failure in insert_dataframe: {e}\n{traceback.format_exc()}"
        )


def upsert_products(df: pd.DataFrame):
    """Upsert product data into dbo.Products with comprehensive logging"""
    try:
        df = df[
            df["ProductID"].notna() & (df["ProductID"].astype(str).str.strip() != "")
        ]
        if df.empty:
            logger.info("No valid products to insert (missing ProductID)")
            return

        conn = get_sql_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
            IF OBJECT_ID('tempdb..#ProductsInsert') IS NOT NULL
                DROP TABLE #ProductsInsert;
            """
            )
            cursor.execute(
                """
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
            """
            )
            conn.commit()
            logger.debug("Created temporary table #ProductsInsert")

            insert_sql = """
            INSERT INTO #ProductsInsert
            (ProductID, Marketplace, ProductGroup, UploadDate, Title, Description, Rating, Price, Currency, NumImages, SellerID)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            df = df.copy()
            df["Title"] = df["Title"].str.slice(0, 500)
            df["Description"] = df["Description"].str.slice(0, 4000)
            df["ProductID"] = df["ProductID"].str.slice(0, 18)
            df["Marketplace"] = df["Marketplace"].str.slice(0, 32)
            df["ProductGroup"] = df["ProductGroup"].str.slice(0, 32)
            df["Currency"] = df["Currency"].str.slice(0, 3)

            rows = []
            for row in df.itertuples(index=False):
                upload_date = row.UploadDate
                if isinstance(upload_date, pd.Timestamp):
                    upload_date = upload_date.to_pydatetime()

                rows.append(
                    (
                        str(row.ProductID).strip(),
                        str(row.Marketplace).strip(),
                        str(row.ProductGroup).strip(),
                        upload_date,
                        str(row.Title).strip(),
                        (
                            str(row.Description).strip()
                            if pd.notnull(row.Description)
                            else None
                        ),
                        float(row.Rating) if pd.notnull(row.Rating) else None,
                        float(row.Price) if pd.notnull(row.Price) else None,
                        (
                            str(row.Currency).strip().upper()
                            if pd.notnull(row.Currency)
                            else None
                        ),
                        int(row.NumImages) if pd.notnull(row.NumImages) else None,
                        int(row.SellerID) if pd.notnull(row.SellerID) else None,
                    )
                )

            param_types = [
                (pyodbc.SQL_VARCHAR, 18),
                (pyodbc.SQL_VARCHAR, 32),
                (pyodbc.SQL_VARCHAR, 32),
                pyodbc.SQL_TYPE_TIMESTAMP,
                (pyodbc.SQL_WVARCHAR, 1000),
                (pyodbc.SQL_WVARCHAR, 4000),
                (pyodbc.SQL_DECIMAL, 3, 2),
                (pyodbc.SQL_DECIMAL, 18, 2),
                (pyodbc.SQL_VARCHAR, 3),
                pyodbc.SQL_INTEGER,
                pyodbc.SQL_INTEGER,
            ]

            cursor.fast_executemany = True
            cursor.setinputsizes(param_types)

            try:
                cursor.executemany(insert_sql, rows)
                conn.commit()
                logger.info(f"Inserted {len(rows)} rows into temp table")
            except Exception as batch_err:
                logger.warning(
                    f"Batch insert failed: {batch_err}. Falling back to row-by-row"
                )
                success_count = 0
                for i, row_data in enumerate(rows):
                    try:
                        cursor.execute(insert_sql, row_data)
                        success_count += 1
                    except Exception as row_err:
                        logger.error(f"Row {i} failed: {row_err}")
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"Problematic row data: {row_data}")
                conn.commit()
                logger.info(f"Inserted {success_count}/{len(rows)} rows to temp table")

            cursor.execute(
                """
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
            """
            )
            conn.commit()
            logger.info(f"Upserted {len(rows)} products into Products table")

        except pyodbc.Error as e:
            logger.error(f"SQL error during upsert: {e}\n{traceback.format_exc()}")
            conn.rollback()
        except Exception as e:
            logger.error(
                f"Unexpected error during upsert: {e}\n{traceback.format_exc()}"
            )
            conn.rollback()
        finally:
            try:
                cursor.execute("DROP TABLE IF EXISTS #ProductsInsert;")
                conn.commit()
            except Exception as e:
                logger.error(f"Error dropping temp table: {e}")
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(
            f"General failure in upsert_products: {e}\n{traceback.format_exc()}"
        )


def upsert_sellers(df: pd.DataFrame) -> pd.DataFrame:
    """Upsert sellers with comprehensive error handling and logging"""
    try:
        if not all(col in df.columns for col in ["Name", "Marketplace", "URL"]):
            logger.error("Missing required columns in seller data")
            return pd.DataFrame(columns=["SellerID", "Name", "Marketplace"])

        initial_count = len(df)
        df = df.dropna(subset=["Name", "Marketplace"])
        df = df[df["Name"].str.strip() != ""]
        df = df[df["Marketplace"].str.strip() != ""]
        filtered_count = len(df)

        if filtered_count == 0:
            logger.warning(
                f"No valid sellers after filtering ({initial_count} input rows)"
            )
            return pd.DataFrame(columns=["SellerID", "Name", "Marketplace"])

        conn = get_sql_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
            IF OBJECT_ID('tempdb..#SellersUpsert') IS NOT NULL
                DROP TABLE #SellersUpsert;
            """
            )
            cursor.execute(
                """
            CREATE TABLE #SellersUpsert (
              Name         NVARCHAR(255) NOT NULL,
              Marketplace  VARCHAR(32)   NOT NULL,
              URL          NVARCHAR(1000) NULL
            );
            """
            )
            conn.commit()
            logger.debug("Created temporary table #SellersUpsert")

            unique_rows = df.sort_values("URL", na_position="last").drop_duplicates(
                subset=["Name", "Marketplace"]
            )
            rows = [
                (
                    str(row.Name).strip()[:255],
                    str(row.Marketplace).strip()[:32],
                    str(row.URL).strip()[:1000] if pd.notnull(row.URL) else None,
                )
                for row in unique_rows.itertuples(index=False)
            ]

            insert_sql = (
                "INSERT INTO #SellersUpsert (Name, Marketplace, URL) VALUES (?, ?, ?)"
            )
            cursor.fast_executemany = True

            try:
                cursor.executemany(insert_sql, rows)
                conn.commit()
                logger.info(f"Inserted {len(rows)} sellers into temp table")
            except Exception as e:
                logger.error(f"Temp insert failed: {e}. Falling back to row-by-row")
                success_count = 0
                for i, row in enumerate(rows):
                    try:
                        cursor.execute(insert_sql, row)
                        success_count += 1
                    except Exception as e:
                        logger.error(f"Row {i} failed: {e}")
                conn.commit()
                logger.info(f"Inserted {success_count}/{len(rows)} rows to temp table")

            cursor.execute(
                """
            MERGE INTO dbo.Sellers AS Target
            USING #SellersUpsert AS Source
              ON Target.Name = Source.Name AND Target.Marketplace = Source.Marketplace
            WHEN MATCHED THEN
              UPDATE SET Target.URL = Source.URL
            WHEN NOT MATCHED BY TARGET THEN
              INSERT (Name, Marketplace, URL)
              VALUES (Source.Name, Source.Marketplace, Source.URL)
            OUTPUT inserted.SellerID, Source.Name, Source.Marketplace;
            """
            )
            results = cursor.fetchall()
            conn.commit()
            logger.info(f"Upserted {len(results)} sellers")

            resolved_sellers = pd.DataFrame.from_records(
                results, columns=["SellerID", "Name", "Marketplace"]
            )
            resolved_sellers = resolved_sellers.dropna(
                subset=["SellerID", "Name", "Marketplace"]
            )
            resolved_sellers["SellerID"] = resolved_sellers["SellerID"].astype("Int64")

            return resolved_sellers

        except pyodbc.Error as e:
            logger.error(f"SQL error during upsert: {e}\n{traceback.format_exc()}")
            conn.rollback()
            return pd.DataFrame(columns=["SellerID", "Name", "Marketplace"])
        except Exception as e:
            logger.error(
                f"Unexpected error during upsert: {e}\n{traceback.format_exc()}"
            )
            conn.rollback()
            return pd.DataFrame(columns=["SellerID", "Name", "Marketplace"])
        finally:
            try:
                cursor.execute("DROP TABLE IF EXISTS #SellersUpsert;")
                conn.commit()
            except Exception as e:
                logger.error(f"Error dropping temp table: {e}")
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(
            f"General failure in upsert_sellers: {e}\n{traceback.format_exc()}"
        )
        return pd.DataFrame(columns=["SellerID", "Name", "Marketplace"])
