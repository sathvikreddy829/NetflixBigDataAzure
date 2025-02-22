# NetflixBigDataAzure

In this project, I used Azure Data Factory (ADF) to automate the loading of data from a GitHub HTTP connection into the Bronze Layer of Azure Data Lake Gen2 for scalable data ingestion. Auto Loader in Databricks was then used to incrementally load master data into the Bronze layer, ensuring efficient processing by only handling new or changed data.

In the Silver Layer, I applied PySpark transformations to clean and correct data, removing invalid records and filling in missing values for downstream analytics. I wrote the transformed data to the Silver layer and automated the pipeline using notebook workflows for seamless data flow and monitoring.

Finally, I created Delta Live Tables in the Gold Layer for real-time data transformations, optimizing the dataset for high-performance querying and enabling automated data quality checks, making the data ready for analytics and business intelligence tools.
