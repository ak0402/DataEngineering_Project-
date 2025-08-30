CREATE TABLE water_tabel 
(
	last_load Varchar(2000)
)

SELECT min(Date_ID) FROM [dbo].[source_cars_data]

INSERT INTO [dbo].[water_tabel]
VALUES('DT00000') 
