-- Create Athena Table from S3 Parquet file
CREATE EXTERNAL TABLE IF NOT EXISTS `1sti`.`data_medalhas` (
  `nome` string,
  `escola` string,
  `tipo` string,
  `municipio` string,
  `uf` string,
  `medalha` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://1sti-data-engineer-challenge/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- Select all from table
SELECT * FROM data_medalhas;

-- Lista das escolas com maior número de medalhas
SELECT escola, tipo, uf, COUNT(medalha) medalha_count
FROM data_medalhas 
GROUP BY escola, tipo, uf
ORDER BY medalha_count DESC;

-- Lista dos estados com maior número de medalhas
SELECT uf, COUNT(medalha) medalha_count
FROM data_medalhas 
GROUP BY uf
ORDER BY medalha_count DESC;

-- Lista dos municipios com maior número de medalhas
SELECT  municipio, COUNT(medalha) medalha_count
FROM data_medalhas 
GROUP BY  municipio
ORDER BY medalha_count DESC