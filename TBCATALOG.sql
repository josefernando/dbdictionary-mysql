LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\TradeInformationConsolidatedFile_20200616_1.csv'
IGNORE
INTO TABLE pregao_b3 FIELDS TERMINATED BY ';' LINES TERMINATED BY '\r\n' IGNORE 1 lines
(
data_pregao  ,
codigo_negociacao_papel  ,
international_securities_id_num  ,
segmento  ,
@preco_minimo  ,
@preco_maximo  ,
@preco_medio  ,
@preco_fechamento ,
@preco_cotacao_ajuste ,
@preco_ref ,
total_negocios  ,
total_titulos_negociados  
)
SET preco_minimo = REPLACE(@preco_minimo, ',', '.')
    , preco_maximo = REPLACE(@preco_maximo, ',', '.')
    , preco_medio = REPLACE(@preco_medio, ',', '.')
    , preco_fechamento = REPLACE(@preco_fechamento, ',', '.')
    , preco_cotacao_ajuste = REPLACE(@preco_cotacao_ajuste, ',', '.')
    , preco_ref = REPLACE(@preco_ref, ',', '.');
