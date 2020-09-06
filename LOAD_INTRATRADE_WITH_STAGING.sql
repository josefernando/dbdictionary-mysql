-- ============================================== LOAD  INTRADAY TRADE STAGING  ==================================================
-- download page: http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/cotacoes/cotacoes/
use b3;

truncate tb_intraday_trade_staging;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\TradeIntraday_20200904_1.txt'
-- IGNORE
INTO TABLE tb_intraday_trade_staging FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 lines
( 
data_referencia ,
codigo_negociacao_papel ,
acao ,   
@preco_negocio ,
titulos_negociados , 
@hora_negocio,
id_negocio,
tipo_sessao_pregao ,
data_pregao 
)

SET preco_negocio = REPLACE(@preco_negocio, ',', '.'),
    hora_negocio = CONCAT(SUBSTR( @hora_negocio,1,2),':',SUBSTR(@hora_negocio,3,2),':',SUBSTR(@hora_negocio,5,2),'.',SUBSTR(@hora_negocio,7,3))
;

-- ============================================== INSERT into from stagig ================================================
-- TRUNCATE tb_intraday_trade;
LOCK TABLES tb_intraday_trade WRITE, tb_intraday_trade_staging WRITE;
INSERT INTO tb_intraday_trade (
						  data_referencia
						, codigo_negociacao_papel
						, acao 
						, preco_negocio
						, titulos_negociados
						, hora_negocio
						, id_negocio
						, tipo_sessao_pregao
						, data_pregao
)
					select data_referencia
						, codigo_negociacao_papel
						, acao 
						, preco_negocio
						, titulos_negociados
						, hora_negocio
						, id_negocio
						, tipo_sessao_pregao
						, data_pregao
					from tb_intraday_trade_staging
                    where codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$|^[qtweryuiopasdfghjklzxcvbnm]{4}[1]{2}$'
                    order by data_pregao, hora_negocio
				;
UNLOCK TABLES;	

use b3;

SELECT  distinct data_pregao from tb_intraday_trade ;
 -- select *  from tb_oportunity_intraday_control;