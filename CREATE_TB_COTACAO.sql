use b3;
-- ====================================================================================================================

-- drop table IF EXISTS tb_cotacao;

create table  tb_cotacao (
id int NOT NULL AUTO_INCREMENT,
data_cotacao date ,
codigo_bdi varchar(2), 
codigo_negociacao_papel varchar(12) ,
tipo_mercado tinyint ,
nome_resumido_empresa_emissora varchar(12), 
especificacao_papel varchar(10) , 
prazo_dias_mercado_termo varchar(3), 
moeda_referencia varchar(4) ,
preco_abertura decimal(13,2) ,
preco_maximo decimal(13,2) ,
preco_minimo decimal(13,2) ,
preco_medio decimal(13,2) ,
preco_fechamento decimal(13,2) ,
preco_melhor_oferta_compra decimal(13,2) , 
preco_melhor_oferta_venda decimal(13,2) ,
total_negocios int ,
total_titulos_negociados bigint , 
volume_titulos_negociados decimal(18,2) ,
preco_exercicio_mercado_opcoes decimal(13,2) , 
indicador_correcao_preco tinyint ,
data_vencimento_mercado_opcoes date ,
fator_cotacao_papel int ,
preco_exercio_pontos decimal(13,2) ,
codigo_interno_papel varchar(12) ,
numero_distrib_papel int,

PRIMARY KEY (id)
)
ENGINE = MYISAM;  --  única maneira de criar non clustered

/*
DO NOT CREATE A PRIMARY KEY: PERFORMANCE ON LOAD
ALTER TABLE tb_cotacao
  ADD CONSTRAINT PK_cotacao
    PRIMARY KEY (data_cotacao, codigo_negociacao_papel, tipo_mercado , especificacao_papel, prazo_dias_mercado_termo);     
commit;
*/

-- DROP INDEX codigo_negociacao_papel_ix ON tb_cotacao;

-- IMPORTANTE: Index non-clustered, por conta de ENGINE=MYISAM
CREATE UNIQUE INDEX codigo_negociacao_papel_ix ON tb_cotacao (codigo_negociacao_papel, codigo_bdi, data_cotacao, prazo_dias_mercado_termo);
commit;

-- IMPORTANTE: Index non-clustered, por conta de ENGINE=MYISAM
CREATE INDEX data_cotacao_ix ON tb_cotacao (data_cotacao, codigo_negociacao_papel);
commit;

select count(*) from tb_cotacao;

-- ====================================================================================================================
use b3;

-- drop table IF EXISTS tb_feriado;

create table  tb_feriado (
id int NOT NULL AUTO_INCREMENT,
data date ,
descricao varchar(100),

PRIMARY KEY (id)
)
ENGINE = MYISAM;  --  única maneira de criar non clustered

/*
DO NOT CREATE A PRIMARY KEY: PERFORMANCE ON LOAD
ALTER TABLE tb_cotacao
  ADD CONSTRAINT PK_cotacao
    PRIMARY KEY (data_cotacao, codigo_negociacao_papel, tipo_mercado , especificacao_papel, prazo_dias_mercado_termo);     
commit;
*/

-- IMPORTANTE: Index non-clustered, por conta de ENGINE=MYISAM
CREATE UNIQUE INDEX tb_feriado_ix ON tb_feriado (data);
commit;

-- FERIADOS B3 BM&FBOVESPA 2020 ==========================================================================================
INSERT INTO tb_feriado (data, descricao) VALUES ('20200101', 'Confraternização Universal');
INSERT INTO tb_feriado (data, descricao) VALUES ('20200224', 'Carnaval');
INSERT INTO tb_feriado (data, descricao) VALUES ('20200225', 'Carnaval');
INSERT INTO tb_feriado (data, descricao) VALUES ('20200410', 'Paixão de Cristo');
INSERT INTO tb_feriado (data, descricao) VALUES ('20200421', 'Tiradentes');
INSERT INTO tb_feriado (data, descricao) VALUES ('20200501', 'Dia do trabalho');
INSERT INTO tb_feriado (data, descricao) VALUES ('20200611', 'Corpus Christi');
-- INSERT INTO tb_feriado (data, descricao) VALUES ('20200709', 'Revolução Constitucionalista');
INSERT INTO tb_feriado (data, descricao) VALUES ('20200907', 'Independência do Brasil');
INSERT INTO tb_feriado (data, descricao) VALUES ('20201012', 'Nossa Senhora Aparecida');
INSERT INTO tb_feriado (data, descricao) VALUES ('20201102', 'Finados');
INSERT INTO tb_feriado (data, descricao) VALUES ('20201224', 'Véspera de Natal');
INSERT INTO tb_feriado (data, descricao) VALUES ('20201225', 'Natal');
INSERT INTO tb_feriado (data, descricao) VALUES ('20201231', 'Expediente interno');
INSERT INTO tb_feriado (data, descricao) VALUES ('20210101', 'Ano Novo');

-- =======================================================================================================================
use b3;

-- drop table IF EXISTS tb_intraday_trade;
-- layout ref.: http://www.b3.com.br/data/files/4F/91/A8/CD/2A280710E7BCA507DC0D8AA8/TradeIntradayFile.pdf
create table  tb_intraday_trade (
id int NOT NULL AUTO_INCREMENT,
data_referencia date ,
codigo_negociacao_papel varchar(30) ,
acao tinyint ,   
preco_negocio decimal(13,3) ,
titulos_negociados bigint , 
hora_negocio time(3),
id_negocio bigint,
tipo_sessao_pregao tinyint ,
data_pregao date, 

PRIMARY KEY (id)
)
ENGINE = MYISAM;  --  única maneira de criar non clustered

-- ALTER TABLE tb_intraday_trade DROP INDEX intraday_trade_ix0;

CREATE UNIQUE INDEX intraday_trade_ix0 ON tb_intraday_trade (id, data_pregao , hora_negocio, codigo_negociacao_papel, id_negocio);
CREATE UNIQUE INDEX intraday_trade_ix1 ON tb_intraday_trade (data_pregao , hora_negocio, codigo_negociacao_papel, id_negocio);

-- =======================================================================================================================
use b3;

-- drop table IF EXISTS tb_intraday_trade_staging;
-- layout ref.: http://www.b3.com.br/data/files/4F/91/A8/CD/2A280710E7BCA507DC0D8AA8/TradeIntradayFile.pdf
create table  tb_intraday_trade_staging (
data_referencia date ,
codigo_negociacao_papel varchar(30) ,
acao tinyint ,   
preco_negocio decimal(13,3) ,
titulos_negociados bigint , 
hora_negocio time(3),
id_negocio bigint,
tipo_sessao_pregao tinyint ,
data_pregao date
)
ENGINE = MYISAM;  --  única maneira de criar non clustered

-- ALTER TABLE tb_intraday_trade DROP INDEX intraday_trade_ix0;

CREATE  INDEX intraday_trade_staging_ix ON tb_intraday_trade_staging (codigo_negociacao_papel);
CREATE unique INDEX intraday_trade_staging_ix1 ON tb_intraday_trade_staging (data_pregao , hora_negocio, codigo_negociacao_papel, id_negocio);

-- ============================================== LOAD  INTRADAY TRADE STAGING  ==================================================
-- download page: http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/cotacoes/cotacoes/
use b3;

truncate tb_intraday_trade_staging;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\TradeIntraday_20200903_1.txt'
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
                    where codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$' -- |^[qtweryuiopasdfghjklzxcvbnm]{4}[1]{2}$'
                    order by data_pregao, hora_negocio
				;
UNLOCK TABLES;	
-- ============================================== LOAD  INTRADAY TRADE  ==================================================
select  * from tb_intraday_trade where codigo_negociacao_papel = 'BOVA11';
-- ========================================================================================================================                    
-- download page: http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/cotacoes/cotacoes/
use b3;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\TradeIntraday_20200831_1.txt'
-- IGNORE
INTO TABLE tb_intraday_trade FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 lines
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
-- ========================================================================================================================
 -- SET @horaneg = '123456789';
 -- SET @horaneg01 = CONCAT(SUBSTR(@horaneg,1,2),':',SUBSTR(@horaneg,3,2),':',SUBSTR(@horaneg,5,2),'.',SUBSTR(@horaneg,7,3));
 -- SELECT @horaneg01;
 -- 12:34:56.789

select * from tbx_intraday_trade
       where data_pregao = '20200805' ;
-- =======================================================================================================================

select * from tb_cotacao where codigo_negociacao_papel = 'BBDC4';

select count(*) from tb_cotacao;

select * from tb_cotacao tcot where codigo_negociacao_papel = 'BBDC4' 
                              and tcot.data_cotacao = '2020-06-08';
-- truncate tb_cotacao;
-- truncate tb_cotacao;
set autocommit = 0;
set unique_checks = 0;
set foreign_key_checks = 0;
set sql_log_bin=0;


select distinct codigo_negociacao_papel, nome_resumido_empresa_emissora from tb_cotacao;

select * from tb_cotacao where codigo_negociacao_papel = 'vvar3' and data_cotacao between '20200710' and '20200710' ;


select distinct codigo_negociacao_papel from tb_cotacao where data_cotacao between '20200601' and '20200619';

select  data_cotacao
        ,codigo_negociacao_papel
        ,preco_abertura
        ,preco_fechamento
        ,total_negocios
        ,total_titulos_negociados
        ,volume_titulos_negociados
        from tb_cotacao 
        where data_cotacao between '20190601' and '20190915'
		and codigo_negociacao_papel = 'PMAM3';
        
-- ======================================================================================================================        
 truncate table tb_cotacao;

select count(*) from  tb_cotacao;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\COTAHIST_D04082020.LOAD'  
-- ignore
INTO TABLE tb_cotacao FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' 
( 
data_cotacao,
codigo_bdi,
codigo_negociacao_papel,
tipo_mercado,
nome_resumido_empresa_emissora, 
especificacao_papel, 
prazo_dias_mercado_termo, 
moeda_referencia, 
preco_abertura,
preco_maximo,
preco_minimo,
preco_medio, 
preco_fechamento, 
preco_melhor_oferta_compra, 
preco_melhor_oferta_venda, 
total_negocios,              -- transações: quantidade de transações, quantas vezes o título trocou de mão
total_titulos_negociados,    -- títulos negociados: quantidade de títulos envolvidos nas transações
volume_titulos_negociados,   -- valores negociados: soma dos valores envolvidos nas transações
preco_exercicio_mercado_opcoes, 
indicador_correcao_preco, 
data_vencimento_mercado_opcoes,
fator_cotacao_papel, 
preco_exercio_pontos,
codigo_interno_papel,
numero_distrib_papel
);

select max(data_cotacao) from tb_cotacao;
-- ===========================================================================================================
/*
use b3;
        DROP TABLE IF EXISTS tb_oportunity_intraday;

        CREATE TABLE tb_oportunity_intraday ( data_trade date,
                ultimo_horario_analisado time,
				codigo_negociacao_papel varchar(12),
                preco_abertura decimal(13,3),
                ultimo_preco   decimal(13,3),
                ultimo_id      bigint,
			    total_negocios int,
                upOrDownUntilNow decimal(13,3),
                total_up int,    -- número de vezes que o preço subiu
                total_down int   -- número de vezes que o preço desceu
        );
        
-- ===========================================================================================================

use b3;
        DROP TABLE IF EXISTS tb_oportunity_intraday_result;

        CREATE TABLE tb_oportunity_intraday_result ( data_trade date,
                ultimo_horario_analisado time,
				codigo_negociacao_papel varchar(12),
                preco_abertura decimal(13,3),
                ultimo_preco   decimal(13,3),
                ultimo_id      bigint,
			    total_negocios int,
                upOrDownUntilNow decimal(13,3),
                total_up int,    -- número de vezes que o preço subiu
                total_down int   -- número de vezes que o preço desceu
        );        
CREATE INDEX oportunity_intraday_result_ix ON tb_oportunity_intraday_result (data_trade, codigo_negociacao_papel, ultimo_id);
commit;
*/
-- ===========================================================================================================
DROP PROCEDURE IF EXISTS IS_FERIADO;
DELIMITER //
CREATE PROCEDURE IS_FERIADO ( date varchar(10), OUT is_feriado boolean)
BEGIN
	SET @pos=0;
    select count(*) into @pos from tb_feriado where data = date;
	if(@pos > 0) then
      SET is_feriado = true;
    else
      SET is_feriado = false;
    end if;  
END   //
DELIMITER ;

CALL IS_FERIADO('20200101', @isferiado);
SELECT @isferiado;

-- ============================================================================================================
DROP PROCEDURE IF EXISTS FIND_NEGOTIATED_PAPERS_BY_DATE;
DELIMITER //
CREATE PROCEDURE FIND_NEGOTIATED_PAPERS_BY_DATE ( date varchar(10))
BEGIN
    CALL FIND_ORPREVIOUS_BUSINESS_DAY (date, @orStartDate); 
    SET date = @orStartDate;
    SELECT distinct codigo_negociacao_papel FROM tb_cotacao WHERE data_cotacao = date;
END   //
DELIMITER ;

-- ==========================================================================================================
DROP PROCEDURE IF EXISTS FIND_NEGOTIATED_PAPERS_BETWEEN_DATES;
DELIMITER //
CREATE PROCEDURE FIND_NEGOTIATED_PAPERS_BETWEEN_DATES ( startDate varchar(10), endDate varchar(10))
BEGIN
    CALL FIND_ORPREVIOUS_BUSINESS_DAY (startDate, @orStartDate); 
    SET startDate = @orStartDate;
    CALL FIND_ORNEXT_BUSINESS_DAY (endDate, @orEndDate); 
    SET endDate = @orEndDate;
    SELECT distinct codigo_negociacao_papel FROM tb_cotacao WHERE data_cotacao between startDate and endDate;
END   //
DELIMITER ;


-- ============================================================================================================
DROP PROCEDURE IF EXISTS FIND_NEGOTIATED_STOCK_SYMBOL_BETWEEN_DATES;
DELIMITER //
CREATE PROCEDURE FIND_NEGOTIATED_STOCK_SYMBOL_BETWEEN_DATES ( startDate varchar(10), endDate varchar(10))
BEGIN
	CALL FIND_ORPREVIOUS_BUSINESS_DAY (startDate, @orStartDate);
    SET startDate = @orStartDate;
    CALL FIND_ORNEXT_BUSINESS_DAY (endDate, @orEndDate);
	SET	 endDate = orEndDate;

    SELECT distinct codigo_negociacao_papel FROM tb_cotacao WHERE data_cotacao between startDate and endDate;
END   //
DELIMITER ;


call FIND_NEGOTIATED_PAPERS_BY_DATE('20200407');

call FIND_NEGOTIATED_PAPERS_BETWEEN_DATES('2020-04-07', '2020-04-08' );

-- =============================================================================================================
DROP PROCEDURE IF EXISTS GET_NEGOTIATED_PAPERS_BETWEEN_DATES;
DELIMITER //
CREATE PROCEDURE GET_NEGOTIATED_PAPERS_BETWEEN_DATES ( paper varchar(10), startDate varchar(10), endDate varchar(10))
BEGIN
	SELECT distinct codigo_negociacao_papel FROM tb_cotacao WHERE 
	codigo_negociacao_papel like CONCAT( paper, '%')
	and data_cotacao between startDate and endDate;
END   //
DELIMITER ;

call GET_NEGOTIATED_PAPERS_BETWEEN_DATES('MGL', '2020-04-07', '2020-04-08' );

-- ==========================================================================================================
DROP PROCEDURE IF EXISTS FIND_ORPREVIOUS_BUSINESS_DAY;
DELIMITER //
CREATE PROCEDURE FIND_ORPREVIOUS_BUSINESS_DAY (date varchar(10), OUT orDate varchar(10))
BEGIN
	SET @start = date;
    SET @isferiado = false;
    CALL IS_FERIADO(@start, @isferiado);
    
	WHILE ((WEEKDAY(@start) > 4) or @isferiado) DO
		   SET @start = @start - INTERVAL 1 day;
           CALL IS_FERIADO(@start, @isferiado);
	END WHILE;
	SET orDate = @start;
END   //
DELIMITER ;

SET @xx = '2020-08-04' - INTERVAL 1 DAY;

CALL FIND_ORPREVIOUS_BUSINESS_DAY (@xx, @orD);
select @orD;

-- ===============================================================================================================
DROP PROCEDURE IF EXISTS FIND_ORNEXT_BUSINESS_DAY;
DELIMITER //
CREATE PROCEDURE FIND_ORNEXT_BUSINESS_DAY (date varchar(10), OUT orDate varchar(10))
BEGIN
	SET @start = date;
    SET @isferiado = false;
    CALL IS_FERIADO(@start, @isferiado);
	WHILE  ((WEEKDAY(@start) > 4) or @isferiado) DO
		   SET @start = @start + INTERVAL 1 day;
           CALL IS_FERIADO(@start, @isferiado);
	END WHILE;
	SET orDate = @start;
END   //
DELIMITER ;
CALL FIND_ORNEXT_BUSINESS_DAY ('2020-09-07', @orend );
select @orend;
-- ================================================================================================================
use b3;

DROP PROCEDURE IF EXISTS FIND_OPORTUNITIES_DOWN;

DELIMITER //
CREATE PROCEDURE FIND_OPORTUNITIES_DOWN (  start_date varchar(10)
                                  , price_percent_param tinyint
                                  , total_negocios_percent_param tinyint
                                  , total_negocios_param int)
	BEGIN
       -- working vars
		DECLARE finished INTEGER DEFAULT 0;

        -- into vars
        DECLARE wcodigo_negociacao_papel varchar(12);

--
		DECLARE curStockSymbols
			CURSOR FOR 
				select  distinct codigo_negociacao_papel
					from tb_cotacao
					where data_cotacao between start_date and NOW()
                    and length(codigo_negociacao_papel) < 7;
--                    limit 100;

		-- declare NOT FOUND handler
		DECLARE CONTINUE HANDLER 
			FOR NOT FOUND SET finished = 1; 
 
        DROP TEMPORARY TABLE if exists tmp_result ;
        
        CREATE TEMPORARY TABLE tmp_result (	data_cotacao varchar(10),
				codigo_negociacao_papel varchar(12),
				preco_fechamento_ant decimal(13,2) ,
				preco_fechamento decimal(13,2) ,
                result_fechamento decimal (13,2),
                result_negocios   decimal (13,2),
			    total_negocios int,
                volume_titulos_negociados decimal(18,2) 
        );
            
       SET @previous_date = start_date - INTERVAL 1 DAY;
	   CALL FIND_ORPREVIOUS_BUSINESS_DAY (@previous_date, @orD);
	   SET  start_date = @orD;     
            
        OPEN curStockSymbols;   

        fetch_symbols: REPEAT
           FETCH curStockSymbols INTO   wcodigo_negociacao_papel;
                   
           CALL FIND_OPORTUNITY_DOWN (wcodigo_negociacao_papel, start_date , price_percent_param, total_negocios_percent_param, total_negocios_param);         
                    
            IF finished = 1 THEN 
				LEAVE fetch_symbols;
		    END IF;
        
         UNTIL finished = 1 END REPEAT fetch_symbols;
         
         CLOSE curStockSymbols; 
         
         select * from tmp_result order by codigo_negociacao_papel, data_cotacao;
	END   //
DELIMITER ; 

-- ================================================================================================================
use b3;

DROP PROCEDURE IF EXISTS FIND_OPORTUNITIES;

DELIMITER //
CREATE PROCEDURE FIND_OPORTUNITIES (  start_date varchar(10)
                                  , price_percent_param tinyint
                                  , total_negocios_percent_param tinyint
                                  , total_negocios_param int)
	BEGIN
       -- working vars
		DECLARE finished INTEGER DEFAULT 0;

        -- into vars
        DECLARE wcodigo_negociacao_papel varchar(12);

--
		DECLARE curStockSymbols
			CURSOR FOR 
				select  distinct codigo_negociacao_papel
					from tb_cotacao
					where data_cotacao between start_date and NOW()
                    and length(codigo_negociacao_papel) < 7;
--                    limit 100;

		-- declare NOT FOUND handler
		DECLARE CONTINUE HANDLER 
			FOR NOT FOUND SET finished = 1; 
 
        DROP TEMPORARY TABLE if exists tmp_result ;
        
        CREATE TEMPORARY TABLE tmp_result (	data_cotacao varchar(10),
				codigo_negociacao_papel varchar(12),
				preco_fechamento_ant decimal(13,2) ,
				preco_fechamento decimal(13,2) ,
                result_fechamento decimal (13,2),
                result_negocios   decimal (13,2),
			    total_negocios int,
                volume_titulos_negociados decimal(18,2) 
        );
            

            
       SET @previous_date = start_date - INTERVAL 1 DAY;
	   CALL FIND_ORPREVIOUS_BUSINESS_DAY (@previous_date, @orD);
	   SET  start_date = @orD;     
            
        OPEN curStockSymbols;   

        fetch_symbols: REPEAT
           FETCH curStockSymbols INTO   wcodigo_negociacao_papel;
                   
           CALL FIND_OPORTUNITY (wcodigo_negociacao_papel, start_date , price_percent_param, total_negocios_percent_param, total_negocios_param);         
                    
            IF finished = 1 THEN 
				LEAVE fetch_symbols;
		    END IF;
        
         UNTIL finished = 1 END REPEAT fetch_symbols;
         
         CLOSE curStockSymbols; 
         
         select * from tmp_result order by codigo_negociacao_papel, data_cotacao;
	END   //
DELIMITER ; 

-- ================================================================================================================
USE b3;

DROP PROCEDURE IF EXISTS FIND_OPORTUNITY_INTRADAY_BY_STOCK;

DELIMITER //
CREATE PROCEDURE FIND_OPORTUNITY_INTRADAY_BY_STOCK (stock_code varchar(12) , start_date varchar(10), pinterval int, OUT error_code int, OUT error_msg varchar(255))
proc_01:	BEGIN
        -- working vars
		DECLARE finished INTEGER DEFAULT 0;
		DECLARE COUNT integer DEFAULT 0;
        DECLARE COUNT_TOTAL integer DEFAULT 0;
        -- 
        
        DECLARE wcount int;
        
        -- into vars
		    DECLARE wcodigo_negociacao_papel varchar(30);
		    DECLARE wpreco_negocio decimal(13,3);
   		  DECLARE whorario_primeiro_negocio time(3);
		    DECLARE wtitulos_negociados int;
		    DECLARE whora_negocio time(3);
        DECLARE wid_negocio int;
		    declare wdata_pregao date;
        declare wultimo_horario_analisado time(3);
        
        DECLARE wlast_date date;
        DECLARE wlast_time time(3);
        DECLARE wlast_codigo_negociacao_papel varchar(12);
        DECLARE wlast_id_trade int;
        
         DECLARE wid int;

         DECLARE woportunity_data_trade date;
         DECLARE woportunity_horario_primeiro_negocio time(3);
         DECLARE woportunity_ultimo_horario_analisado time(3);
		     DECLARE woportunity_codigo_negociacao_papel varchar(12);
         DECLARE woportunity_preco_abertura decimal(13,3);
         DECLARE woportunity_ultimo_preco decimal(13,3);
         DECLARE woportunity_ultimo_id    int ;   
		     DECLARE woportunity_titulos_negociados int;
         DECLARE woportunity_UpOrDownUntilNow decimal(13,3);
         DECLARE woportunity_total_up int;    
         DECLARE woportunity_total_same int;    
         DECLARE woportunity_total_down int;
        
        -- ========================================================================================
        -- algorith vars
        DECLARE wresultado_preco_negocio decimal(13,3) DEFAULT 0;
       	DECLARE wUpOrDown decimal(13,3);
        DECLARE wUpOrDownUntilNow decimal(13,3);
        DECLARE wtotal_up int;
        DECLARE wtotal_same int;
        DECLARE wtotal_down int;
        
        -- ============================================================================================
        
        -- Error handling 
          DECLARE sql_state CHAR(5) DEFAULT '00000';
		      DECLARE sql_msg_text TEXT;
          DECLARE sql_mysql_error_number int;
          DECLARE sql_exception boolean DEFAULT false;
		      DECLARE nrows INT;
		      DECLARE result TEXT;
          
--
		DECLARE curTradeIntraDay
			CURSOR FOR 
				select    id
                        , data_pregao
                        , codigo_negociacao_papel
                        , preco_negocio
                        , titulos_negociados
                        , hora_negocio
                        , id_negocio
					from tb_intraday_trade
					where data_pregao = start_date
                    and codigo_negociacao_papel like CONCAT(stock_code,'%')
		            and id > wid
                    and hora_negocio < time(wultimo_horario_analisado) + interval +pinterval minute
                    and length(codigo_negociacao_papel) < 7
                    order by id, data_pregao, hora_negocio
				;
      
      DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished = 1;
      
      DECLARE EXIT HANDLER
		FOR SQLEXCEPTION   -- RETURNED_SQLSTATE not starting with '00', '01' or '02'
			BEGIN
			  GET DIAGNOSTICS CONDITION 1
				  sql_state = RETURNED_SQLSTATE
                , sql_msg_text = MESSAGE_TEXT
                , sql_mysql_error_number = MYSQL_ERRNO;
                
                SET error_code = sql_mysql_error_number;
                SET error_msg  = CONCAT( sql_state, ' - ' , sql_msg_text);
                set sql_exception = true;
			END;           
          
	IF COUNT_TOTAL = 0 THEN
		SELECT id, hora_negocio into wid, wultimo_horario_analisado
			from tb_intraday_trade
			where data_pregao = start_date
			and codigo_negociacao_papel like CONCAT(stock_code,'%')
			and length(codigo_negociacao_papel) < 7
			order by id, data_pregao, hora_negocio limit 1;
		IF finished = 1 then  
			SET error_code = 1;
			SET error_msg = CONCAT('Não existe Transações para a data: ' , start_date);
			LEAVE proc_01;
		END IF; 
        SET wid = wid - 1;  -- para incluir o primeira transação desta ação
	ELSE 
		SELECT id, hora_negocio into wid, wultimo_horario_analisado
		from tb_intraday_trade
		where data_pregao = start_date
		and codigo_negociacao_papel like CONCAT(stock_code,'%')
		and length(codigo_negociacao_papel) < 7
		and id > ( select id 
					from tb_oportunity_intraday_control_by_stock
					where last_date = start_date and codigo_negociacao_papel = stock_code)
					order by id, data_pregao, hora_negocio limit 1;
		IF finished = 1 then  
			SET error_code = 11;
			SET error_msg = CONCAT('Não existe Transações depois desta data/horário: ' , start_date);
			LEAVE proc_01;
		END IF;
    END IF;

	OPEN curTradeIntraDay; 

	START TRANSACTION;
    fetch_trading: REPEAT
		FETCH curTradeIntraDay INTO   wid
					, wdata_pregao
					, wcodigo_negociacao_papel
					, wpreco_negocio
					, wtitulos_negociados
					, whora_negocio
					, wid_negocio;

		IF finished = 1 then
			IF COUNT_TOTAL = 0 THEN
				SET error_code = 111;
				SET error_msg = CONCAT('Sem Transações para o período!: ', wid, ' - ',  start_date, ' - ', wLast_time, ' - ', stock_code, ' - ', pinterval)  ;
				LEAVE proc_01;
			 ELSE 
				LEAVE fetch_trading;
			 END IF;
		END IF;                        

		SELECT * INTO woportunity_data_trade,
			woportunity_horario_primeiro_negocio,
			woportunity_ultimo_horario_analisado,
			woportunity_codigo_negociacao_papel,
			woportunity_preco_abertura,
			woportunity_ultimo_preco,
			woportunity_ultimo_id,
			woportunity_titulos_negociados,
			woportunity_UpOrDownUntilNow,
			woportunity_total_up,    
			woportunity_total_same,    
			woportunity_total_down 
			FROM tb_oportunity_intraday_by_stock
			WHERE codigo_negociacao_papel = wcodigo_negociacao_papel;
                
			IF finished = 1 THEN   -- primeira transação da  ação para a data
               SET finished = 0;
				INSERT INTO tb_oportunity_intraday_by_stock VALUES (
						  wdata_pregao
						, whora_negocio     -- primeiro negocio  
						, whora_negocio     -- ultimo negocio
						, wcodigo_negociacao_papel
						, wpreco_negocio
						, wpreco_negocio
						, wid_negocio 
						, wtitulos_negociados
						, 0           -- UpOrDownUntilNow
						, 0           -- total_up
						, 0           -- = same
						, 0);         -- total_down
				   
				   GET DIAGNOSTICS nrows = ROW_COUNT;
				   IF nrows = 0 then
					 SET error_msg = 'Erro na inclusão da tabela tb_oportunity_intraday';
					 SET error_code = 8001;
					 leave proc_01;
				   END IF;
                   
					INSERT INTO tb_oportunity_intraday_control_by_stock 
                         ( last_date, last_time, codigo_negociacao_papel, last_id)
                           VALUES (start_date, whora_negocio, wcodigo_negociacao_papel, wid_negocio);
                           
				GET DIAGNOSTICS nrows = ROW_COUNT;
			   if nrows = 0 then
				 SET error_msg = 'Erro na INSERÇÃO TB_oportunity_intraday_control_by_stock';
				 SET error_code = 8005;
				 leave proc_01;
			   end if;       
            ELSE      -- transações already exists
				SET wUpOrDown = (wpreco_negocio / woportunity_ultimo_preco - 1) * 100;
                SET wUpOrDownUntilNow = (wpreco_negocio / woportunity_preco_abertura - 1) * 100;

				IF wUpOrDown > 0 THEN
						SET woportunity_total_up =  woportunity_total_up + 1;
				ELSEIF wUpOrDown < 0 THEN
						SET woportunity_total_down =  woportunity_total_down + 1;
				ELSEIF wUpOrDown = 0 THEN
                        SET woportunity_total_same = woportunity_total_same + 1;
				END IF;
                
				UPDATE tb_oportunity_intraday_by_stock
					SET ultimo_horario_analisado = whora_negocio,
						ultimo_preco = wpreco_negocio,
						titulos_negociados = woportunity_titulos_negociados + wtitulos_negociados,
						ultimo_id = wid_negocio,
                        resultado_ate_agora = wUpOrDownUntilNow,
                        total_up  = woportunity_total_up, 
                        total_same = woportunity_total_same,
						total_down = woportunity_total_down
                    WHERE codigo_negociacao_papel = wcodigo_negociacao_papel;
 			
			   GET DIAGNOSTICS nrows = ROW_COUNT;
			   IF nrows = 0 then
				 SET error_msg = 'Erro na atualização  tb_oportunity_intraday';
				 SET error_code = 8002;
				 leave proc_01;
			   END IF;
		  END IF;

		  UPDATE tb_oportunity_intraday_control_by_stock
			SET id = wid,
				last_time = whora_negocio,
				last_id = wid_negocio
				WHERE last_date = wdata_pregao 
                and	codigo_negociacao_papel = wcodigo_negociacao_papel;

                
           GET DIAGNOSTICS nrows = ROW_COUNT;
		   if nrows = 0 then
			 SET error_msg = 'Erro na atualização tb_oportunity_intraday_control_by_stock';
			 SET error_code = 8003;
			 leave proc_01;
		   end if;

		 SET COUNT = COUNT + 1;
         
         IF COUNT % 1000 = 0 THEN
			COMMIT;
            SET COUNT_TOTAL = COUNT_TOTAL + COUNT;
            SET COUNT = 0;
		 END IF;

         UNTIL finished = 1 END REPEAT fetch_trading;

         COMMIT;
         CLOSE curTradeIntraDay; 
         
         SET error_code = 0;
		 SET error_msg = 'Processamento OK!' ;    
    
END   //
DELIMITER ;
-- ================================================================================================================
use b3;

Set @@GLOBAL.innodb_change_buffering=all;   
SET autocommit=0;

set @time_previous = now();
CALL FIND_OPORTUNITY_INTRADAY_BY_STOCK ('IRBR3', 20200902 , 10, @error_code, @error_msg);   -- IMPORTANTE VERIFIQUE LIMIT DE LINHAS RETORNADAS
select timediff(now(),@time_previous);
select @error_code, @error_msg;

INSERT INTO tb_oportunity_intraday_result
   		SELECT  distinct t1.*  FROM tb_oportunity_intraday t1
             inner JOIN tb_oportunity_intraday_result t2
             ON t1.codigo_negociacao_papel = t2.codigo_negociacao_papel and t1.data_trade = t2.data_trade
	         where t1.ultimo_id not in (select ultimo_id from tb_oportunity_intraday_result t3
                                        where t3.ultimo_id = t1.ultimo_id);

SELECT *   FROM tb_oportunity_intraday_control_by_stock;
SELECT *   FROM tb_oportunity_intraday_by_stock;

select    id
		, data_pregao
		, codigo_negociacao_papel
		, preco_negocio
		, titulos_negociados
		, hora_negocio
		, id_negocio
	from tb_intraday_trade
	where data_pregao = 20200902
	and codigo_negociacao_papel like CONCAT('IRBR3','%')
	and id >= 36507
	and hora_negocio between time('10:10:04.233')  and time('10:10:04.233') + interval +1 minute
	and length(codigo_negociacao_papel) < 7
	order by id, data_pregao, hora_negocio
;

-- ====================================================================================================================
 USE b3;

 DROP PROCEDURE IF EXISTS FIND_OPORTUNITY_INTRADAY;

DELIMITER //
CREATE PROCEDURE FIND_OPORTUNITY_INTRADAY (start_date varchar(10), pinterval int, OUT error_code int, OUT error_msg varchar(255))
proc_01:	BEGIN
        -- working vars
		DECLARE finished INTEGER DEFAULT 0;
		DECLARE COUNT integer DEFAULT 0;
        DECLARE COUNT_TOTAL integer DEFAULT 0;
        -- 
        
        DECLARE wcount int;
        
        -- into vars
			DECLARE wcodigo_negociacao_papel varchar(30);
		  DECLARE wpreco_negocio decimal(13,3);
   		DECLARE whorario_primeiro_negocio time(3);
		  DECLARE wtitulos_negociados int;
		  DECLARE whora_negocio time(3);
      DECLARE wid_negocio int;
		  DECLARE wdata_pregao date;
      DECLARE wultimo_horario_analisado time(3);
      
         DECLARE wmenor_preco    decimal(13,3);
         DECLARE whora_menor_preco time(3);
         DECLARE wid_menor_preco int;
         DECLARE wmaior_preco    decimal(13,3);
         DECLARE wid_maior_preco int;
         DECLARE whora_maior_preco time(3);
        
        DECLARE wlast_date date;
        DECLARE wlast_time time(3);
        DECLARE wlast_codigo_negociacao_papel varchar(12);
        DECLARE wlast_id_trade int;
        
         DECLARE wid int;
        
         DECLARE woportunity_data_trade date;
         DECLARE woportunity_horario_primeiro_negocio time(3);
         DECLARE woportunity_ultimo_horario_analisado time(3);
		     DECLARE woportunity_codigo_negociacao_papel varchar(12);
         DECLARE woportunity_preco_abertura decimal(13,3);
         DECLARE woportunity_menor_preco    decimal(13,3);
         DECLARE woportunity_hora_menor_preco time(3);
         DECLARE woportunity_id_menor_preco int;
         DECLARE woportunity_maior_preco    decimal(13,3);
         DECLARE woportunity_id_maior_preco int;
         DECLARE woportunity_hora_maior_preco time(3);
         DECLARE woportunity_ultimo_preco decimal(13,3);
         DECLARE woportunity_ultimo_id    int ;   
		     DECLARE woportunity_titulos_negociados int;
         DECLARE woportunity_UpOrDownUntilNow decimal(13,3);
         DECLARE woportunity_total_up int;    
         DECLARE woportunity_total_same int;    
         DECLARE woportunity_total_down int;
        
        -- ========================================================================================
        -- algorith vars
        DECLARE wresultado_preco_negocio decimal(13,3) DEFAULT 0;
       	DECLARE wUpOrDown decimal(13,3);
        DECLARE wUpOrDownUntilNow decimal(13,3);
        DECLARE wtotal_up int;
        DECLARE wtotal_same int;
        DECLARE wtotal_down int;
        
        -- ============================================================================================
        
        -- Error handling 
          DECLARE sql_state CHAR(5) DEFAULT '00000';
		  DECLARE sql_msg_text TEXT;
          DECLARE sql_mysql_error_number int;
          DECLARE sql_exception boolean DEFAULT false;
		  DECLARE nrows INT;
		  DECLARE result TEXT;

--
		DECLARE curTradeIntraDay
			CURSOR FOR 
				select    id
                        , data_pregao
                        , codigo_negociacao_papel
                        , preco_negocio
                        , titulos_negociados
                        , hora_negocio
                        , id_negocio
					from tb_intraday_trade
					where data_pregao = start_date
                    and codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$'
		            and id > wid 
                    and hora_negocio < time(wultimo_horario_analisado) + interval +pinterval minute
                    and length(codigo_negociacao_papel) < 7
                    order by id, data_pregao, hora_negocio
				;
                
	  -- declare NOT FOUND handler
	  DECLARE CONTINUE HANDLER
		FOR NOT FOUND SET finished = 1;
            
      DECLARE EXIT HANDLER
		FOR SQLEXCEPTION   -- RETURNED_SQLSTATE not starting with '00', '01' or '02'
			BEGIN
			  GET DIAGNOSTICS CONDITION 1
				  sql_state = RETURNED_SQLSTATE
                , sql_msg_text = MESSAGE_TEXT
                , sql_mysql_error_number = MYSQL_ERRNO;
                
                SET error_code = sql_mysql_error_number;
                SET error_msg  = CONCAT( sql_state, ' - ' , sql_msg_text);
                set sql_exception = true;
			END;      
      
	select id, last_time into wid,  wLast_time from tb_oportunity_intraday_control
				where last_date = start_date; 
/*                
    if sql_exception then
		leave proc_01;
    end if;    
 */
 
	if finished = 1 then
	   SET finished = 0;
	   SET wid = 0;
	   SET wLast_time = '09:59:59.999';
       
       insert into tb_oportunity_intraday_control values (wid, start_date, wLast_time, null, null);
       
/*                
    if sql_exception then
		leave proc_01;
    end if;    
 */     

       GET DIAGNOSTICS nrows = ROW_COUNT;
		  if nrows = 0 then
			SET error_msg = 'Zero linhas inseridasna tabela de controle';
            SET error_code = 8000;
            leave proc_01;
          end if;
	end if;
									
	SET wultimo_horario_analisado = wLast_time;

	OPEN curTradeIntraDay; 

	START TRANSACTION;
    fetch_trading: REPEAT
           FETCH curTradeIntraDay INTO   wid
                        , wdata_pregao
                        , wcodigo_negociacao_papel
                        , wpreco_negocio
                        , wtitulos_negociados
                        , whora_negocio
                        , wid_negocio;
                        
/*                
    if sql_exception then
		leave proc_01;
    end if;    
 */    

            IF finished = 1 THEN   
				IF COUNT_TOTAL = 0 THEN
					SET error_code = 1;
					SET error_msg = 'Sem Transações para o período! ' ;
					LEAVE proc_01;
                 ELSE 
					LEAVE fetch_trading;
                 END IF;   
			END IF;

			select * into woportunity_data_trade,
                woportunity_horario_primeiro_negocio,
                woportunity_ultimo_horario_analisado,
				        woportunity_codigo_negociacao_papel,
                woportunity_preco_abertura,
								woportunity_menor_preco,    
								woportunity_hora_menor_preco,
								woportunity_id_menor_preco, 
								woportunity_maior_preco,    
								woportunity_id_maior_preco, 
								woportunity_hora_maior_preco,              
                woportunity_ultimo_preco,
                woportunity_ultimo_id,
			          woportunity_titulos_negociados,
                woportunity_UpOrDownUntilNow,
                woportunity_total_up,    
                woportunity_total_same,    
                woportunity_total_down 
                from tb_oportunity_intraday
                where codigo_negociacao_papel = wcodigo_negociacao_papel;
                
/*                
    if sql_exception then
		leave proc_01;
    end if;    
 */
			IF finished = 1 THEN   -- primeira transação da  ação para a data
					SET finished = 0;
					INSERT INTO tb_oportunity_intraday VALUES (
							  wdata_pregao
              , whora_negocio     -- primeiro negocio  
							, whora_negocio     -- ultimo negocio
							, wcodigo_negociacao_papel
							, wpreco_negocio
 							, wpreco_negocio    -- menor preco
 							, wid_negocio       -- id menor preco
							, whora_negocio     -- hora menor preço
 							, wpreco_negocio    -- maior preco
 							, wid_negocio       -- id maior preco
							, whora_negocio     -- hora maior preço              
							, wpreco_negocio
							, wid_negocio 
							, wtitulos_negociados
							, 0           -- UpOrDownUntilNow
							, 0           -- total_up
							, 0           -- = same
							, 0);         -- total_down

/*                
    if sql_exception then
		leave proc_01;
    end if;    
 */
				   
				   GET DIAGNOSTICS nrows = ROW_COUNT;
				   if nrows = 0 then
						 SET error_msg = 'Zero linhas inseridasna tabela de controle';
						 SET error_code = 8001;
						 leave proc_01;
					 END IF;
    ELSE      -- transações already exists
        -- =================================================================================
        			if wpreco_negocio < woportunity_menor_preco then
								SET woportunity_menor_preco = wpreco_negocio;
              	SET woportunity_hora_menor_preco = whora_negocio;
								SET woportunity_id_menor_preco = wid_negocio;
              end if;
        			if wpreco_negocio > woportunity_maior_preco then
								SET woportunity_maior_preco = wpreco_negocio;
              	SET woportunity_hora_maior_preco = whora_negocio;
								SET woportunity_id_maior_preco = wid_negocio;
              end if;
        -- =================================================================================
    
				SET wUpOrDown = (wpreco_negocio / woportunity_ultimo_preco - 1) * 100;
        SET wUpOrDownUntilNow = (wpreco_negocio / woportunity_preco_abertura - 1) * 100;

				IF wUpOrDown > 0 THEN
						SET woportunity_total_up =  woportunity_total_up + 1;
				ELSEIF wUpOrDown < 0 THEN
						SET woportunity_total_down =  woportunity_total_down + 1;
				ELSEIF wUpOrDown = 0 THEN
                        SET woportunity_total_same = woportunity_total_same + 1;
				END IF;
                
				UPDATE tb_oportunity_intraday
					SET ultimo_horario_analisado = whora_negocio,
          
							menor_preco = woportunity_menor_preco,
							hora_menor_preco = woportunity_hora_menor_preco,
							id_menor_preco = woportunity_id_menor_preco,
							maior_preco = woportunity_maior_preco,
							id_maior_preco = woportunity_id_maior_preco,
							hora_maior_preco = woportunity_hora_maior_preco,
          
						ultimo_preco = wpreco_negocio,
						titulos_negociados = woportunity_titulos_negociados + wtitulos_negociados,
						ultimo_id = wid_negocio,
            resultado_ate_agora = wUpOrDownUntilNow,
            total_up  = woportunity_total_up, 
            total_same = woportunity_total_same,
						total_down = woportunity_total_down
                    WHERE codigo_negociacao_papel = wcodigo_negociacao_papel;
                    
/*                
    if sql_exception then
		leave proc_01;
    end if;    
 */    
              
			   if nrows = 0 then
				 SET error_msg = 'Zero linhas atualizadas tabela de oportunidades';
				 SET error_code = 8002;
				 leave proc_01;
			   end if;
		  END IF;
              
		  UPDATE tb_oportunity_intraday_control
			SET id = wid,
				last_time = whora_negocio,
				last_codigo_negociacao_papel = wcodigo_negociacao_papel,
				last_id = wid_negocio
				WHERE last_date = wdata_pregao;
                    
/*                
    if sql_exception then
		leave proc_01;
    end if;    
 */    
              
			   if nrows = 0 then
				 SET error_msg = 'Zero linhas atualizadas tabela de controle';
				 SET error_code = 8003;
				 leave proc_01;
			   end if;

		 SET COUNT = COUNT + 1;
         
         IF COUNT % 1000 = 0 THEN
			COMMIT;
            SET COUNT_TOTAL = COUNT_TOTAL + COUNT;
            SET COUNT = 0;
		 END IF;

         UNTIL finished = 1 END REPEAT fetch_trading;

         COMMIT;
         CLOSE curTradeIntraDay; 
         
         SET error_code = 0;
		 SET error_msg = 'Processamento OK!' ;
END   //
DELIMITER ;
       
Set @@GLOBAL.innodb_change_buffering=all;   
SET autocommit=0;

SELECT @@innodb_buffer_pool_size;

-- =========================================================================   
-- IMPORTANTE PARA O DESEMPENHO AS 2 CONFIGURAÇÕES ABAIXO DOS PARÂMETROS:
--  set @@GLOBAL.innodb_change_buffering=all e  SET autocommit=0; 
use b3;

Set @@GLOBAL.innodb_change_buffering=all;   
SET autocommit=0;

SET @date_param = '20200831';
SET @interval_param = 15;
set @time_previous = now();
CALL FIND_OPORTUNITY_INTRADAY (@date_param , @interval_param, @error_code, @error_msg);   -- IMPORTANTE VERIFIQUE LIMIT DE LINHAS RETORNADAS
select @error_code, @error_msg;
select timediff(now(),@time_previous);

-- AMBOS OS INSERTS PRECISAM SER RODADOS
INSERT INTO tb_oportunity_intraday_result
		SELECT *  FROM tb_oportunity_intraday
        where codigo_negociacao_papel NOT IN (select distinct codigo_negociacao_papel from  tb_oportunity_intraday_result
												where data_trade = @date_param)
                                                and data_trade = @date_param;
INSERT INTO tb_oportunity_intraday_result
   		SELECT  distinct t1.*  FROM tb_oportunity_intraday t1
             inner JOIN tb_oportunity_intraday_result t2
             ON t1.codigo_negociacao_papel = t2.codigo_negociacao_papel and t1.data_trade = t2.data_trade
	         where t1.ultimo_id not in (select ultimo_id from tb_oportunity_intraday_result t3
                                        where t3.ultimo_id = t1.ultimo_id);
               
/*               
		SELECT t1.*  FROM tb_oportunity_intraday t1, tb_oportunity_intraday_result t2
	         where t1.data_trade = t2.data_trade
             and t1.codigo_negociacao_papel = t2.codigo_negociacao_papel
             and t1.ultimo_id != t2.ultimo_id;
*/

				select   *
					from tb_intraday_trade
					where data_pregao = '20200903'
                    and codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$'
		            and id > 0 
                    and length(codigo_negociacao_papel) < 7
                    order by id, data_pregao, hora_negocio limit 1;
                    
                    
                    
				select    *
					from tb_intraday_trade
					where data_pregao = '20200902'
                    and codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$'
		            and id > 0 
                    and hora_negocio < time('10:10:04.233') + interval +10 minute
                    and length(codigo_negociacao_papel) < 7
                    order by id, data_pregao, hora_negocio ;                   
                    
select *  FROM tb_oportunity_intraday_control where last_date = '20200903';

SELECT * FROM tb_oportunity_intraday where data_trade = '20200903';

SELECT * FROM tb_oportunity_intraday_result where data_trade = '20200902';

SELECT * from tb_oportunity_intraday_result where data_trade = "20200831";
-- truncate tb_oportunity_intraday_result;
-- commit;

select @error_code, @error_msg;

select timediff(now(),@time_previous);
-- FIND_NEGOTIATED_STOCK_SYMBOL_BETWEEN_DATES
-- =========================================================================
select id, last_time into @id,  @Last_time from tb_oportunity_intraday_control
				where last_date = '20200714'; 
                
                
				select    hora_negocio
                        					from tb_intraday_trade
					where data_pregao = @ppdate
                    and codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$'
		            and id > @ppwid
                    and hora_negocio < time(@pptime) + interval +@ppinterval minute
                    and length(codigo_negociacao_papel) < 7
                    order by id, data_pregao, hora_negocio  ;              
                
 
SET @pptime = '09:59:59.999';
SET @ppwid  = 0;
SET @ppdate = '20200902'; 
SET @ppinterval = 10;
				select    id
                        , data_pregao
                        , codigo_negociacao_papel
                        , preco_negocio
                        , titulos_negociados
                        , hora_negocio
                        , id_negocio
					from tb_intraday_trade
					where data_pregao = @ppdate
                    and codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$'
		            and id > @ppwid
                    and hora_negocio < time(@pptime) + interval +@ppinterval minute
                    and length(codigo_negociacao_papel) < 7
                    order by id, data_pregao, hora_negocio
				;              

select * from tb_oportunity_intraday_control;
 -- DELETE from tb_oportunity_intraday_control ;

select *  from tb_oportunity_intraday;
 -- DELETE from tb_oportunity_intraday;
 
 
-- truncate tb_oportunity_intraday_result;
 
 INSERT INTO tb_oportunity_intraday_result SELECT * FROM tb_oportunity_intraday;
	
select * from tb_oportunity_intraday_result;
    
 
SELECT @@default_storage_engine;

select * from tb_oportunity_intraday;

SET @wstart_time = '17:12:14.000';
SET c = '20200714';
				select    id
                        , data_pregao
                        , codigo_negociacao_papel
                        , preco_negocio
                        , titulos_negociados
                        , hora_negocio
                        , id_negocio
					from tb_intraday_trade
					where data_pregao = 132334201
                    and codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$'
		            and id > 132334201 
                    and hora_negocio < time(@wstart_time) + interval +15 minute
                    and length(codigo_negociacao_papel) < 7
                    order by id, data_pregao, hora_negocio;
        
select *  from tb_intraday_trade where data_pregao = '20200902'  LIMIT 20000;

SET @wstart_time = '9:59:59.999';
				select    id
                        , data_pregao
                        , codigo_negociacao_papel
                        , preco_negocio
                        , titulos_negociados
                        , hora_negocio
                        , id_negocio
					from tb_intraday_trade
					where data_pregao between  '20200714' and '20200723'
                    and codigo_negociacao_papel like CONCAT('wlmm3','%')
		            and id > 0 
--                    and hora_negocio > time(@wstart_time) 
--                    order by id, data_pregao, hora_negocio
				;
                    
                    select 'ITUB4' regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}';
                    
select count(*) from tb_intraday_trade where data_pregao = '20200714';
-- ===========================================================================================================
use b3;
        -- DROP TABLE IF EXISTS tb_oportunity_intraday;
        CREATE TABLE tb_oportunity_intraday ( data_trade date not null,
                horario_primeiro_negocio time(3) not null,
                ultimo_horario_analisado time(3) not null,
				        codigo_negociacao_papel varchar(12) not null,
                preco_abertura decimal(13,3) not null,
                menor_preco    decimal(13,3) not null,
                hora_menor_preco time(3) not null,
                id_menor_preco int not null,
                maior_preco    decimal(13,3) not null,
                id_maior_preco int not null,
                hora_maior_preco time(3) not null,
                ultimo_preco   decimal(13,3) not null,
                ultimo_id      int not null,
			          titulos_negociados int not null,
                resultado_ate_agora decimal(13,3) not null,
                total_up int not null,    -- número de vezes que o preço subiu
                total_same int not null,  -- número de vezes que o preço nem subiu nem desceu
                total_down int not null   -- número de vezes que o preço desceu
        );
        
        CREATE unique INDEX tb_oportunity_intraday_ix ON tb_oportunity_intraday (codigo_negociacao_papel);
commit;

use b3;
        -- DROP TABLE IF EXISTS tb_oportunity_intraday_by_stock;
        CREATE TABLE tb_oportunity_intraday_by_stock ( data_trade date not null,
                horario_primeiro_negocio time(3) not null,
                ultimo_horario_analisado time(3) not null,
				        codigo_negociacao_papel varchar(12) not null,
                preco_abertura decimal(13,3) not null,
                menor_preco    decimal(13,3) not null,
                hora_menor_preco time(3) not null,
                id_menor_preco int not null, 
                maior_preco    decimal(13,3) not null,
                id_maior_preco int not null,
                hora_maior_preco time(3) not null, 
                ultimo_preco   decimal(13,3) not null,
                ultimo_id      int not null,
			          titulos_negociados int not null,
                resultado_ate_agora decimal(13,3) not null,
                total_up int not null,    -- número de vezes que o preço subiu
                total_same int not null,  -- número de vezes que o preço nem subiu nem desceu
                total_down int not null   -- número de vezes que o preço desceu
        );
        
        CREATE unique INDEX tb_oportunity_intraday_by_stock_ix ON tb_oportunity_intraday_by_stock (codigo_negociacao_papel);
-- ====================================================================================================================

use b3;
        -- DROP TABLE IF EXISTS tb_oportunity_intraday_result;
        CREATE TABLE tb_oportunity_intraday_result  ( data_trade date ,
                horario_primeiro_negocio time(3),
                ultimo_horario_analisado time(3),
				        codigo_negociacao_papel varchar(12),
                preco_abertura decimal(13,3) ,
                menor_preco    decimal(13,3) not null,
                hora_menor_preco time(3) not null,
                id_menor_preco int not null, 
                maior_preco    decimal(13,3) not null,
                id_maior_preco int not null,
                hora_maior_preco time(3) not null,
                ultimo_preco   decimal(13,3) , 
                ultimo_id      int,
			          titulos_negociados int ,
                resultado_ate_agora decimal(13,3),
                total_up int,    -- número de vezes que o preço subiu
                total_same int,  -- número de vezes que o preço nem subiu nem desceu
                total_down int   -- número de vezes que o preço desceu
        );        

CREATE unique INDEX oportunity_intraday_result_ix ON tb_oportunity_intraday_result (data_trade, codigo_negociacao_papel, ultimo_id);
CREATE INDEX oportunity_intraday_result_ix1 ON tb_oportunity_intraday_result (codigo_negociacao_papel);
CREATE INDEX oportunity_intraday_result_ix2 ON tb_oportunity_intraday_result (ultimo_id);

-- commit;
        
use b3;
--        DROP TABLE IF EXISTS tb_oportunity_intraday_control;
        CREATE TABLE tb_oportunity_intraday_control (
                id int,
                last_date date,
                last_time time(3),
				last_codigo_negociacao_papel varchar(12),
                last_id      int
        ); 
        
CREATE unique INDEX oportunity_intraday_control_ix ON tb_oportunity_intraday_control (last_date);


use b3;
        DROP TABLE IF EXISTS tb_oportunity_intraday_control_by_stock;
        CREATE TABLE tb_oportunity_intraday_control_by_stock (
                id int,
                last_date date,
                last_time time(3),
				codigo_negociacao_papel varchar(12),
                last_id      int
        ); 
        
CREATE unique INDEX oportunity_intraday_control_by_stock_ix ON tb_oportunity_intraday_control_by_stock (last_date, codigo_negociacao_papel);

commit;        
-- ================================================================================================================

USE b3;

DROP PROCEDURE IF EXISTS FIND_OPORTUNITY_DOWN;
/*
   Pesquisa oportunidades de bons resultados analisando dados históricos da ação conforme algoritmo a seguir:
   1. Definir uma data  de "base line" para se estabelecer possibilidade de bons resultados
   2. Caso as seguintes afirmações sejam verdadeiras então nesta data houve uma possibilidade de aumento do valor da ação
      de  modo a proporcionar ganhos imediatos:
      ?2.1. O valor da ação desta data é mais que dobro da média do valor deste o início da data base line?
      ?2.2. A total de negócios (transações)  é mais que  dobro da quantidade de negócio do pregão anterior?
      ?2.3. O volume de negócios (somatória dos valores das transaçõa) é mais que o volue de negócios do pregão anterior?
*/
DELIMITER //
CREATE PROCEDURE FIND_OPORTUNITY_DOWN (stock_code varchar(20)
                                  , start_date varchar(10)
                                  , price_percent_param tinyint
                                  , total_negocios_percent_param tinyint
                                  , total_negocios_param int)
	BEGIN
        -- working vars
		DECLARE finished INTEGER DEFAULT 0;

        -- into vars
		DECLARE wdata_cotacao varchar(10);
        DECLARE wcodigo_negociacao_papel varchar(12);
        DECLARE wpreco_abertura decimal(13,2) ;
	    DECLARE wpreco_fechamento decimal(13,2) ;
        DECLARE wtotal_negocios int;
	    DECLARE wtotal_titulos_negociados bigint;
        DECLARE wvolume_titulos_negociados decimal(18,2);
        
        -- algorith vars
        DECLARE wresultado_preco_fechamento decimal(13,2) DEFAULT 0;
   	    DECLARE wresultado_total_negocios int DEFAULT 0;
   	    DECLARE wpreco_fechamento_ant decimal(13,2) DEFAULT 0;
	    DECLARE wtotal_negocios_ant int DEFAULT 0;

--
		DECLARE curStockTrade
			CURSOR FOR 
				select  data_cotacao
					,codigo_negociacao_papel
  					,preco_abertura
					,preco_fechamento
					,total_negocios
                    ,volume_titulos_negociados
					from tb_cotacao
					where codigo_negociacao_papel = stock_code
					and data_cotacao between start_date and NOW();

		-- declare NOT FOUND handler
		DECLARE CONTINUE HANDLER 
			FOR NOT FOUND SET finished = 1; 
/*
        DROP TEMPORARY TABLE if exists tmp_result ;
        
        CREATE TEMPORARY TABLE tmp_result (	data_cotacao varchar(10),
				codigo_negociacao_papel varchar(12),
				preco_fechamento_ant decimal(13,2) ,
				preco_fechamento decimal(13,2) ,
                result_fechamento decimal (6,2),
                result_negocios   decimal (6,2),
			    total_negocios int
        );
*/          
        OPEN curStockTrade;   

        fetch_trading: REPEAT
           FETCH curStockTrade INTO   wdata_cotacao
					, wcodigo_negociacao_papel
   					, wpreco_abertura
					, wpreco_fechamento
					, wtotal_negocios
                    , wvolume_titulos_negociados;
                    
            IF finished = 1 THEN 
				LEAVE fetch_trading;
		    END IF;
  
           IF wtotal_negocios_ant > 0  -- nao é o primeiro fetch
              THEN
                 SET wresultado_preco_fechamento = (wpreco_fechamento/wpreco_fechamento_ant - 1) * 100;
               
                 SET wresultado_total_negocios = (wtotal_negocios/wtotal_negocios_ant -1) * 100; 
             
--                 if wresultado_preco_fechamento > price_percent_param AND (wresultado_total_negocios > total_negocios_percent_param or wtotal_negocios > total_negocios_param)  then 
                 if wresultado_preco_fechamento < price_percent_param AND (wtotal_negocios > total_negocios_param)  then 
                 insert into tmp_result values ( wdata_cotacao, wcodigo_negociacao_papel, wpreco_fechamento_ant, wpreco_fechamento, wresultado_preco_fechamento, wresultado_total_negocios, wtotal_negocios,wvolume_titulos_negociados);
                 end if;
           END IF;        
           
           set wpreco_fechamento_ant = wpreco_fechamento;
           set wtotal_negocios_ant = wtotal_negocios;
        
         UNTIL finished = 1 END REPEAT fetch_trading;
         
         CLOSE curStockTrade; 
         
 --       select * from tmp_result;
       
END   //
DELIMITER ;
-- ================================================================================================================
-- ================================================================================================================


CALL FIND_OPORTUNITIES ( '2020-07-', 10, 10, 200);      -- maiores altas

CALL FIND_OPORTUNITIES_DOWN('2020-07-31', -4, 2, 10000);  -- maiores baixas

CALL FIND_OPORTUNITY ('irbr3', '2020-01-28', 3, 2, 10000);  -- chamado por FIND_OPORTUNITIES ou FIND_OPORTUNITIES_DOWN



				select  data_cotacao
					,codigo_negociacao_papel
   					,preco_abertura
					,preco_fechamento
					,total_negocios
					from tb_cotacao 
					where codigo_negociacao_papel = 'A1MD34'
					and data_cotacao between '2020-07-20' and NOW();
                    
				select  distinct codigo_negociacao_papel
					from tb_cotacao 
					where total_negocios > 10000
					and data_cotacao between  '2020-07-01'  and now();  
                    
                    select  distinct codigo_negociacao_papel 
					from tb_cotacao
					where data_cotacao between '2020-07-20' and NOW()
                    limit 10;
                    

select  * from tb_intraday_trade 
		where data_pregao = '2020-07-14' 
        and hora_negocio > ''
        order by codigo_negociacao_papel, hora_negocio;

select  min(hora_negocio) from tb_intraday_trade where data_pregao = '2020-07-14' into @wstart_time;
select @wstart_time;

