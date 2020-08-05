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
hora_negocio time,
id_negocio bigint,
tipo_sessao_pregao tinyint ,
data_pregao date, 

PRIMARY KEY (id)
)
ENGINE = MYISAM;  --  única maneira de criar non clustered

CREATE UNIQUE INDEX intraday_trade_ix0 ON tb_intraday_trade (data_referencia , hora_negocio, codigo_negociacao_papel, id_negocio);
commit;

-- ============================================== LOAD  INTRADAY TRADE  ==================================================
-- download page: http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/cotacoes/cotacoes/
use b3;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\TradeIntraday_20200714_1.txt'
IGNORE
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
    hora_negocio = @hora_negocio/1000
;
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
         
         select * from tmp_result;
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
         
         select * from tmp_result;
	END   //
DELIMITER ;                                  

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
USE b3;

DROP PROCEDURE IF EXISTS FIND_OPORTUNITY;
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
CREATE PROCEDURE FIND_OPORTUNITY (stock_code varchar(20)
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
                 if wresultado_preco_fechamento > price_percent_param AND (wtotal_negocios > total_negocios_param)  then 
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

CALL FIND_OPORTUNITIES ( '2020-08-04', 5, 10, 200);

CALL FIND_OPORTUNITIES_DOWN('2020-08-03', -5, 2, 1000);

CALL FIND_OPORTUNITY ('irbr3', '2020-01-28', 3, 2, 10000);



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
                    
