use b3;

-- DROP PROCEDURE IF EXISTS GET_NEXT_TIME_CONTROL_BY_STOCK;

DELIMITER //
CREATE PROCEDURE GET_NEXT_TIME_CONTROL_BY_STOCK ( IN pstart_date varchar(10),
                                                  IN pinterval time(3),
                                                  OUT pnext_start_time varchar(12),
                                                  OUT pnext_end_time varchar(12))
proc_00: BEGIN

DECLARE finished INTEGER DEFAULT 0;
DECLARE daily_start_time varchar (12) DEFAULT '10:00:00.000';

select max(next_start_time) into pnext_start_time  from tb_oportunity_intraday_control_by_stock where last_date = pstart_date limit 1;

if pnext_start_time IS NULL then
   SET pnext_start_time = daily_start_time;
end if;

SET pnext_end_time = time(pnext_start_time) +interval +pinterval minute;

END //
DELIMITER ;
-- ===========================================================================================================
use b3;
-- DROP PROCEDURE IF EXISTS GET_NEXT_TIME_CONTROL_BY_STOCK_HISTORIC;
DELIMITER //
CREATE PROCEDURE GET_NEXT_TIME_CONTROL_BY_STOCK_HISTORIC ( IN pstart_date varchar(10),
                                                  IN pinterval time(3),
                                                  OUT pnext_start_time varchar(12),
                                                  OUT pnext_end_time varchar(12))
proc_00: BEGIN

DECLARE finished INTEGER DEFAULT 0;
DECLARE daily_start_time varchar (12) DEFAULT '10:00:00.000';

select max(next_start_time) into pnext_start_time  from tb_oportunity_intraday_control_by_stock_historic where last_date = pstart_date limit 1;

if pnext_start_time IS NULL then
   SET pnext_start_time = daily_start_time;
end if;

SET pnext_end_time = time(pnext_start_time) +interval +pinterval minute;

END //
DELIMITER ;
-- ===========================================================================================================

SET @s = '20200730';
SET @i = 15;
SET @r1 = ' ';
SET @r2 = ' ';

CALL GET_NEXT_TIME_CONTROL_BY_STOCK(@s , @i, @r1, @r2);

SELECT @r1, @r2;

-- ===========================================================================================================
use b3;

DROP PROCEDURE IF EXISTS POSICIONA_CURSOR_INTRADAY_BY_STOCK;

DELIMITER //
CREATE PROCEDURE POSICIONA_CURSOR_INTRADAY_BY_STOCK (IN    pStock_code varchar(12),
												 INOUT pStart_date varchar(10),
                                                 OUT   pRestart_time time(3),
                                                 OUT   pId int,
                                                 OUT   error_code int,
                                                 OUT   error_msg  varchar(255))
proc_00: BEGIN
    
	-- Error handling 
	DECLARE sql_state CHAR(5) DEFAULT '00000';
	DECLARE sql_msg_text TEXT;
	DECLARE sql_mysql_error_number int;   
    
    DECLARE nrows int;    
      
    DECLARE wlast_time time(3);
    DECLARE wid int;      
        
    DECLARE EXIT HANDLER
		FOR SQLEXCEPTION   -- RETURNED_SQLSTATE not starting with '00', '01' or '02'
			BEGIN
			  GET DIAGNOSTICS CONDITION 1
				  sql_state = RETURNED_SQLSTATE
                , sql_msg_text = MESSAGE_TEXT
                , sql_mysql_error_number = MYSQL_ERRNO;
                
                SET error_code = sql_mysql_error_number;
                SET error_msg  = CONCAT( sql_state, ' - ' , sql_msg_text);
			END; 
    
    -- find último id processado
   SELECT id, last_time into pid,  pRestart_time from tb_oportunity_intraday_control_by_stock
		   where last_date = pStart_date and codigo_negociacao_papel = pStock_code;
       
   SET error_code = 0;
   SET error_msg  = CONCAT(' Processamento OK');    
   
	GET DIAGNOSTICS nrows = ROW_COUNT;
	IF nrows = 0 then
		select id, hora_negocio into pId,  pRestart_time
			from tb_intraday_trade
			where data_pregao = pStart_date
			and codigo_negociacao_papel = pStock_code
			order by id, data_pregao, hora_negocio limit 1;
			
			GET DIAGNOSTICS nrows = ROW_COUNT;
			IF nrows = 0 THEN
                SET pRestart_time = null;
                SET pId = null;
				SET error_code = 1;
				SET error_msg  = CONCAT(' Não existe negociações para esta data: ', pStart_date);
				leave proc_00;
			END IF;
	END IF;    

END //
DELIMITER ;

SET @start_date = 20200902;

CALL POSICIONA_CURSOR_INTRADAY_BY_STOCK ('IRBR3', @start_date , @restart_time, @id, @error_code, @error_msg);

select @restart_time, @id, @error_code, @error_msg;

-- =========================================================================================================
use b3;

DROP PROCEDURE IF EXISTS FIND_REPROCESSOR_TIME_INTRADAY;

DELIMITER //
CREATE PROCEDURE FIND_REPROCESSOR_TIME_INTRADAY (INOUT pStart_date varchar(10)
												, OUT pStart_time varchar(8)
												, OUT pStock varchar(12)
                                                , OUT pId int ) 
proc_00: BEGIN
	-- handle vars
    DECLARE finished int DEFAULT 0;
    

 
    
    -- select into 
    DECLARE wData_pregao date;
    DECLARE wHora_negocio time;
    DECLARE wCodigo_negociacao_papel varchar(12);
    DECLARE wId_negocio int;
    
    -- 
    DECLARE wLast_date date;
    DECLARE wLast_time time;
    DECLARE wLast_stock varchar(12);
    DECLARE wLast_id int;
    
    DECLARE wFound boolean DEFAULT false;
    
   	DECLARE curRestart
			CURSOR FOR 
			select data_pregao, hora_negocio , codigo_negociacao_papel, id_negocio
				from tb_intraday_trade
				where data_pregao = pStart_date
				and codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$'
				and hora_negocio between  time(wLast_time) and time(wLast_time) + interval +1 second
				and length(codigo_negociacao_papel) < 7
				order by data_pregao, hora_negocio ;

    DECLARE CONTINUE HANDLER
		FOR NOT FOUND SET finished = 1;
        
      DECLARE CONTINUE HANDLER
		FOR SQLEXCEPTION
			BEGIN
			  GET DIAGNOSTICS CONDITION 1
				pId = RETURNED_SQLSTATE, pStock = MESSAGE_TEXT;
--                SET pId = code;
--                SET pStock = msg;
			END;        
        
     CALL FIND_ORNEXT_BUSINESS_DAY (pStart_date, @rStart_date); -- encontra próxima data útil em caso de sab, dom e feriados
     
     SET pStart_date = @rStart_date;

	 select * into wLast_date, wLast_time, wLast_stock, wLast_id from tb_oportunity_intraday_control
 					where last_date = pStart_date;        
            
    OPEN curRestart ;   
    
    SET @xtotal = 0;
    
    IF  finished = 1 THEN   -- PROCESSO "SERÁ INICIADO A PARTIRA DAS 10 HORAS DA MANHÃ
		select finished = 0;
		select data_pregao, hora_negocio , codigo_negociacao_papel, id_negocio into 
			   wData_pregao, wHora_negocio , wCodigo_negociacao_papel, wId_negocio
			from tb_intraday_trade
			where data_pregao = pStart_date
			and codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$'
			and hora_negocio between  time(0) and time(0) + interval +11 hour
			and length(codigo_negociacao_papel) < 7
			order by data_pregao, hora_negocio LIMIT 1; 
            
        if  finished = 1 then  -- não existe registro para esta data. Ação : carregar o banco de dados com esta data
            SET pStart_date = 999;
            SET pStart_time = null;
            SET pStock = null;
            SET pId = null;
            LEAVE proc_00;
         else
            SET pStart_date = wData_pregao;
            SET pStart_time = wHora_negocio;
            SET pStock = null;
            SET pId = null;
            LEAVE proc_00;         
        end if;
    ELSE              -- restart the process
    repeat_01: REPEAT
		FETCH curRestart INTO   wData_pregao, wHora_negocio , wCodigo_negociacao_papel, wId_negocio;
        SET @xtotal = @xtotal + 1;
		IF  finished = 1 then  -- sem movimento após o último horário processado
			SET pStart_date = wData_pregao;
			SET pStart_time = wLast_time;
			SET pStock = null;
			SET pId = 9;
			LEAVE repeat_01;
		 ELSE
            if  wFound = true  then
				SET pStart_date = wData_pregao;
				SET pStart_time = wHora_negocio;
				SET pStock = wCodigo_negociacao_papel;
				SET pId = wId_negocio;
				LEAVE repeat_01;
             end if;  
             if wCodigo_negociacao_papel = wLast_stock and wId_negocio = wLast_id  then
                SET @xtotal = 9999;
			    SET wFound = true ; 	
             end if;
		END IF;
        UNTIL  false = true END REPEAT repeat_01;
    END IF;
END //
DELIMITER ;

-- SET @xtotal = 777;
SET @pStart_date = '20200804';
CALL FIND_REPROCESSOR_TIME_INTRADAY (  @pStart_date
												, @pStart_time
												, @pStock
                                                , @pId ) ;
                                                
 select @pStart_date, @pStart_time, @pStock, @pId, @xtotal;      
 
select * from tb_oportunity_intraday_control;

	 select *  from tb_oportunity_intraday_control
 					where last_date = '20200804'; 

		select data_pregao, hora_negocio , codigo_negociacao_papel, id_negocio 
			from tb_intraday_trade
			where data_pregao = '20200714'
			and codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$'
			and hora_negocio between  time(171240) and time(171240) + interval +1 second
			and length(codigo_negociacao_papel) < 7
			order by data_pregao, hora_negocio ;
            
            
		select *  from tb_oportunity_intraday_control
			where last_date = '20200714'; 
        
        select * into @last_date, @last_time, @last_stock, @last_id from tb_oportunity_intraday_control;
        
       select @last_date, @last_time, @last_stock, @last_id;        
        
		select data_pregao, hora_negocio , codigo_negociacao_papel, id_negocio 
			from tb_intraday_trade
			where data_pregao = 20200714
			and codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$'
			and hora_negocio between  time(100600) and time(100600) + interval +1 second
			and length(codigo_negociacao_papel) < 7
			order by data_pregao, hora_negocio LIMIT 1;  
            
		select data_pregao, hora_negocio , codigo_negociacao_papel, id_negocio 
			from tb_intraday_trade
			where data_pregao = '20200824'
			and codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$'
			and hora_negocio between  time(0) and time(0) + interval +11 hour
			and length(codigo_negociacao_papel) < 7
			order by data_pregao, hora_negocio LIMIT 1; 
        
        select * into @last_date, @last_time, @last_stock, @last_id from tb_oportunity_intraday_control;
        
        select @last_date, @last_time, @last_stock, @last_id;
        
        select * from tb_oportunity_intraday_control;
        
        delete from tb_oportunity_intraday_control;
        
        insert  into  tb_oportunity_intraday_control values(
                       20200714, 101313, 'ARZZ3', 2590);

-- ====================================================================================================================
 USE b3;

 DROP PROCEDURE IF EXISTS FIND_OPORTUNITY_INTRADAY_BY_STOCK;

DELIMITER //
CREATE PROCEDURE FIND_OPORTUNITY_INTRADAY_BY_STOCK (stock_code varchar(12), start_date varchar(10), pinterval int, OUT error_code int, OUT error_msg varchar(255))
proc_01:	BEGIN
        -- working vars
		DECLARE finished INTEGER DEFAULT 0;
    DECLARE dup_key INTEGER DEFAULT 0;
    
    DECLARE first_interval_processing INTEGER DEFAULT 0;

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
        
--        DECLARE wlast_date date;
--        DECLARE wlast_time time(3);
        
		DECLARE wnext_start_time time(3);
		
		DECLARE wlast_codigo_negociacao_papel varchar(12);
		DECLARE wlast_id_trade int;
        
	  DECLARE wid int;
	  DECLARE wlast_trade_id int;
        
	  DECLARE woportunity_data_trade date;
	  DECLARE woportunity_horario_primeiro_negocio time(3);
	  DECLARE woportunity_ultimo_horario_analisado time(3);
	  DECLARE woportunity_codigo_negociacao_papel varchar(12);
	  DECLARE woportunity_preco_abertura decimal(13,3);
    DECLARE woportunity_primeiro_preco_intervalo decimal(13,3);
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
 	  DECLARE woportunity_resultado_intervalo decimal(13,3);
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
					from tb_intraday_trade_daily
					where codigo_negociacao_papel =  stock_code
						and data_pregao = start_date
						and id_negocio > wlast_trade_id
						and hora_negocio < time(wnext_start_time)
					order by id, data_pregao, hora_negocio
				;

	  -- declare NOT FOUND handler
	  DECLARE CONTINUE HANDLER
				FOR NOT FOUND SET finished = 1;
  /*      
    DECLARE CONTINUE HANDLER FOR 1062   -- Error number: 1062; Symbol: ER_DUP_KEY; SQLSTATE: 23000
					BEGIN
          SET dup_key = 1;
					END;
     */       
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
-- Configuração para a melhoria de desempenho      
  Set @@GLOBAL.innodb_change_buffering=all;   
	SET autocommit=0;    

 	select id, next_start_time, last_time, last_trade_id 
        into wid, wnext_start_time, wultimo_horario_analisado, wlast_trade_id 
        from tb_oportunity_intraday_control_by_stock
				where last_date = start_date and codigo_negociacao_papel = stock_code;
 
	IF finished = 1 then  -- primeiro processamento do dia para esse código de ação
	   SET finished = 0;
	   SET wid = 0;
     SET wnext_start_time = '10:00:00.000';   -- considera às 10 horas da manhão o início do pregão
     SET wultimo_horario_analisado = '10:00:00.000'; 
     SET wlast_trade_id = 0;
     insert into tb_oportunity_intraday_control_by_stock values (wid, wnext_start_time , start_date, wultimo_horario_analisado, stock_code , 0);

     /*
     if dup_key = 1 then 
        SET dup_key = 0 ;
				SET error_msg = CONCAT('Registro de controle já existe para a data: ', start_date);
				SET error_code = 1022;
				leave proc_01;
     end if;
     */

      GET DIAGNOSTICS nrows = ROW_COUNT;
		  if nrows = 0 then
				SET error_msg = 'Zero linhas inseridas tabela de controle';
				SET error_code = 8000;
				leave proc_01;
      end if;
	END IF;
	
  SET wnext_start_time =  time(wnext_start_time) + interval +pinterval minute ;
  
	UPDATE tb_oportunity_intraday_control_by_stock
		SET next_start_time =  time(wnext_start_time)
		WHERE last_date = start_date and codigo_negociacao_papel = stock_code;

 GET DIAGNOSTICS nrows = ROW_COUNT;
 IF nrows = 0 then
		SET error_msg = 'Zero linhas atualizadas tabela de controle - next_start_time';
		SET error_code = 8001;
		leave proc_01;
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

        IF finished = 1 THEN  
          SET finished = 0;
					IF COUNT_TOTAL = 0 THEN
             select id into wid from tb_oportunity_intraday_control_by_stock 
										 where last_date = start_date and codigo_negociacao_papel = stock_code;
                        
             IF finished = 1 then
              	SET error_code = 9;
								SET error_msg = 'Erro na leitura inicial do registro de controle! ' ;
                LEAVE proc_01;
             END IF ;

						 SET error_code = 100;
						 SET error_msg = CONCAT('Sem transações para a data: ', start_date , ' e horário: ' , wultimo_horario_analisado , ' - ' , wnext_start_time ) ;                
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
                woportunity_primeiro_preco_intervalo,
								woportunity_menor_preco,    
								woportunity_hora_menor_preco,
								woportunity_id_menor_preco, 
								woportunity_maior_preco,    
								woportunity_id_maior_preco, 
								woportunity_hora_maior_preco,              
                woportunity_ultimo_preco,
                woportunity_ultimo_id,
			          woportunity_titulos_negociados,
                woportunity_resultado_intervalo,
                woportunity_UpOrDownUntilNow,
                woportunity_total_up,    
                woportunity_total_same,    
                woportunity_total_down 
                FROM tb_oportunity_intraday_by_stock
                WHERE codigo_negociacao_papel = stock_code AND   data_trade = start_date;
                
		 GET DIAGNOSTICS nrows = ROW_COUNT;
		 IF nrows > 1 then
				SET error_msg = CONCAT('Mais de 1 linha no processamento de resutados para a ação: ', stock_code);
				SET error_code = 8010;
				leave proc_01;
		 END IF;                

		 IF finished = 1 THEN       -- primeira transação da  ação para a data
			 SET finished = 0;
	     SET first_interval_processing = 1;
			 INSERT INTO tb_oportunity_intraday_by_stock VALUES (
						wdata_pregao
					, whora_negocio     -- primeiro negocio  
					, whora_negocio     -- ultimo negocio
					, wcodigo_negociacao_papel
					, wpreco_negocio    -- preço do primeiro negócio do dia
 					, wpreco_negocio    -- primeiro do intervalo
					, wpreco_negocio    -- menor preco
					, wid_negocio       -- id menor preco
					, whora_negocio     -- hora menor preço
					, wpreco_negocio    -- maior preco
					, wid_negocio       -- id maior preco
					, whora_negocio     -- hora maior preço              
					, wpreco_negocio    -- preço do último horário analisado
					, wid_negocio       
					, wtitulos_negociados  -- quantidade de títulos de negócio
					, 0           -- resultado do intervalo processado
					, 0           -- resultado desde o início do pregao
					, 0           -- total_up
					, 0           -- = same
					, 0);         -- total_down
       
			  GET DIAGNOSTICS nrows = ROW_COUNT;
				if nrows = 0 then
					 SET error_msg = '0 linhas inseridas na tabela de controle';
					 SET error_code = 8001;
					 leave proc_01;
				END IF;
    ELSE                                          -- transações já exixstente para o dia
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
    
				SET wUpOrDown = (wpreco_negocio / woportunity_ultimo_preco - 1) * 100;
        SET wUpOrDownUntilNow = (wpreco_negocio / woportunity_preco_abertura - 1) * 100;

				IF wUpOrDown > 0 THEN
						SET woportunity_total_up =  woportunity_total_up + 1;
				ELSEIF wUpOrDown < 0 THEN
						SET woportunity_total_down =  woportunity_total_down + 1;
				ELSEIF wUpOrDown = 0 THEN
            SET woportunity_total_same = woportunity_total_same + 1;
				END IF;
        
        IF first_interval_processing = 0 then
					SET first_interval_processing = 1;
					SET woportunity_primeiro_preco_intervalo = wpreco_negocio;
        END IF;
        
        SET woportunity_resultado_intervalo = (wpreco_negocio / woportunity_primeiro_preco_intervalo - 1) * 100;
                
				UPDATE tb_oportunity_intraday_by_stock
					SET ultimo_horario_analisado = whora_negocio,
							menor_preco = woportunity_menor_preco,
							hora_menor_preco = woportunity_hora_menor_preco,
							id_menor_preco = woportunity_id_menor_preco,
							maior_preco = woportunity_maior_preco,
							id_maior_preco = woportunity_id_maior_preco,
							hora_maior_preco = woportunity_hora_maior_preco,
							ultimo_preco = wpreco_negocio,
              primeiro_preco_intervalo = woportunity_primeiro_preco_intervalo,
							titulos_negociados = woportunity_titulos_negociados + wtitulos_negociados,
							ultimo_id = wid_negocio,
							resultado_intervalo = woportunity_resultado_intervalo,
              resultado_ate_agora = wUpOrDownUntilNow,
							total_up  = woportunity_total_up, 
							total_same = woportunity_total_same,
							total_down = woportunity_total_down
                    WHERE codigo_negociacao_papel = wcodigo_negociacao_papel
                    AND data_trade = start_date;

			   if nrows = 0 then
					 SET error_msg = 'Zero linhas atualizadas tabela de oportunidades';
					 SET error_code = 8002;
					 leave proc_01;
			   end if;
		  END IF;
              
		  UPDATE tb_oportunity_intraday_control_by_stock
				SET id = wid,
					  last_time = whora_negocio,
					  last_trade_id = wid_negocio
					WHERE last_date = start_date  
                and codigo_negociacao_papel = stock_code;
              
			   if nrows = 0 then
					 SET error_msg = 'Zero linhas atualizadas tabela de controle';
					 SET error_code = 8003;
					 leave proc_01;
			   end if;

		 SET COUNT = COUNT + 1;
     SET COUNT_TOTAL = COUNT_TOTAL + 1;   
     
		 IF COUNT % 1000 = 0 THEN
				COMMIT;
--            SET COUNT_TOTAL = COUNT_TOTAL + COUNT;
				SET COUNT = 0;
		 END IF;

   UNTIL finished = 1 END REPEAT fetch_trading;
 
  --  inserção em duas fases
  --  Fase 1: insere ações do primeiro intervalo de processamento. Por exemplo: das 10:00 às 10:15
  
	INSERT INTO tb_oportunity_intraday_result_by_stock
			SELECT *  FROM tb_oportunity_intraday_by_stock
					WHERE codigo_negociacao_papel NOT IN ( select distinct codigo_negociacao_papel 
                                                 from  tb_oportunity_intraday_result_by_stock
													                       where data_trade = start_date )
					AND data_trade = start_date;
          
  --  Fase 2: insere açoes do segundo intervalo em diante. Assim essa tabela terá o resultado do processamento
  --          de todos os intervalos processados
  /*
	INSERT INTO tb_oportunity_intraday_result_by_stock
		SELECT  distinct t1.*  FROM tb_oportunity_intraday_by_stock t1 INNER JOIN tb_oportunity_intraday_result_by_stock t2
			ON t1.codigo_negociacao_papel = t2.codigo_negociacao_papel AND t1.data_trade = t2.data_trade
			WHERE t1.ultimo_id NOT IN (select ultimo_id from tb_oportunity_intraday_result_by_stock t3
																		where t3.ultimo_id = t1.ultimo_id );
  */

  INSERT INTO tb_oportunity_intraday_result_by_stock
		select * from tb_oportunity_intraday_by_stock t1
                                      where t1.codigo_negociacao_papel = stock_code
                                      and t1.data_trade = start_date 
                                      and t1.ultimo_horario_analisado >  
 ( select max(t2.ultimo_horario_analisado) from tb_oportunity_intraday_result_by_stock t2
                                      where t2.codigo_negociacao_papel = stock_code
                                      and t2.data_trade = start_date );

   COMMIT;
   CLOSE curTradeIntraDay; 
         
   SET error_code = 0;
	 SET error_msg = CONCAT('Processamento OK! Número de Transações efetuadas: ', COUNT_TOTAL) ;
END   //
DELIMITER ;
-- ===========================================================================================================
 USE b3;

DROP PROCEDURE IF EXISTS FIND_OPORTUNITY_INTRADAY_BY_STOCK_HISTORIC;

DELIMITER //
CREATE PROCEDURE FIND_OPORTUNITY_INTRADAY_BY_STOCK_HISTORIC (stock_code varchar(12), start_date varchar(10), pinterval int, OUT error_code int, OUT error_msg varchar(255))
proc_01:	BEGIN
-- Working vars
		DECLARE finished INTEGER DEFAULT 0;
    DECLARE dup_key INTEGER DEFAULT 0;
    
    DECLARE first_interval_processing INTEGER DEFAULT 0;

		DECLARE COUNT integer DEFAULT 0;
    DECLARE COUNT_TOTAL integer DEFAULT 0;
-- 
    DECLARE wcount int;
    DECLARE wprevious_trade_day varchar(10);

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
        
		DECLARE wnext_start_time time(3);
		
		DECLARE wlast_codigo_negociacao_papel varchar(12);
		DECLARE wlast_id_trade int;
        
	  DECLARE wid int;
	  DECLARE wlast_trade_id int;
        
	  DECLARE woportunity_data_trade date;
	  DECLARE woportunity_horario_primeiro_negocio time(3);
	  DECLARE woportunity_ultimo_horario_analisado time(3);
	  DECLARE woportunity_codigo_negociacao_papel varchar(12);
    DECLARE	woportunity_preco_fechamento_previous_trade decimal(13,3);
    DECLARE	woportunity_hora_fechamento_previous_trade time(3);
	  DECLARE woportunity_preco_abertura decimal(13,3);
    DECLARE woportunity_primeiro_preco_intervalo decimal(13,3);
	  DECLARE woportunity_menor_preco    decimal(13,3);
	  DECLARE woportunity_hora_menor_preco time(3);
	  DECLARE woportunity_id_menor_preco int;
	  DECLARE woportunity_maior_preco    decimal(13,3);
	  DECLARE woportunity_id_maior_preco int;
	  DECLARE woportunity_hora_maior_preco time(3);
	  DECLARE woportunity_ultimo_preco decimal(13,3);
	  DECLARE woportunity_ultimo_id    int ;   
	  DECLARE woportunity_titulos_negociados int;
	  DECLARE woportunity_resultado_intervalo decimal(13,3);
	  DECLARE woportunity_UpOrDownUntilNow decimal(13,3);
    DECLARE woportunity_ResultadoPreviousTrade decimal(13,3);
	  DECLARE woportunity_total_up int;    
	  DECLARE woportunity_total_same int;    
	  DECLARE woportunity_total_down int;
-- Algorithm vars
		DECLARE wresultado_preco_negocio decimal(13,3) DEFAULT 0;
		DECLARE wUpOrDown decimal(13,3);
		DECLARE wUpOrDownUntilNow decimal(13,3);
    DECLARE wResultadoPreviousTrade decimal(13,3);
		DECLARE wtotal_up int;
		DECLARE wtotal_same int;
		DECLARE wtotal_down int;
-- Error handling 
		DECLARE sql_state CHAR(5) DEFAULT '00000';
		DECLARE sql_msg_text TEXT;
		DECLARE sql_mysql_error_number int;
		DECLARE sql_exception boolean DEFAULT false;
		DECLARE nrows INT;
		DECLARE result TEXT;
-- Cursor definition
		DECLARE curTradeIntraDay
			CURSOR FOR 
				select    id
								, data_pregao
								, codigo_negociacao_papel
								, preco_negocio
								, titulos_negociados
								, hora_negocio
								, id_negocio
					from tb_intraday_trade_daily_historic
					where codigo_negociacao_papel =  stock_code
						and data_pregao = start_date
						and id_negocio > wlast_trade_id
						and hora_negocio < time(wnext_start_time)
					order by id, data_pregao, hora_negocio
				;

	  -- declare NOT FOUND handler
	  DECLARE CONTINUE HANDLER
				FOR NOT FOUND SET finished = 1;
  /*      
    DECLARE CONTINUE HANDLER FOR 1062   -- Error number: 1062; Symbol: ER_DUP_KEY; SQLSTATE: 23000
					BEGIN
          SET dup_key = 1;
					END;
     */       
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
-- Configuração para a melhoria de desempenho      
  Set @@GLOBAL.innodb_change_buffering=all;   
	SET autocommit=0;    

 	select id, next_start_time, last_time, last_trade_id 
        into wid, wnext_start_time, wultimo_horario_analisado, wlast_trade_id 
        from tb_oportunity_intraday_control_by_stock_historic
				where last_date = start_date and codigo_negociacao_papel = stock_code;
 
	IF finished = 1 then  -- primeiro processamento do dia para esse código de ação
	   SET finished = 0;
	   SET wid = 0;
     SET wnext_start_time = '10:00:00.000';   -- considera às 10 horas da manhão o início do pregão
     SET wultimo_horario_analisado = '10:00:00.000'; 
     SET wlast_trade_id = 0;
     insert into tb_oportunity_intraday_control_by_stock_historic values (wid, wnext_start_time , start_date, wultimo_horario_analisado, stock_code , 0);

      GET DIAGNOSTICS nrows = ROW_COUNT;
		  if nrows = 0 then
				SET error_msg = 'Zero linhas inseridas tabela de controle';
				SET error_code = 8000;
				leave proc_01;
      end if;
	END IF;
	
  SET wnext_start_time =  time(wnext_start_time) + interval +pinterval minute ;
  
	UPDATE tb_oportunity_intraday_control_by_stock_historic
		SET next_start_time =  time(wnext_start_time)
		WHERE last_date = start_date and codigo_negociacao_papel = stock_code;

 GET DIAGNOSTICS nrows = ROW_COUNT;
 IF nrows = 0 then
		SET error_msg = 'Zero linhas atualizadas tabela de controle - next_start_time';
		SET error_code = 8001;
		leave proc_01;
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

        IF finished = 1 THEN  
          SET finished = 0;
					IF COUNT_TOTAL = 0 THEN
             select id into wid from tb_oportunity_intraday_control_by_stock_historic 
										 where last_date = start_date and codigo_negociacao_papel = stock_code;
                        
             IF finished = 1 then
              	SET error_code = 9;
								SET error_msg = 'Erro na leitura inicial do registro de controle! ' ;
                LEAVE proc_01;
             END IF ;

						 SET error_code = 100;
						 SET error_msg = CONCAT('Sem transações para a data: ', start_date , ' e horário: ' , wultimo_horario_analisado , ' - ' , wnext_start_time ) ;                
						 LEAVE proc_01;
					ELSE 
							LEAVE fetch_trading;
					END IF;   
			END IF;

			SELECT * INTO woportunity_data_trade,
                woportunity_horario_primeiro_negocio,
                woportunity_ultimo_horario_analisado,
				        woportunity_codigo_negociacao_papel,
                woportunity_preco_fechamento_previous_trade,
                woportunity_hora_fechamento_previous_trade,
                woportunity_preco_abertura,
                woportunity_primeiro_preco_intervalo,
								woportunity_menor_preco,    
								woportunity_hora_menor_preco,
								woportunity_id_menor_preco, 
								woportunity_maior_preco,    
								woportunity_id_maior_preco, 
								woportunity_hora_maior_preco,              
                woportunity_ultimo_preco,
                woportunity_ultimo_id,
			          woportunity_titulos_negociados,
                woportunity_resultado_intervalo,
                woportunity_UpOrDownUntilNow,
                woportunity_ResultadoPreviousTrade,
                woportunity_total_up,    
                woportunity_total_same,    
                woportunity_total_down 
                FROM tb_oportunity_intraday_by_stock_historic
                WHERE codigo_negociacao_papel = stock_code AND   data_trade = start_date;
                
		 GET DIAGNOSTICS nrows = ROW_COUNT;
		 IF nrows > 1 then
				SET error_msg = CONCAT('Mais de 1 linha no processamento de resutados para a ação: ', stock_code);
				SET error_code = 8010;
				leave proc_01;
		 END IF;                

		 IF finished = 1 THEN       -- primeira transação da  ação para a data
			 SET finished = 0;
	     SET first_interval_processing = 1;

       SET wprevious_trade_day = start_date - INTERVAL 1 DAY;
			 CALL FIND_ORPREVIOUS_BUSINESS_DAY (wprevious_trade_day);
       
		   SELECT ultimo_horario_analisado, ultimo_preco INTO woportunity_hora_fechamento_previous_trade, woportunity_preco_fechamento_previous_trade 
          FROM  tb_oportunity_intraday_result_by_stock_historic
					WHERE codigo_negociacao_papel = stock_code
          AND data_trade = wprevious_trade_day
					AND ultimo_horario_analisado IN (select max(ultimo_horario_analisado) 
																						FROM tb_oportunity_intraday_result_by_stock_historic
																							WHERE codigo_negociacao_papel = stock_code 
                                              and data_trade = wprevious_trade_day ); 
 
-- Se não tiver ultimo preco do dia anteior então marca o horário com '00:00:00.000' 
       IF finished = 1 THEN
       			 SET finished = 0;
             SET woportunity_preco_fechamento_previous_trade = wpreco_negocio;
             SET woportunity_hora_fechamento_previous_trade = '00:00:00.000';
       END IF;
       SET wResultadoPreviousTrade = (wpreco_negocio / woportunity_preco_fechamento_previous_trade - 1) * 100;
			 INSERT INTO tb_oportunity_intraday_by_stock_historic VALUES (
						wdata_pregao
					, whora_negocio     -- primeiro negocio  
					, whora_negocio     -- ultimo negocio
					, wcodigo_negociacao_papel
          , woportunity_preco_fechamento_previous_trade  -- ultimo preço do pregão anterior
          , woportunity_hora_fechamento_previous_trade   -- horário do preço do pregão anterior
					, wpreco_negocio    -- preço do primeiro negócio do dia
 					, wpreco_negocio    -- primeiro do intervalo
					, wpreco_negocio    -- menor preco
					, wid_negocio       -- id menor preco
					, whora_negocio     -- hora menor preço
					, wpreco_negocio    -- maior preco
					, wid_negocio       -- id maior preco
					, whora_negocio     -- hora maior preço              
					, wpreco_negocio    -- preço do último horário analisado
					, wid_negocio       
					, wtitulos_negociados  -- quantidade de títulos de negócio
					, 0           -- resultado do intervalo processado
					, 0           -- resultado desde o início do pregao
          , wResultadoPreviousTrade          -- resultado desde do último preço do pregao anterior
					, 0           -- total_up
					, 0           -- = same
					, 0);         -- total_down
       
			  GET DIAGNOSTICS nrows = ROW_COUNT;
				if nrows = 0 then
					 SET error_msg = '0 linhas inseridas na tabela de controle';
					 SET error_code = 8001;
					 leave proc_01;
				END IF;
    ELSE                                          -- transações já existente para o dia
        
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
    
				SET wUpOrDown = (wpreco_negocio / woportunity_ultimo_preco - 1) * 100;
        SET wUpOrDownUntilNow = (wpreco_negocio / woportunity_preco_abertura - 1) * 100;
--        SET wResultadoPreviousTrade = (wpreco_negocio / woportunity_preco_fechamento_previous_trade - 1) * 100;

				IF wUpOrDown > 0 THEN
						SET woportunity_total_up =  woportunity_total_up + 1;
				ELSEIF wUpOrDown < 0 THEN
						SET woportunity_total_down =  woportunity_total_down + 1;
				ELSEIF wUpOrDown = 0 THEN
            SET woportunity_total_same = woportunity_total_same + 1;
				END IF;
        
        IF first_interval_processing = 0 then
					SET first_interval_processing = 1;
					SET woportunity_primeiro_preco_intervalo = wpreco_negocio;
        END IF;
        
        SET woportunity_resultado_intervalo = (wpreco_negocio / woportunity_primeiro_preco_intervalo - 1) * 100;
        SET wResultadoPreviousTrade = (wpreco_negocio / woportunity_preco_fechamento_previous_trade - 1) * 100;
                
				UPDATE tb_oportunity_intraday_by_stock_historic
					SET ultimo_horario_analisado = whora_negocio,
							menor_preco = woportunity_menor_preco,
							hora_menor_preco = woportunity_hora_menor_preco,
							id_menor_preco = woportunity_id_menor_preco,
							maior_preco = woportunity_maior_preco,
							id_maior_preco = woportunity_id_maior_preco,
							hora_maior_preco = woportunity_hora_maior_preco,
							ultimo_preco = wpreco_negocio,
              primeiro_preco_intervalo = woportunity_primeiro_preco_intervalo,
							titulos_negociados = woportunity_titulos_negociados + wtitulos_negociados,
							ultimo_id = wid_negocio,
							resultado_intervalo = woportunity_resultado_intervalo,
              resultado_ate_agora = wUpOrDownUntilNow,
              resultado_previous_trade = wResultadoPreviousTrade,
							total_up  = woportunity_total_up, 
							total_same = woportunity_total_same,
							total_down = woportunity_total_down
                    WHERE codigo_negociacao_papel = wcodigo_negociacao_papel
                    AND data_trade = start_date;

			   if nrows = 0 then
					 SET error_msg = 'Zero linhas atualizadas tabela de oportunidades';
					 SET error_code = 8002;
					 leave proc_01;
			   end if;
		  END IF;
              
		  UPDATE tb_oportunity_intraday_control_by_stock_historic
				SET id = wid,
					  last_time = whora_negocio,
					  last_trade_id = wid_negocio
					WHERE last_date = start_date  
          AND codigo_negociacao_papel = stock_code;
              
			   if nrows = 0 then
					 SET error_msg = 'Zero linhas atualizadas tabela de controle';
					 SET error_code = 8003;
					 leave proc_01;
			   end if;

		 SET COUNT = COUNT + 1;
     SET COUNT_TOTAL = COUNT_TOTAL + 1;   
     
		 IF COUNT % 1000 = 0 THEN
				COMMIT;
--            SET COUNT_TOTAL = COUNT_TOTAL + COUNT;
				SET COUNT = 0;
		 END IF;

   UNTIL finished = 1 END REPEAT fetch_trading;
 
  --  inserção em duas fases
  --  Fase 1: insere ações do primeiro intervalo de processamento. Por exemplo: das 10:00 às 10:15
  
	INSERT INTO tb_oportunity_intraday_result_by_stock_historic
			SELECT *  FROM tb_oportunity_intraday_by_stock_historic
					WHERE codigo_negociacao_papel NOT IN ( SELECT DISTINCT codigo_negociacao_papel 
                                                 FROM  tb_oportunity_intraday_result_by_stock_historic
													                       WHERE data_trade = start_date )
					AND data_trade = start_date;
          
--  Fase 2: insere açoes do segundo intervalo em diante. Assim essa tabela terá o resultado do processamento
--          de todos os intervalos processados

  INSERT INTO tb_oportunity_intraday_result_by_stock_historic
		select * from tb_oportunity_intraday_by_stock_historic t1
                                      where t1.codigo_negociacao_papel = stock_code
                                      and t1.data_trade = start_date 
                                      and t1.ultimo_horario_analisado >  
 ( select max(t2.ultimo_horario_analisado) from tb_oportunity_intraday_result_by_stock_historic t2
                                      where t2.codigo_negociacao_papel = stock_code
                                      and t2.data_trade = start_date );

   COMMIT;
   CLOSE curTradeIntraDay; 
         
   SET error_code = 0;
	 SET error_msg = CONCAT('Processamento OK! Número de Transações efetuadas: ', COUNT_TOTAL) ;
END   //
DELIMITER ;

-- ===========================================================================================================
SET @ec = 0;
SET @em = ' ';
CALL FIND_OPORTUNITY_INTRADAY_BY_STOCK ('IRBR3', '20200923', 15, @ec, @em);


select @ec , @em;
-- ===========================================================================================================

select * from tb_oportunity_intraday_result_by_stock t1
                                      where codigo_negociacao_papel = 'BTOW3'
                                      and data_trade = '20200923' 
                                      and ultimo_horario_analisado >  -- '17:44:58.992'
 ( select max(ultimo_horario_analisado) from tb_oportunity_intraday_result_by_stock t3
                                      where codigo_negociacao_papel = 'BTOW3'
                                      and data_trade = '20200923' );
	--																	where t3.ultimo_id = t1.ultimo_id ;

-- ====================================== EXECUTION ===============================================================
use b3;

Set @@GLOBAL.innodb_change_buffering=all;   
SET autocommit=0;

SET @stock_code = 'TIET3';
SET @date_param = '20200914';
SET @interval_param = 30;
set @time_previous = now();
CALL FIND_OPORTUNITY_INTRADAY_BY_STOCK ( @stock_code, @date_param , @interval_param, @error_code, @error_msg);   -- IMPORTANTE VERIFIQUE LIMIT DE LINHAS RETORNADAS
select @error_code, @error_msg;
select timediff(now(),@time_previous);

-- AMBOS OS INSERTS PRECISAM SER RODADOS
INSERT INTO tb_oportunity_intraday_result_by_stock
		SELECT *  FROM tb_oportunity_intraday_by_stock
        where codigo_negociacao_papel NOT IN (select distinct codigo_negociacao_papel from  tb_oportunity_intraday_result_by_stock
												where data_trade = @date_param)
                                                and data_trade = @date_param;
INSERT INTO tb_oportunity_intraday_result_by_stock
   		SELECT  distinct t1.*  FROM tb_oportunity_intraday_by_stock t1
             inner JOIN tb_oportunity_intraday_result_by_stock t2
             ON t1.codigo_negociacao_papel = t2.codigo_negociacao_papel and t1.data_trade = t2.data_trade
	         where t1.ultimo_id not in (select ultimo_id from tb_oportunity_intraday_result_by_stock t3
                                        where t3.ultimo_id = t1.ultimo_id);
-- ==================================================================================================================

				select    id
								, data_pregao
								, codigo_negociacao_papel
								, preco_negocio
								, titulos_negociados
								, hora_negocio
								, id_negocio
					from tb_intraday_trade_daily
					where data_pregao = '20200915'
						and codigo_negociacao_papel =  'ADHM3'
						and id_negocio > 0
						and hora_negocio < time('10:20:00.000') + interval +20 minute
					order by id, data_pregao, hora_negocio
				;

-- ===========================================================================================================
-- truncate tb_oportunity_intraday_control_by_stock;
select id  from tb_oportunity_intraday_control_by_stock 
			 where last_date = @date_param
					and codigo_negociacao_papel = @stock_code;
          
select *  from   tb_oportunity_intraday_control_by_stock;        

select * from tb_intraday_trade_daily where codigo_negociacao_papel = 'GOAU3F';

select * from tb_intraday_trade_daily where data_pregao = '20200914' limit 1;

truncate tb_intraday_trade_daily;
select * from tb_intraday_trade_daily;
-- ==================================================================================================================
use b3;

-- truncate tb_oportunity_intraday_control_by_stock;
select distinct last_date  from tb_oportunity_intraday_control_by_stock where last_date = '20200921';

 -- truncate tb_oportunity_intraday_control_by_stock_historic;
 -- truncate tb_oportunity_intraday_control_by_stock;
select  * from tb_oportunity_intraday_control_by_stock where last_date = '20200921';

select * from tb_oportunity_intraday_control_by_stock_historic where last_date = '20200803';
-- delete from tb_oportunity_intraday_control_by_stock_historic where last_date = '20200731' or last_date = '20200803';

-- truncate tb_oportunity_intraday_by_stock;
select data_trade, ultimo_horario_analisado, codigo_negociacao_papel, resultado_ate_agora, titulos_negociados, total_up, total_same, total_down from    tb_oportunity_intraday_by_stock where  data_trade = '20200923';

-- truncate tb_oportunity_intraday_by_stock_historic;
select * from tb_oportunity_intraday_by_stock_historic where  data_trade = '20200803'; -- where  codigo_negociacao_papel = 'MXRF11';
-- delete from tb_oportunity_intraday_by_stock_historic where data_trade = '20200731' or data_trade = '20200803';

--  truncate tb_oportunity_intraday_result_by_stock_historic;
--  truncate tb_oportunity_intraday_result_by_stock;
select * from tb_oportunity_intraday_result_by_stock_historic where  data_trade = '20200731' and codigo_negociacao_papel = 'HBOR3F';
-- delete from tb_oportunity_intraday_result_by_stock_historic where data_trade = '20200731' or data_trade = '20200803';

select  preco_fechamento_previous_trade from tb_oportunity_intraday_result_by_stock_historic
				where codigo_negociacao_papel = 'irbr3' 
        and ultimo_horario_analisado in (select max(ultimo_horario_analisado) 
																						from tb_oportunity_intraday_result_by_stock_historic
																							where codigo_negociacao_papel = 'irbr3' );
        

-- select data_trade, codigo_negociacao_papel, resultado_ate_agora, titulos_negociados, total_up, total_same, total_down from tb_oportunity_intraday_result_by_stock where data_trade = '20200923' ;

commit;

       SET @stock_code = 'AALR3';
	     SET @start_date = '20200803';
       SET @previous_trade_day = @start_date - INTERVAL 1 DAY;

			 CALL FIND_ORPREVIOUS_BUSINESS_DAY (@previous_trade_day);
              select @previous_trade_day;
       
		   SELECT ultimo_horario_analisado, ultimo_preco
          FROM  tb_oportunity_intraday_result_by_stock_historic
					WHERE codigo_negociacao_papel = @stock_code
          AND data_trade = @previous_trade_day
					AND ultimo_horario_analisado IN (select max(ultimo_horario_analisado) 
																						FROM tb_oportunity_intraday_result_by_stock_historic
																							WHERE codigo_negociacao_papel = @stock_code 
                                              and data_trade = @previous_trade_day ); 
