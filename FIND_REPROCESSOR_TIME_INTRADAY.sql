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
use b3