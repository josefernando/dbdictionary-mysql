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

    DECLARE CONTINUE HANDLER
		FOR NOT FOUND SET finished = 1;
        
        
     CALL FIND_ORNEXT_BUSINESS_DAY (pStart_date, @rStart_date);
     
     SET pStart_date = @rStart_date;
     
    select * into wLast_date, wLast_time, wLast_stock, wLast_id from tb_oportunity_intraday_control;        
            
    IF  finished = 1 THEN   -- PROCESSO "NAO RETOMADO" DO INÍCIO
		SET finished = 0;
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
		select data_pregao, hora_negocio , codigo_negociacao_papel, id_negocio into
               wData_pregao, wHora_negocio , wCodigo_negociacao_papel, wId_negocio
			from tb_intraday_trade
			where data_pregao = pStart_date
			and codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$'
			and hora_negocio between  time(wLast_time) and time(wLast_time) + interval +1 second
			and length(codigo_negociacao_papel) < 7
			order by data_pregao, hora_negocio LIMIT 1;
            
        IF  finished = 1 then  -- sem movimento até o horário para as ações alvos: com finais 3, 4, 5 e 66
            SET pStart_date = null;
            SET pStart_time = null;
            SET pStock = null;
            SET pId = 999;
            LEAVE proc_00;
         ELSE
            SET pStart_date = wData_pregao;
            SET pStart_time = wHora_negocio;
            SET pStock = wCodigo_negociacao_papel;
            SET pId = wId_negocio;
            LEAVE proc_00;         
        END IF;            
    END IF;
END //
DELIMITER ;



SET @pStart_date = '20200714';
CALL FIND_REPROCESSOR_TIME_INTRADAY (  @pStart_date
												, @pStart_time
												, @pStock
                                                , @pId ) ;
                                                
 select @pStart_date, @pStart_time, @pStock, @pId;                                         

	select data_pregao, hora_negocio , codigo_negociacao_papel, id_negocio
		from tb_intraday_trade
		where data_pregao = '20200714'
		and codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$'
		and hora_negocio between  time(0) and time(0) + interval +11 hour
		and length(codigo_negociacao_papel) < 7
		order by data_pregao, hora_negocio LIMIT 1;
        
        select * into @last_date, @last_time, @last_stock, @last_id from tb_oportunity_intraday_control;
        
       select @last_date, @last_time, @last_stock, @last_id;        
        
	select data_pregao, hora_negocio , codigo_negociacao_papel, id_negocio
		from tb_intraday_trade
		where data_pregao = '20200714'
		and codigo_negociacao_papel regexp '^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}$|^[qtweryuiopasdfghjklzxcvbnm]{4}[3456]{1}F$'
		and hora_negocio between  time(@last_time) and time(@last_time) + interval +1 second
		and length(codigo_negociacao_papel) < 7
		order by data_pregao, hora_negocio LIMIT 1;        
        
        select * into @last_date, @last_time, @last_stock, @last_id from tb_oportunity_intraday_control;
        
        select @last_date, @last_time, @last_stock, @last_id;
        
        select * from tb_oportunity_intraday_control;
        
        delete from tb_oportunity_intraday_control;
        
        insert  into  tb_oportunity_intraday_control values(
                       20200714, 101313, 'ARZZ3', 2590);
