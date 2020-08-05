use b3;

select count(*) from pregao_b3;

-- SELECT FirstName, LastName, 
--       OrderCount = (SELECT COUNT(O.Id) FROM [Order] O WHERE O.CustomerId = C.Id)
--  FROM Customer C 

select min(T1.data_pregao), 
       @p1 := (select preco_fechamento from pregao_b3 where codigo_negociacao_papel = 'MGLU3'
                                and data_pregao = (select min(data_pregao) 
                                                   from pregao_b3 where codigo_negociacao_papel = 'MGLU3')) as p1,
       max(T2.data_pregao) ,
       @p2 := (select preco_fechamento from pregao_b3 where codigo_negociacao_papel = 'MGLU3'
                                and data_pregao = (select max(data_pregao) 
                                                   from pregao_b3 where codigo_negociacao_papel = 'MGLU3')) as p2  ,
                                                   (@p2/@p1 -1)* 100 as resultado
    from pregao_b3 T1, pregao_b3 T2 
	where T1.codigo_negociacao_papel = T2.codigo_negociacao_papel
    AND T1.codigo_negociacao_papel = 'MGLU3';
    
    -- -----------------------------------------------------------------------------------------
    SET @papel = 'itub4';  -- vale por session e não somente para o comando
    
    select @papel as papel,  @min_data := min(T1.data_pregao) as mindataa, 
       @p1 := (select preco_fechamento from pregao_b3 where codigo_negociacao_papel = @papel
                                and data_pregao = @min_data ) as p1,
                                @max_data := max(T2.data_pregao) as maxndataa,
       @p2 := (select preco_fechamento from pregao_b3 where codigo_negociacao_papel = @papel
                                and data_pregao = (select max(data_pregao) 
                                                   from pregao_b3 where codigo_negociacao_papel = @papel)) as p2  ,
                                                   (@p2/@p1 -1)* 100 as resultado
    from pregao_b3 T1, pregao_b3 T2 
	where T1.codigo_negociacao_papel = T2.codigo_negociacao_papel
    AND T1.codigo_negociacao_papel = @papel;
    
-- ======================================================================================================
use b3;
select codigo_negociacao_papel, data_pregao, preco_fechamento from pregao_b3 where codigo_negociacao_papel = 'bova11' and   data_pregao between '20200601' and '20200715';   
    
-- ======================================================================================================
    SET @start_date = '2020-07-21';
    SET @end_date   = '2020-07-22';
    -- data;título;?;valor;quantidade;horário;número da operação;?;data?
    -- 2020-07-10;VVAR3;0;17,690;3900;175957990;878390;1;2020-07-10
    -- 2020-07-10;VVAR3;0;17,690;3100;175958277;878400;1;2020-07-10
    select * from pregao_b3 where data_pregao = '2020-07-10' and codigo_negociacao_papel = 'vvar3';
    
    select T1.codigo_negociacao_papel as papel, T1.total_negocios AS negocios, T1.data_pregao as start_date, @preco_start_date := T1.preco_fechamento as 'preço inicial'
                                     , T2.data_pregao as end_date, @preco_end_date := T2.preco_fechamento as 'preço final'
                                     , ROUND((@preco_end_date/@preco_start_date - 1) * 100,4) as resultado
							     , ROUND((T2.preco_minimo/T1.preco_fechamento - 1) * 100,4) as min_result
			        			     , ROUND((T2.preco_maximo/T1.preco_fechamento - 1) * 100,4) as max_result

			from pregao_b3 T1, pregao_b3 T2
            where T1.codigo_negociacao_papel = T2.codigo_negociacao_papel
					and T1.data_pregao = @start_date
                    and T2.data_pregao = @end_date
--                    and (T1.total_negocios > 5000 or T2.total_negocios > 5000)
/*
                    and (T1.codigo_negociacao_papel = 'IRBR3'
                         or T1.codigo_negociacao_papel = 'BRKM5')
*/
/*
                         and ( T1.codigo_negociacao_papel = 'USIM5'
                         or T1.codigo_negociacao_papel = 'CSNA3'
                         or T1.codigo_negociacao_papel = 'VALE3'
                         or T1.codigo_negociacao_papel = 'GGBR4')
*/
/*
                      and ( T1.codigo_negociacao_papel = 'RENT3'
                         or T1.codigo_negociacao_papel = 'CVCB3'
                         or T1.codigo_negociacao_papel = 'GOLL4'
                         or T1.codigo_negociacao_papel = 'AZUL4')
*/
/*
                      and ( T1.codigo_negociacao_papel = 'BEEF3'
                         or T1.codigo_negociacao_papel = 'MRFG3'
                         or T1.codigo_negociacao_papel = 'BRFS3'
                         or T1.codigo_negociacao_papel = 'JBSS3')
*/
/*
                      and ( T1.codigo_negociacao_papel = 'VVAR3'
                         or T1.codigo_negociacao_papel = 'BTOW3'
                         or T1.codigo_negociacao_papel = 'LAME4'
                         or T1.codigo_negociacao_papel = 'MGLU3')
*/
/*
                      and ( T1.codigo_negociacao_papel = 'CRFB3'
                         or T1.codigo_negociacao_papel = 'PCAR3')
*/

                      and ( T1.codigo_negociacao_papel = 'VALE3'
                         or T1.codigo_negociacao_papel = 'BOVA11'
                         or T1.codigo_negociacao_papel = 'ITUB4'
                         or T1.codigo_negociacao_papel = 'BBDC4'
                         or T1.codigo_negociacao_papel = 'PETR4'
                         or T1.codigo_negociacao_papel = 'PETR3'
                         or T1.codigo_negociacao_papel = 'B3SA3'
                         or T1.codigo_negociacao_papel = 'ITSA4'
                         or T1.codigo_negociacao_papel = 'LREN3'
                         or T1.codigo_negociacao_papel = 'SUZB3'
                         or T1.codigo_negociacao_papel = 'UGPA3'
                         or T1.codigo_negociacao_papel = 'JBSS3'
                         or T1.codigo_negociacao_papel = 'BBDC3'
                         or T1.codigo_negociacao_papel = 'RAIL3'
                        or T1.codigo_negociacao_papel = 'VIVT4'
                         or T1.codigo_negociacao_papel = 'BRFS3'
                         or T1.codigo_negociacao_papel = 'RENT3'   
                         or T1.codigo_negociacao_papel = 'BBSE3'
                         or T1.codigo_negociacao_papel = 'SANB11'
                         or T1.codigo_negociacao_papel = 'KROT3'
                         or T1.codigo_negociacao_papel = 'EQTL3' 
                         or T1.codigo_negociacao_papel = 'ABEV3')
 
                    order by resultado desc ;
                                
-- ===================================== PREÇO MÍNIMO  --------------------------------------------------------------------

select codigo_negociacao_papel, data_pregao, preco_fechamento
	from pregao_b3
    where preco_fechamento in (select min(preco_fechamento) from pregao_b3 where codigo_negociacao_papel = 'AZUL4')
    and codigo_negociacao_papel = 'AZUL4';

-- ==============================================================================================================


-- ===================================== LOAD --------------------------------------------------------------------
use b3;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\TradeInformationConsolidatedFile_20200727_1.csv'
-- IGNORE
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
    , preco_ref = REPLACE(@preco_ref, ',', '.')
    , preco_ref = IFNULL(@preco_ref,0)
;
SET id = IF(MID(@sldate, 6, 2) = '02', NULL, 1), sldate = @sldate
-- select *    from pregao_b3 where data_pregao = '20200715' and codigo_negociacao_papel = 'AFSQ20'  and international_securities_id_num ='BRBMEFAFS0J3' ;
-- select *    from pregao_b3 where data_pregao = '20200715' and codigo_negociacao_papel = 'AFSV20'  and international_securities_id_num ='BRBMEFAFS0L9' ;