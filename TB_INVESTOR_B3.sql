use b3;
-- ====================================================================================================
drop table if exists tb_operation;
drop table if exists tb_investor;
drop table if exists tb_corretora;
-- =========================================================================================================

create table tb_investor (
	email varchar(200),                   
    name varchar(200),
    created_on timestamp
);

ALTER TABLE tb_investor
  ADD CONSTRAINT PK_investor
    PRIMARY KEY (email);   
    
 --   delete from tb_investor;
    
insert into tb_investor(email, name, created_on) 
	values ('jose.ze.fernando@gmail.com', 'JOSE ZE', current_timestamp());

select * from tb_investor;
-- ===================================================
create table tb_corretora (
	cnpj dec (14),
    name varchar(100)
);
 
 ALTER TABLE tb_corretora
  ADD CONSTRAINT PK_corretora
    PRIMARY KEY (cnpj); 
    
-- ===================================================   
create table tb_operation (
    fk_investor_id   varchar(200),
    fk_paper_code varchar(50),
    fk_corretora_id  dec(14),
	date           timestamp,
    type           varchar(1),     -- c(compra) v(venda)
    quantity        int,
    value_un        dec (13,2)
);

ALTER TABLE tb_operation
  ADD CONSTRAINT PK_operation
    PRIMARY KEY (fk_investor_id, fk_paper_code, date, type);  
    
ALTER TABLE tb_operation ADD CONSTRAINT FK_operation_investor FOREIGN KEY (fk_investor_id)
REFERENCES tb_investor (email);
    
ALTER TABLE tb_operation ADD CONSTRAINT FK_operation_investor FOREIGN KEY (fk_corretora_id)
REFERENCES tb_corretora (cnpj);

/*
insert into tb_operation (fk_investor_id, fk_paper_code, date, type, quantity, value_un)
	values ('jose.ze.fernando@gmail.com', 'IRBR3', current_timestamp(), 'C', 100, 13.44);
*/
-- select * from tb_operation;
-- ============================================================
