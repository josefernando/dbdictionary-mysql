
-- ESTA SINTÁXE É PARA BANCO DE DADOS MYSQL 

DROP database IF EXISTS dbbia;
create database dbbia;

use dbbia;

DROP table IF EXISTS tbconta_corrente;
create table tbconta_corrente (
	num_conta int not null primary key,
	nome_cliente varchar (100) not null
);

DROP table IF EXISTS tblancamento;
create table tblancamento (
	fk_num_conta int,
	tipo CHAR,
	valor decimal
 --       constraint fk_conta_corrente_lancamento foreign key(fk_num_conta) references tbconta_corrente(num_conta)
);

CREATE INDEX tipo_ix ON tblancamento (tipo);

-- TABELA DE TRADUÇÃO OU DECODIFICAÇÃO 
DROP table IF EXISTS tbcodigo;
create table tbcodigo (
	codigo char not null primary key,
	descricao varchar (50) 
);

insert into tblancamento(fk_num_conta, tipo, valor) values (1, 'D', 200);
insert into tblancamento(fk_num_conta, tipo, valor) values (1, 'D', 300);
insert into tblancamento(fk_num_conta, tipo, valor) values (2, 'C', 300);
insert into tblancamento(fk_num_conta, tipo, valor) values (2, 'D', -50);

insert into tblancamento(fk_num_conta) values (100);

insert into tbcodigo (codigo, descricao) values ('C', 'CRÉDITO');
insert into tbcodigo (codigo, descricao) values ('D', 'DÉBITO');
insert into tbcodigo (codigo, descricao) values ('E', 'ESTORNO');


select avg(valor) from tblancamento
	where fk_num_conta = 1;

-- insere linha na tabela 
insert into tbconta_corrente(num_conta, nome_cliente) values (3, 'MUÇURANA $%¨)');

select * from tbconta_corrente;

select * from tblancamento;

select * from tbcodigo;

select nome_cliente, valor from  tbconta_corrente, tblancamento
	where fk_num_conta = num_conta  --  entraria o comando inner join relacionamento
   ;

select  c.nome_cliente valor, t.descricao from tbconta_corrente c, tblancamento l, tbcodigo t
	where fk_num_conta = num_conta and 
          l.tipo = t.codigo
          and l.tipo = 'D';
	
select  c.nome_cliente valor, t.descricao from tbconta_corrente c, tblancamento l, tbcodigo t
	where fk_num_conta = num_conta and 
          l.tipo = t.codigo
          and l.tipo = 'D';

-- join #1
select  * from tbconta_corrente c, tblancamento l, tbcodigo t
	where fk_num_conta = num_conta and 
          l.tipo = t.codigo;

-- RESULTADO          
-- # num_conta, nome_cliente,           fk_num_conta, tipo, valor, codigo, descricao
-- '2',         'BIA NUMATA',            '2',         'C', '300',   'C',    'CRÉDITO'
-- '1',         'JOSÉ FERNANDO PEREIRA', '1',         'D', '200',   'D',    'DÉBITO'
-- '1',         'JOSÉ FERNANDO PEREIRA', '1',         'D', '300',   'D',    'DÉBITO'
-- '2',         'BIA NUMATA',            '2',         'D', '-50',   'D',    'DÉBITO'
    
-- join #2    
select  * from (( tbconta_corrente c -- , tbcodigo t
		          INNER JOIN tblancamento l ON c.num_conta=l.fk_num_conta)
                  INNER JOIN tbcodigo t on l.tipo = t.codigo);
                  
--  MESMO RESULTADO, SÓ QUE BEEEM MAIS COMPLICADO          
-- # num_conta, nome_cliente,           fk_num_conta, tipo, valor, codigo, descricao
-- '2',         'BIA NUMATA',            '2',         'C', '300',   'C',    'CRÉDITO'
-- '1',         'JOSÉ FERNANDO PEREIRA', '1',         'D', '200',   'D',    'DÉBITO'
-- '1',         'JOSÉ FERNANDO PEREIRA', '1',         'D', '300',   'D',    'DÉBITO'
-- '2',         'BIA NUMATA',            '2',         'D', '-50',   'D',    'DÉBITO'


	-- join #3    
	select  * from ( tbconta_corrente c -- , tbcodigo t
						LEFT JOIN tblancamento l ON c.num_conta=l.fk_num_conta);

	-- join #4 
	select  * from ( tbconta_corrente c -- , tbcodigo t
						right JOIN tblancamento l ON c.num_conta=l.fk_num_conta);
                        
	-- join #5
	select  * from ( tbconta_corrente c -- , tbcodigo t
						LEFT JOIN tblancamento l ON c.num_conta=l.fk_num_conta)
     union                   
	select  * from ( tbconta_corrente c -- , tbcodigo t
						right JOIN tblancamento l ON c.num_conta=l.fk_num_conta);                        
         
          
          