
use b3;

-- http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/boletim-diario/arquivos-para-download/

drop table IF EXISTS pregao_b3;

create table  pregao_b3 (
data_pregao date ,
codigo_negociacao_papel varchar(50) ,
international_securities_id_num varchar(50) ,
segmento varchar(50) ,
preco_minimo decimal(13,2) ,
preco_maximo decimal(13,2) ,
preco_medio decimal(13,2) ,
preco_fechamento decimal(13,2) ,
preco_cotacao_ajuste decimal(13,2),
preco_ref decimal(13,2),
total_negocios int ,
total_titulos_negociados bigint 
);

ALTER TABLE pregao_b3
  ADD CONSTRAINT PK_pregao_b3
    PRIMARY KEY (data_pregao, codigo_negociacao_papel, international_securities_id_num);     

CREATE INDEX codigo_negociacao_papel_b3_ix ON pregao_b3 (codigo_negociacao_papel);
CREATE INDEX data_pregao_b3_ix ON pregao_b3 (data_pregao);

