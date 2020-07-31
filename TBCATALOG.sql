use DBRECATALOG;

DROP TABLE IF EXISTS TBCATALOG;

CREATE TABLE TBCATALOG (
    ID VARCHAR(255) NOT NULL,
    DT_CREATED DATETIME(6) NOT NULL,
    NAME VARCHAR(255) NOT NULL,
    DESCRIPTION VARCHAR(255) NOT NULL,
    PARENT_ID VARCHAR(255)
);

CREATE INDEX IX_PK_TBCATALOG ON TBCATALOG (ID);    
    
ALTER TABLE TBCATALOG
  ADD CONSTRAINT PK_TBCATALOG
    PRIMARY KEY (ID);     
    
ALTER TABLE TBCATALOG ADD CONSTRAINT FK1_TBCATALOG  FOREIGN KEY (PARENT_ID)
	REFERENCES TBCATALOG (ID);    
    
    
    
    select *  from tbcatalog;
    select *  from tbcatalog_item;
    
    select *  from tbproject;
    select i.* from tbcatalog_item i, tbproject p where i.id = p.id; 
    
   delet from tbproject where ID = "FEIRA";
   delet  from tbcatalog_item where ID = "FEIRA";

    
    delete from tbcatalog  where  LEN(RTRIM(LTRIM(id))) = 0;
    
        delet from tbcatalog ;

    
    
    SELECT * FROM time_zone_transition_type;