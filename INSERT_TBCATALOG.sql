use DBRECATALOG;
insert into TBCATALOG (id, dt_created, name, description) -- parent_id) 
	VALUES("SEGUROS", now(6) ,"SEGUROS", "PROJETO R1PAB001");   -- now(6): inlcui microseconds with 6 digits
    
insert into TBCATALOG (id, dt_created, name, description, parent_id) -- parent_id) 
	VALUES("SEGUROS.REPOSITORY", now(6) ,"R1PAB001", "PROJETO R1PAB001", "SEGUROS"); -- now(6): inlcui microseconds with 6 digits

    
SELECT * FROM    TBCATALOG; 
    

