-- DROP database IF EXISTS dbauth;
-- create database dbauth;
-- commit;

use dbauth;

DROP TABLE IF EXISTS tbrole_authority;
commit;

DROP TABLE IF EXISTS tbaccount_permission;
commit;

DROP TABLE IF EXISTS tbaccount;
commit;

DROP TABLE IF EXISTS tbrole;
commit;

DROP TABLE IF EXISTS tbauthority;
commit;

DROP TABLE IF EXISTS tbpermission;
commit;

create table tbaccount (
	user_login varchar (50) not null,
	user_full_name varchar (50) not null,
	password varchar (50) not null,
	enabled boolean not null,
	account_expired boolean not null,
	credentials_expired boolean not null,
	account_locked boolean not null
	) ;
    
    CREATE INDEX pk_tbaccount ON tbaccount (user_login);    
    
	ALTER TABLE tbaccount
	  ADD CONSTRAINT pk_tbaccount
		PRIMARY KEY (user_login); 

create table tbpermission (
	permission_id varchar (50) not null,
   	permission_description varchar (200) not null
);

CREATE INDEX pk_tbpermission ON tbpermission (permission_id);    

ALTER TABLE tbpermission
  ADD CONSTRAINT pk_tbpermission
	PRIMARY KEY (permission_id); 

create table tbrole (
	permission_id varchar (50) not null primary key,
    constraint fk_role_permission_id foreign key(permission_id) references tbpermission(permission_id)
);

create table tbauthority (
	permission_id varchar (50) not null primary key,
    constraint fk_authority_permission_id foreign key(permission_id) references tbpermission(permission_id)
);

/*
create table tbrole_authority (
	role_id varchar (50) not null,
	authority_id varchar (50) not null,
    constraint fk_role_authority_role_id foreign key(role_id) references tbrole(permission_id),
    constraint fk_role_authority_authority_id foreign key(authority_id) references tbauthority(permission_id)
);
*/

create table tbaccount_permission (
	account_id varchar (50) not null,
    permission_id varchar (50) not null,
    primary key (account_id , permission_id),
    constraint fk_account_permission_account_id foreign key(account_id) references tbaccount(user_login),
    constraint fk_account_permission_permission_id foreign key(permission_id) references tbpermission(permission_id)
    )
;

-- load data

insert into tbaccount (user_login, user_full_name, password, enabled, account_expired, credentials_expired, account_locked) 
	values ('user', 'full user', 'pass', true, false, false, false);
    
insert into tbaccount (user_login, user_full_name, password, enabled, account_expired, credentials_expired, account_locked) 
	values ('admin', 'full admin', 'pass', true, false, false, false);



insert into tbpermission ( permission_id, permission_description) 
		values ('ADMIN' , 'DESC ADMIN');
insert into tbauthority( permission_id) 
		values ('ADMIN' );

insert into tbpermission ( permission_id, permission_description) 
		values ('USER' , 'DESC USER');        
insert into tbauthority ( permission_id) 
		values ('USER' );
        
insert into tbpermission ( permission_id, permission_description) 
		values ('ROLE_ADMIN' , 'DESC ROLE_ADMIN');
insert into tbrole( permission_id) 
		values ('ROLE_ADMIN' );

insert into tbpermission ( permission_id, permission_description) 
		values ('ROLE_USER' , 'DESC ROLE_USER');        
insert into tbrole ( permission_id) 
		values ('ROLE_USER' );   
        

select * from tbaccount;    
        
select * from tbpermission;         

insert into tbaccount_permission (account_id, permission_id)
	values ('user', 'USER');
insert into tbaccount_permission (account_id, permission_id)
	values ('user', 'ROLE_USER');  
    
insert into tbaccount_permission (account_id, permission_id)
	values ('user', 'ROLE_ADMIN');    
    
insert into tbaccount_permission (account_id, permission_id)
	values ('admin', 'ADMIN');
insert into tbaccount_permission (account_id, permission_id)
	values ('admin', 'ROLE_ADMIN');     
    
select * from tbaccount_permission;      

/*
insert into tbrole_authority( role_id, authority_id)
			values ('ROLE_USER', 'USER');

select * from tbrole_authority;
*/