use dbsecurity;

DROP TABLE IF EXISTS tbcustom_granted_authority;
commit;

DROP TABLE IF EXISTS tbcustom_user_details;
commit;

create table tbcustom_user_details(
	username varchar (50) not null primary key,
	password varchar (50) not null,
	enabled boolean not null,
    accountNonExpired boolean not null,
	credentialsNonExpired boolean not null,
	accountNonLocked boolean not null
);

create table tbcustom_granted_authority (
	username varchar (50) not null,
	authority varchar (50) not null,
	constraint fk_custom_granted_authority foreign key(username) references tbcustom_user_details(username)
);
create unique index ix_auth_custom_username on tbcustom_granted_authority (username, authority);

-- para o curso sprin security
-- https://www.youtube.com/watch?v=LKvrFltAgCQ&list=PLqq-6Pq4lTTYTEooakHchTGglSvkZAjnE&index=7
-- https://docs.spring.io/spring-security/site/docs/3.0.x/apidocs/org/springframework/security/core/userdetails/User.html

insert into tbcustom_user_details(username, password, enabled, accountNonExpired, credentialsNonExpired, accountNonLocked) 
values ('user', 'pass', true, true, true, true);

insert into tbcustom_user_details(username, password, enabled, accountNonExpired, credentialsNonExpired, accountNonLocked) 
values ('admin', 'pass', true, true, true, true);

insert into tbcustom_granted_authority(username, authority) values ('user', 'ROLE_USER'); 
insert into tbcustom_granted_authority(username, authority) values ('admin', 'ROLE_ADMIN'); 
insert into tbcustom_granted_authority(username, authority) values ('admin', 'ROLE_USER'); 

use dbsecurity;
select * from tbcustom_user_details;
select * from tbcustom_granted_authority ;

delet from tbcustom_granted_authority where authority = 'ROLE_USER';