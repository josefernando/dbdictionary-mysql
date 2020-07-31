DROP database IF EXISTS dbsecurity;
create database dbsecurity;

use dbsecurity;

DROP TABLE IF EXISTS authorities;
commit;

DROP TABLE IF EXISTS users;
commit;

create table users(
	username varchar (50) not null primary key,
	password varchar (50) not null,
	enabled boolean not null
);

create table authorities (
	username varchar (50) not null,
	authority varchar (50) not null,
	constraint fk_authorities_users foreign key(username) references users(username)
);
create unique index ix_auth_username on authorities (username,authority);

-- para o curso sprin security
-- https://www.youtube.com/watch?v=LKvrFltAgCQ&list=PLqq-6Pq4lTTYTEooakHchTGglSvkZAjnE&index=7
insert into users(username, password, enabled) values ('user', 'pass', true);
insert into users(username, password, enabled) values ('admin', 'pass', true);

insert into authorities(username, authority) values ('user', 'ROLE_USER'); 
insert into authorities(username, authority) values ('admin', 'ROLE_ADMIN'); 

select * from users;
select * from authorities;
