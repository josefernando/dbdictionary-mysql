DROP database IF EXISTS db_demo_user;
create database db_demo_user;

commit;

use db_demo_user;
create table demo_user (
	id int not null primary key,
    name varchar (50) not null
);

DROP database IF EXISTS db_demo_book;
create database db_demo_book;
commit;

use db_demo_book;

create table demo_book (
	id int not null primary key,
    name varchar (50) not null
);






