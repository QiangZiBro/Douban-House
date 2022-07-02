CREATE DATABASE IF NOT EXISTS Douban;
USE Douban;
CREATE TABLE IF NOT EXISTS houses (
	id int primary key,
	lasttime varchar(100),
	author varchar(100),
	title varchar(255),
	url varchar(255)
) ENGINE=INNODB;
