SET SESSION SQL_LOG_BIN=0;

set names utf8;
create database if not exists mysql_identity;
use mysql_identity;
create table if not exists `rep_log_info` (
  `server_id` bigint(20) unsigned NOT NULL,
  `log_file` varchar(20) NOT NULL,
  `log_pos` int(10) unsigned NOT NULL,
  `state` varchar(10) DEFAULT NULL,
  `log_time` datetime DEFAULT NULL,
  `ip` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- replication account
GRANT RELOAD, PROCESS, SUPER, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'slave'@'192.168.%.%' IDENTIFIED BY PASSWORD 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON mysql_identity.* TO 'slave'@'192.168.%.%';
GRANT RELOAD, PROCESS, SUPER, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'slave'@'10.100.%.%' IDENTIFIED BY PASSWORD 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON mysql_identity.* TO 'slave'@'10.100.%.%';

-- check_mysql.pl 
GRANT CREATE, PROCESS, SUPER, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mysqlha'@'127.0.0.1' IDENTIFIED BY PASSWORD 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP ON mysql_identity.* TO 'mysqlha'@'127.0.0.1';

flush privileges;

SET SESSION SQL_LOG_BIN=1;
