CREATE DATABASE IF NOT EXISTS `libkv`;
USE `libkv`;
CREATE TABLE IF NOT EXISTS `libkv` (
	`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
	`field0` VARCHAR(127) NOT NULL,
	`field1` VARCHAR(127) NOT NULL,
	`field2` VARCHAR(127) NOT NULL,
	`field3` VARCHAR(127) NOT NULL,
	`field4` VARCHAR(127) NOT NULL,
	`field5` VARCHAR(127) NOT NULL,
	`field6` VARCHAR(127) NOT NULL,
	`field7` VARCHAR(127) NOT NULL,
	`lock_session` VARCHAR(64) NOT NULL DEFAULT '',
	`last_index` BIGINT UNSIGNED NOT NULL,
	`value` LONGTEXT NOT NULL,
	`create_at` DATETIME NOT NULL,
	`update_at` DATETIME NOT NULL,
	PRIMARY KEY (`id`),
	UNIQUE KEY  `by_field` (`field0`,`field1`,`field2`,
		`field3`,`field4`,`field5`,`field6`,`field7`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
