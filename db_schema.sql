CREATE TABLE `records` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `timestamp` int(11) NOT NULL,
  `station_id` smallint(6) NOT NULL,
  `bikes_cnt` tinyint(4) NOT NULL,
  `free_spases_cnt` tinyint(4) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `timestamp` (`timestamp`,`station_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 