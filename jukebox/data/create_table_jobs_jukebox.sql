CREATE TABLE `jobs_jukebox` (
  `job_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` text,
  `locked` tinyint(1) NOT NULL DEFAULT '0',
  `status` text,
  `params` text,
  `log` text,
  `date_created` datetime DEFAULT CURRENT_TIMESTAMP,
  `date_modified` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `date_done` datetime DEFAULT NULL,
  PRIMARY KEY (`job_id`)
) ENGINE=InnoDB AUTO_INCREMENT=27 DEFAULT CHARSET=latin1;