from invenio.dbquery import run_sql


def info():
    return """New bibauthorid table (aidDISAMBIGUATIONLOG) to contain info
    about running disambiguation tasks"""


def do_upgrade():
    """ New table aidDISAMBIGUATIONLOG  """
    run_sql("""CREATE TABLE IF NOT EXISTS `aidDISAMBIGUATIONLOG` (
  `taskid` BIGINT( 16 ) UNSIGNED NOT NULL ,
  `phase` VARCHAR( 256 ) NOT NULL,
  `progress` FLOAT( 8 ) UNSIGNED NOT NULL,
  `args' longblob,
  `start_time` datetime NOT NULL,
  `end_time` datetime NOT NULL,
  `status` enum('RUNNING, SUCEEDED, FAILED') NOT NULL,
) ENGINE=MyISAM""")