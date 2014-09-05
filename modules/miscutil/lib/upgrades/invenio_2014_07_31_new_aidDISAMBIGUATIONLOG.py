from invenio.dbquery import run_sql


def info():
    return """New bibauthorid table (aidDISAMBIGUATIONLOG) to contain info
    about running disambiguation tasks"""


def do_upgrade():
    """ New table aidDISAMBIGUATIONLOG  """
    run_sql("""CREATE TABLE IF NOT EXISTS `aidDISAMBIGUATIONLOG` (
                `taskid` BIGINT( 16 ) UNSIGNED NOT NULL ,
                `surname` VARCHAR( 255 ),
                `phase` VARCHAR( 255 ),
                `progress` FLOAT( 8 ),
                `args` longblob NOT NULL,
                `start_time` datetime,
                `end_time` datetime,
                `status` enum('SCHEDULED', 'RUNNING', 'SUCCEEDED', 'FAILED',
                'KILLED', 'MERGED') NOT NULL
                ) ENGINE=MyISAM""")

    run_sql("""CREATE TABLE IF NOT EXISTS `aidDISAMBIGUATIONSTATS` (
                `taskid` BIGINT( 16 ) UNSIGNED NOT NULL ,
                `stats`  longblob NOT NULL
                ) ENGINE=MyISAM""")