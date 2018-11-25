from stupendous_cow.db.main import Database
from stupendous_cow.importers.generic_ss.configuration \
    import ConfigurationFileParser
from stupendous_cow.importers.generic_ss.directors import Director
from stupendous_cow.importers.spreadsheets import Workbook
from util.cmd_line_args import SimpleCmdLineArgs, parse_args_and_exec
import logging
import sys

LOGGING_LEVEL_MAP = { 'TRACE' : logging.NOTSET, 'DEBUG' : logging.DEBUG,
                      'INFO' : logging.INFO, 'WARN' : logging.WARNING,
                      'ERROR' : logging.ERROR, 'OFF' : logging.CRITICAL }

class CmdLineArgs(SimpleCmdLineArgs):
    def __init__(self):
        SimpleCmdLineArgs.__init__(self,
                                   (('--log-file', 'Log file', False,
                                     'logging_filename'),
                                    ('--log-level', 'Logging level', False,
                                     tuple(LOGGING_LEVEL_MAP)),
                                    ('--config', 'Configuration file', True,
                                     'configuration_filename'),
                                    ('--db', 'Database file', True,
                                     'database_filename'),
                                    ('', 'Workbook name', True,
                                     'workbook_filename')))
    def _init(self, args):
        SimpleCmdLineArgs._init(args)
        args.log_level = LOGGING_LEVEL_MAP['OFF']

def run(args):
    logging_args = { 'format' : '%{asctime} %{levelname} %{message}',
                     'datefmt' : '%Y-%m-%d %H:%M:%S',
                     'level' : LOGGING_LEVEL_MAP[args.logging_level] }
    if hasattr(args, 'logging_filename'):
        logging_args['filename'] = args.logging_filename
    else:
        logging_args['stream'] = sys.stdout
    logging.basicConfig(**logging_args)
    
    config_file_parser = ConfigurationFileParser()
    configuration = config_file_parser.load(args.configuration_filename)

    db = Database(args.database_filename)
    if not venue:
        print 'ERROR: No such venue "%s"' % args.venue_name
        exit(1)

    workbook = Workbook(args.workbook_filename)
    director = Director(configuration, db)
    (num_imported, num_failed) = director.process(workbook, db)
    print 'Imported %d articles from %d groups' % (num_imported,
                                                   len(configuration.groups))
    print '%d articles failed to load' % num_failed

def usage(args = None):
    print """generic_ss_importer.py [--log-file <file>] [--log-level <level>]
                       --config <file> --db <file> <workbook>
  <workbook>            File with spreadsheet
  --config <file>       Configuration file
  --db <file>           Database file
  --log-file <file>     Log file.  Default is to write the log to stdout
  --log-level <level>   Logging level (TRACE, DEBUG, INFO, WARN, ERROR, OFF).
                        The default is OFF
"""

if __name__ == '__main__':
    parse_args_and_exec(CmdLineArgs(), run, usage)
