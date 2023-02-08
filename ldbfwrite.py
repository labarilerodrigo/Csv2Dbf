#!/usr/bin/python3.6
"""
ldbfwrite utility script
"""
import argparse
import logging
import logging.config
import os
from typing import List, cast, Type
from typing import Optional
import dbf
import csv
import sys

# << Global variables >>
# Hdfs commands
hdfs_rm = "hadoop fs -rm -r"
hdfs_mkdir = "hadoop fs -mkdir -p"
hdfs_ls = "hadoop fs -ls"
hdfs_copyToLocal = "hadoop fs -copyToLocal"
hdfs_prefix = "hdfs://"
lm_rm = "rm -r"

class LdbfLogger:
    def __init__(self):
        frmt = "%(asctime)s [PID %(process)5s] [%(levelname)-5.5s] - %(module)s - %(lineno)s - msg: %(message)s"
        logging.basicConfig(level=logging.DEBUG, format=frmt, handlers=[logging.StreamHandler()])
        self.logger = logging.getLogger(self.__class__.__name__)
        self.level = logging.DEBUG
        self.format = logging.Formatter(frmt)


class AbstractLdbfWriteCommand:
    @classmethod
    def arguments(cls,sub_parser: argparse.ArgumentParser) -> None:
        pass

    def __init__(self) -> None:
        pass

    def execute(self, options: argparse.Namespace) -> None:
        pass


class dbfWriter(LdbfLogger):    
    def writeDbfTable(self, csv_filename, dbf_filename, fieldSpecs):
        table = dbf.Table(dbf_filename, fieldSpecs)
        self.logger.info("DBF file %s", dbf_filename)
        self.logger.info("CSV file %s", csv_filename)
        self.logger.info("DBF definition created with field names args %s", table.field_names)

        with open(csv_filename, newline='', encoding='utf-8') as f:
            self.logger.info("Opening DBF file %s", dbf_filename)
            with table.open(dbf.READ_WRITE):
                for line in csv.reader(f):
                    table.append(tuple(line))
                    self.logger.info("Tupple added %s", tuple(line))

        table.close()


class BaseLdbfWriteCommand(object):
    @property
    def base_parser(self) -> argparse.ArgumentParser:
        return self.base_parser

    @base_parser.getter
    def base_parser(self) -> argparse.ArgumentParser:
        sub_parser: argparse.ArgumentParser = argparse.ArgumentParser(prog="Ldbfwrite")
        sub_parser.add_argument('-CSV',
                                '--csvfile',
                                dest='csvfile',
                                help='Input .csv/.CSV filename to read',
                                type=str,
                                required=True
                                )

        sub_parser.add_argument('-DBF',
                                '--dbffile',
                                dest='dbffile',
                                help='Input .dbf/.DBF filename to write',
                                type=str,
                                required=True
                                )

        sub_parser.add_argument('-FS',
                                '--fieldspecs',
                                dest='fieldspecs',
                                help='Field specifications for .dbf/.DBF filename\n For further details: http://www.dbase.com/help/Design_Tables/IDH_TABLEDES_FIELD_TYPES.htm',
                                type=str,
                                required=True
                                )

        return sub_parser


class LdbfLoadApp(LdbfLogger):
    csvextension = ".CSV"
    dbfextension = ".DBF"

    def __init__(self, *args, **kw):
        super().__init__()

    def pre_process(self, cli):
        try:
            self.logger.info("Pre-processing steps")
            self.hdfs_copytolocal(cli.args.csvfile, cli.args.dbffile)
        except Exception as e:
            raise e

    def hdfs_copytolocal(self, src, dst):
        self.logger.info("parsing path..")
        dst_parsed=self.remove_extension(dst) + self.csvextension
        lmcmd = f"{lm_rm} {dst_parsed}"
        rt = os.system(lmcmd)
        hdfscmd = f"{hdfs_copyToLocal} {src} {dst_parsed}"
        rt = os.system(hdfscmd)
        if bool(rt):
            self.logger.exception(f"command {hdfscmd} failed")
            raise Exception(f"command {hdfscmd} failed")
        else:
            msg=f"file {dst_parsed} copied to edge node"
            self.logger.info(msg)

    def post_process(self, cli):
        try:
            self.logger.info("Post-processing steps")
            self.logger.info("no steps processed")
            pass
        except Exception as e:
            raise e

    def remove_extension(self, path):
        return os.path.splitext(path)[0]

    def process(self, cli):
        try:
            self.logger.info("Processing steps")
            dst_parsed=self.remove_extension(cli.args.dbffile) + self.csvextension
            app = dbfWriter()
            app.writeDbfTable(dst_parsed, cli.args.dbffile, cli.args.fieldspecs)
        except Exception as e:
            raise e


class LdbfloadCLI():
    def __init__(self, *args, **kwargs):
        self.args = self.get_parser()

    def get_parser(self, argv: List[str] = sys.argv[1:]) -> argparse.Namespace:
        parser = BaseLdbfWriteCommand().base_parser
        options = parser.parse_args(argv)
        return options


if __name__ == '__main__':
    try:
        cli = LdbfloadCLI()
        options = cli.get_parser(sys.argv[1:])
        app = LdbfLoadApp()
        app.pre_process(cli)
        app.process(cli)
        app.post_process(cli)

    except Exception as e:
        raise SystemExit(f"Application failed {e}, exiting...")
