import threading
import traceback
import queue as Queue
import time
import sys
import os
import string
import logging
import logging.handlers as lh
import pycurl
import certifi
from io import BytesIO

config = {
    "LogDirectory": "./",
    "LogFileName": "noaa_downloads.log",
    "MaxLogFileBytes": 1<<20,
    "BackupCount": 5,
    "MaxAttempts": 3
}

def SetupLogging():
    logger = logging.getLogger("Console & File Logger")
    logger.setLevel(logging.DEBUG)

    #Console Logger
    console_handler = logging.StreamHandler()
    console_log_formatter = logging.Formatter(
        "[%(asctime)s.%(msecs)03d] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S")
    console_handler.setFormatter(console_log_formatter)
    logger.addHandler(console_handler)

    # Extracts the filepath else sets logs to current working directory
    filepath = config["LogDirectory"]
    fqname = os.path.join(filepath, config["LogFileName"])
    if not os.path.exists(filepath):
        os.makedirs(filepath, exist_ok=True)
    if os.path.isfile(fqname):
        os.remove(fqname)

    #File Logger
    # Creates rotating filehandler
    file_handler = lh.RotatingFileHandler(filename=fqname, maxBytes=config["MaxLogFileBytes"],
                                        backupCount=config["BackupCount"])
    file_log_formatter = logging.Formatter(
        "[%(asctime)s.%(msecs)03d] %(levelname)s:%(message)s", datefmt="%Y%m%d %H:%M:%S")
    file_handler.setFormatter(file_log_formatter)
    logger.addHandler(file_handler)
    return logger

LOGGER = SetupLogging()

class ReqURLGenerator():
    '''
    URLs have this format
    https://nomads.ncep.noaa.gov:443/dods/gefs/gefs20201207/gefs_pgrb2ap5_all_00z.ascii?tmp2m[0:30][0:64][255][560]
    $URLBASE $DATE $QUERYPREFIX $TIME $QUERYPOSTFIX $VARIABLE $RANGE1 $RANGE2 $LAT $LON
    URLBASE: https://nomads.ncep.noaa.gov:443/dods/gefs/gefs
    DATE: 20201207
    QUERYPREFIX: /gefs_pgrb2ap5_all_
    TIME: 00z
    QUERYPOSTFIX: .ascii?
    VARIABLE: tmp2m
    RANGE1: [0:30]
    RANGE2: [0:64]
    LAT: [255]
    LON: [560]
    '''
    URLBASE = 'https://nomads.ncep.noaa.gov:443/dods/gefs/gefs'
    QUERYPREFIX = 'gefs_pgrb2ap5_all_'
    TIME = [ '00z', '06z', '12z', '18z' ]
    QUERYPOSTFIX = '.ascii?'
    VARIABLE = [ 'tmp2m', 'pressfc', 'rh2m', 'dlwrfsfc', 'dswrfsfc', 'apcpsfc', 'ugrd10m', 'vgrd10m' ]
    RANGE1 = '[0:30]'
    RANGE2 = '[0:64]'

    def __init__(self, lat, lon, date):
        self._lat = lat
        self._lon = lon
        self._date = date
        self.urllist = []

    def __repr__(self):
        return "urllist = %s" % (self.urllist)

    def __iter__(self):
        for x in self.urllist:
            yield x

    def __len__(self):
        return len(self.urllist)

    def GenList(self):
        self.urllist.clear()
        for i in range(0, len(self.TIME)):
            for j in range(0, len(self.VARIABLE)):
                url = self.URLBASE + self._date + "/" + self.QUERYPREFIX + self.TIME[i] + \
                    self.QUERYPOSTFIX + self.VARIABLE[j] + self.RANGE1 + self.RANGE2 + \
                    self._lat + self._lon
                fn = self.QUERYPREFIX + self.TIME[i] + self.QUERYPOSTFIX + self.VARIABLE[j] + self.RANGE1 + self.RANGE2 + \
                    self._lat + self._lon
                self.urllist.append((url, fn))

class DownloadItem():
  def __init__(self, url, local_filename, timeout=90):
    self.url = url
    self.local_filename = local_filename
    self.attempts = 0
    self.timeout = timeout

class QueuedDownloader():
    def __init__(self):
        self._logger = LOGGER
        self._download_queue = Queue.Queue()
        self._dl_thread = threading.Thread(target=self.__worker, daemon=True)
        self._dl_thread.start()
        self._failed = []

    def AddDownloadItem(self, entry):
        self._download_queue.put(entry)

    def ValidateDownloaded(self, entry):
        self._logger.info("Validating file %s", entry.local_filename)
        with open(entry.local_filename) as f:
            if 'lon, [1]' in f.read():
                return True
        return False

    def CheckDownloaded(self, entry):
        self._logger.info("Checking if file already exists for %s as %s", entry.url, entry.local_filename)
        if os.path.exists(entry.local_filename):
            self._logger.info("File already exists %s", entry.local_filename)
            if self.ValidateDownloaded(entry):
                self._logger.info("File validated %s", entry.local_filename)
                return True
            else:
                self._logger.info("File not validated %s", entry.local_filename)
                return False
        self._logger.info("File does not exist %s", entry.local_filename)
        return False

    def Download(self, entry):
        self._logger.info("Attempting to download resource %s as %s", entry.url, entry.local_filename)
        if self.CheckDownloaded(entry):
            self._logger.info("Already downloaded, skipping %s", entry.local_filename)
            return
        entry.attempts += 1
        c = pycurl.Curl()
        try:
            with open(entry.local_filename, 'wb') as f:
                c.setopt(c.URL, entry.url)
                c.setopt(c.TIMEOUT, entry.timeout)
                c.setopt(c.WRITEDATA, f)
                c.setopt(c.CAINFO, certifi.where())
                c.perform()
                self._logger.info("Number of Attempts: %i", entry.attempts)
                if entry.attempts >= config["MaxAttempts"]:
                    raise err0
                c.close()
                # TODO:validate file
                if self.ValidateDownloaded(entry):
                    self._logger.info("... %s download completed", entry.local_filename)
                else:
                    self._logger.info("Validation failed, requeuing for download %s", entry.url)
                    self.AddDownloadItem(entry)
        except Exception as err0:
            self._logger.info("Exhausted %i attempts for %s", entry.attempts, entry.url)
            self._failed.append(entry)
        except pycurl.error as err:
            log_msg = "Download error occurred:\n{0}".format(err)
            self._logger.warning(log_msg)
            if entry.attempts < config["MaxAttempts"]:
                self._logger.info("Requeuing for download %s", entry.url)
                self.AddDownloadItem(entry)
            else:
                self._logger.info("Exhausted %i attempts for %s", entry.attempts, entry.url)
                self._failed.append(entry)

    def WaitUntilJobsComplete(self):
        self._download_queue.join()

    def ThreadJoin(self):
        self._dl_thread.join()

    def __worker(self):
        self._logger.info("Starting the download loop")
        while True:
            entry = self._download_queue.get()
            # Terminate when entry is None
            if entry is None:
                self._logger.info("Exiting the download loop")
                self._download_queue.task_done()
                return
            else:
                try:
                    self.Download(entry)
                except Exception as err:
                    log_msg = "Download error:{0}\n{1}".format(err, traceback.format_exc())
                    self._logger.error(log_msg)
                finally:
                    self._download_queue.task_done()

    def Report(self):
        if len(self._failed) == 0:
            self._logger.info("All downloads completed successfully")
            return 0
        else:
            self._logger.info("%i items failed to download\n%s", len(self._failed), str(self._failed))
            return 1

def main():
    if len(sys.argv) != 5:
        print("Usage: " + sys.argv[0] + " base_directory date lat lon\n")
        print("Example: " + sys.argv[0] + " /opt/flare 20201207 255 560\n")
        return

    lat = '[' + str(sys.argv[3]) + ']'
    lon = '[' + str(sys.argv[4]) + ']'
    date = str(sys.argv[2])
    basedir = str(sys.argv[1]) + '/' + date + '/'
    if not os.path.exists(basedir):
        os.makedirs(basedir)

    url_gen = ReqURLGenerator(lat, lon, date)
    url_gen.GenList()
    LOGGER.debug("%i items generated for download\n%s", len(url_gen), str(url_gen))

    qd = QueuedDownloader()
    index = 0
    for req in url_gen:
        item = DownloadItem(req[0], basedir + "{0}".format(req[1]))
        index += 1
        qd.AddDownloadItem(item)


    qd.WaitUntilJobsComplete()
    qd.AddDownloadItem(None)
    qd.ThreadJoin()
    retcode = qd.Report()
    sys.exit(retcode)

if __name__ == "__main__":
    main()
