from multiprocessing import Process
import logging
import ftplib
import time


class FTPDataSUS:
    """Common class to connect to DataSUS FTP server"""

    def __init__(self):
        self.host = 'ftp.datasus.gov.br'
        self.FTP = None

    def connect(self):
        self.FTP = ftplib.FTP(self.host)
        self.FTP.connect()
        self.FTP.login()

    def reconnect(self):
        """Verifies if the connection is up, reconnects if needed"""
        try:
            self.FTP.voidcmd('NOOP')
        except (BrokenPipeError, ConnectionResetError):
            self.connect()
            logging.debug(f'Reconnecting to {self.host}')

    def close(self):
        if self.FTP is not None:
            self.FTP.quit()
            self.FTP = None

class FTPDataSUSClient:
    """
    The client will keep the connection alive as a Process
    running in the background, according to its timeout. It
    will start the process after connecting to the FTP and
    run the NOOP at every 15 seconds, until the timeout is
    reached. Closing the Client will close the process, or
    it will close itself when the timeout time expires.
    """

    def __init__(self, keepalive_timeout=3600):
        self.server = FTPDataSUS()
        self.keepalive_proc: Process = None
        self.timeout = keepalive_timeout
        self.conn_time = None

    def connect(self):
        self.conn_time = time.time()
        self.keepalive_proc = Process(
            target=self._keep_alive_loop, daemon=True
        )
        self.server.connect()
        self.keepalive_proc.start()

    def close(self):
        self.server.close()
        self.keepalive_proc.close()
        self.keepalive_proc = None
        self.conn_time = None

    def _keep_alive_loop(self):
        while time.time() - self.conn_time < self.timeout:
            self.server.reconnect()
            time.sleep(15)
        logging.debug(f'Connection timed out')
        self.close()
