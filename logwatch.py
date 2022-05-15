import json
import os
import time
import errno
import stat
import time
from kafka import KafkaProducer
import sqlparse as sp

from query_parser import parse_queries

class LogWatcher(object):
    """Looks for changes in all files of a directory.
    This is useful for watching log file changes in real-time.
    It also supports files rotation.
    """

    def __init__(self, folder, callback, extensions=["log"], tail_lines=0,retention_period = 7200):
        """Arguments:

        (str) @folder:
            the folder to watch

        (callable) @callback:
            a function which is called every time a new line in a
            file being watched is found;
            this is called with "filename" and "lines" arguments.

        (list) @extensions:
            only watch files with these extensions

        (int) @tail_lines:
            read last N lines from files being watched before starting
        """
        self.files_map = {}
        self.callback = callback
        self.folder = os.path.realpath(folder)
        self.extensions = extensions
        self.retention_period = retention_period
        assert os.path.isdir(self.folder), "%s does not exists" \
                                            % self.folder
        assert callable(callback)
        self.update_files()
        # The first time we run the script we move all file markers at EOF.
        # In case of files created afterwards we don't do this.
        with open('state.json') as json_file:
            state_dict = json.load(json_file)
        json_file.close()
        for id, file in self.files_map.items():
            if id in state_dict:
                cursor = state_dict[id][1]
            else:
                cursor = 0
            print(cursor, file.name)
            file.seek(cursor)  # EOF
            if tail_lines:
                lines = self.tail(file.name, tail_lines)
                if lines:
                    self.callback(file.name, lines)

    def __del__(self):
        self.close()

    def loop(self, interval=0.1, async1=False):
        """Start the loop.
        If async is True make one loop then return.
        """
        while 1:
            self.update_files()
            for fid, file in list(self.files_map.items()):
                self.readfile(file)
            self.update_state()
            if async1:
                return
            time.sleep(interval)

    def update_state(self):
        state_dict = {k: [v.name, v.tell()] for k, v in self.files_map.items()}
        with open("state.json", "w") as outfile:
            json.dump(state_dict, outfile)
        outfile.close()

    def log(self, line):
        """Log when a file is un/watched"""
        print(line)

    def listdir(self):
        """List directory and filter files by extension.
        You may want to override this to add extra logic or
        globbling support.
        """
        ls = os.listdir(self.folder)
        if self.extensions:
            return [x for x in ls if os.path.splitext(x)[1][1:] \
                                           in self.extensions]
        else:
            return ls

    @staticmethod
    def tail(fname, window):
        """Read last N lines from file fname."""
        try:
            f = open(fname, 'r')
        except IOError as err:
            if err.errno == errno.ENOENT:
                return []
            else:
                raise
        else:
            BUFSIZ = 1024
            f.seek(0, os.SEEK_END)
            fsize = f.tell()
            block = -1
            data = ""
            exit = False
            while not exit:
                step = (block * BUFSIZ)
                if abs(step) >= fsize:
                    f.seek(0)
                    exit = True
                else:
                    f.seek(step, os.SEEK_END)
                data = f.read().strip()
                if data.count('\n') >= window:
                    break
                else:
                    block -= 1
            return data.splitlines()[-window:]

    def update_files(self):
        ls = []
        for name in self.listdir():
            absname = os.path.realpath(os.path.join(self.folder, name))
            try:
                st = os.stat(absname)
            except EnvironmentError as err:
                if err.errno != errno.ENOENT:
                    raise
            else:
                modificationTime = int(time.time() - st[stat.ST_MTIME]) // self.retention_period
                if not stat.S_ISREG(st.st_mode) or modificationTime > 0:
                    continue
                fid = self.get_file_id(st)
                ls.append((fid, absname))

        # check existent files
        for fid, file in list(self.files_map.items()):
            try:
                st = os.stat(file.name)
            except EnvironmentError as err:
                if err.errno == errno.ENOENT:
                    self.unwatch(file, fid)
                else:
                    raise
            else:
                if fid != self.get_file_id(st):
                    # same name but different file (rotation); reload it.
                    self.unwatch(file, fid)
                    self.watch(file.name)
                modificationTime = int(time.time() - st[stat.ST_MTIME]) // self.retention_period
                if modificationTime > 0:
                    self.unwatch(file, fid)

        # add new ones
        for fid, fname in ls:
            if fid not in self.files_map:
                self.watch(fname)

    def readfile(self, file):
        lines = file.read().splitlines()
        if lines:
            self.callback(file.name, lines)

    def watch(self, fname):
        try:
            file = open(fname, "r")
            fid = self.get_file_id(os.stat(fname))
        except EnvironmentError as err:
            if err.errno != errno.ENOENT:
                raise
        else:
            self.log("watching logfile %s" % fname)
            self.files_map[fid] = file

    def unwatch(self, file, fid):
        # file no longer exists; if it has been renamed
        # try to read it for the last time in case the
        # log rotator has written something in it.
        lines = self.readfile(file)
        self.log("un-watching logfile %s" % file.name)
        del self.files_map[fid]
        if lines:
            self.callback(file.name, lines)

    @staticmethod
    def get_file_id(st):
        return "%xg%x" % (st.st_dev, st.st_ino)

    def close(self):
        for id, file in self.files_map.items():
            file.close()
        self.files_map.clear()




if __name__ == '__main__':
    # producer = KafkaProducer(bootstrap_servers='localhost:9092')


    def callback(filename, lines):
        try:
            querylist = []
            str = ""
            for line in lines:
                if("timestamp" in line):
                    if str:
                        querylist.append(" ".join(str.split()))
                    str = line
                else:
                    str = str + line
            querylist.append(" ".join(str.split()))
            
            for i in querylist:
                print(i)
                # if "statement: " in i:
                #     queries = i.split("statement: ")[1]
                #     parser = parse_queries(queries)
                #     hook_message = parser.get_final_message()
                #     print(hook_message)
                        # producer.send('stream-input',i.encode(encoding="UTF-8",errors="ignore")) 
        except Exception as e:
            print(time.time(), 'caught exception ',e,' rendering a new log line in %s' % filename)

    l = LogWatcher("/Library/PostgreSQL/13/pg_log", callback)
    #l = LogWatcher("log_files", callback)
    l.loop()

    