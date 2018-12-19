#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno
import sqlite3

from fuse import FUSE, FuseOSError, Operations

def Trace(f):
    def my_f(*args, **kwargs):
        print "entering ",  f.__name__ ,"with args ",args,kwargs
        result= f(*args, **kwargs)
        print "exiting " +  f.__name__
        return result
    my_f.__name = f.__name__
    my_f.__doc__ = f.__doc__
    return my_f



class Passthrough(Operations):
    def __init__(self, root):
        self.root = root

    def isTable(self, path):
        if len(path.split("/")) == 1:
            return True
        return False

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    @Trace
    def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    @Trace
    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    @Trace
    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    @Trace
    def getattr(self, path, fh=None):
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        attr = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
        print(attr)
        return attr

    @Trace
    def readdir(self, path, fh):
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

    @Trace
    def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    @Trace
    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    @Trace
    def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    @Trace
    def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)

    @Trace
    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    @Trace
    def unlink(self, path):
        return os.unlink(self._full_path(path))

    @Trace
    def symlink(self, name, target):
        return os.symlink(target, self._full_path(name))

    @Trace
    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    @Trace
    def link(self, target, name):
        return os.link(self._full_path(name), self._full_path(target))

    @Trace
    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    @Trace
    def open(self, path, flags):
        full_path = self._full_path(path)
        return os.open(full_path, flags)

    @Trace
    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    @Trace
    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    @Trace
    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    @Trace
    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    @Trace
    def flush(self, path, fh):
        return os.fsync(fh)

    @Trace
    def release(self, path, fh):
        return os.close(fh)

    @Trace
    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)


def main(mountpoint, root):
    try:
        os.lstat(mountpoint)
    except:
        os.mkdir(mountpoint)
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
