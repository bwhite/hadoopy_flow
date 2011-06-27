import gevent
import gevent.event
import gevent.monkey
gevent.monkey.patch_all()
import subprocess
import sys
import atexit
import os
HADOOPY_OUTPUTS = {}  # Output paths (key is the path, value is an event).  A listener waits till the event is true.
GREENLETS = []
USE_EXISTING = False


def Greenlet(func, *args, **kw):
    a = gevent.Greenlet(func, *args, **kw)
    GREENLETS.append(a)
    return a


def _wait_on_input(in_path):
    import hadoopy
    if not hadoopy.exists(in_path) and in_path not in HADOOPY_OUTPUTS:
        #print('Flow: Path [%s] does not exist yet, we will wait for it but you must create it eventually.' % in_path)
        print('Flow: Path [%s] does not exist yet, you will probably get an error from hadoop.' % in_path)
    if in_path in HADOOPY_OUTPUTS:  # not hadoopy.exists(in_path)
        print('Flow: Waiting for [%s]' % in_path)
        HADOOPY_OUTPUTS.setdefault(in_path, gevent.event.Event()).wait()
        print('Flow: Obtained [%s]' % in_path)


def _set_output(out_path):
    print('Flow: Output [%s]' % out_path)
    HADOOPY_OUTPUTS.setdefault(out_path, gevent.event.Event()).set()


def _new_output(out_path):
    print('Flow: New Output [%s]' % out_path)
    HADOOPY_OUTPUTS.setdefault(out_path, gevent.event.Event())


def joinall():
    if GREENLETS:
        print('Flow: Waiting until all Greenlets finish')
        while [x for x in GREENLETS if not x.ready()]:
            gevent.sleep(.1)
        print('Flow: Joining all outstanding Greenlets')
        gevent.joinall(GREENLETS)


class LazyReturn(object):

    def __init__(self, greenlet):
        self._greenlet = greenlet

    def __getattr__(self, name):
        return getattr(self._greenlet.get(), name)

    def __getitem__(self, index):
        return self._greenlet.get().__getitem__(index)

    def __setitem__(self, index, value):
        return self._greenlet.get().__setitem__(index, value)

    def __delitem__(self, index):
        return self._greenlet.get().__delitem__(index)


def canonicalize_path(path):
    import hadoopy
    return hadoopy.abspath(path)


def patch_all():
    if 'hadoopy' in sys.modules:
        raise ImportError('You must import hadoopy_flow before hadoopy!')
    import hadoopy
    
    def _patch_launch(launch):
        def _inner(in_path, out_path, *args, **kw):
            out_path = canonicalize_path(out_path)
            _new_output(out_path)
            if isinstance(in_path, str):
                in_path = canonicalize_path(in_path)
            else:
                in_path = [canonicalize_path(x) for x in in_path]
            gevent.sleep()
            if isinstance(in_path, str):
                _wait_on_input(in_path)
            else:
                for x in in_path:
                    _wait_on_input(x)
            print('Flow: All inputs available [%s]' % str(in_path))
            if USE_EXISTING and hadoopy.exists(out_path):
                print(("Flow: Resusing output [%s].  1.) You can't use the return value"
                       " of this command (it is set to None) and 2.) The existing output is assumed to be correct.") % out_path)
                p = None
            else:
                p = launch(in_path, out_path, wait=False, *args, **kw)
                while p['process'].poll() is None:
                    gevent.sleep(.1)
                print('Flow: Process completed')
                if p['process'].returncode:
                    for x in range(10):
                        print('Flow: Task failed....[%d/10]' % x)
                    raise subprocess.CalledProcessError(p['process'].returncode, p['hadoop_cmds'][0])
            _set_output(out_path)
            return p

        def _wrap(*x, **y):
            GREENLETS.append(gevent.Greenlet(_inner, *x, **y))
            GREENLETS[-1].start()
            return LazyReturn(GREENLETS[-1])
        return _wrap

    def _patch_readers(hdfs):  # ls, readtb

        def _inner(path, *args, **kw):
            path = canonicalize_path(path)
            print('Flow: Reader called on [%s]' % path)
            # Wait for everything to finish up until this point
            gevent.sleep()
            _wait_on_input(path)
            return hdfs(path, *args, **kw)
        return _inner

    def _patch_writers(hdfs):  # writetb

        def _inner(out_path, *args, **kw):
            out_path = canonicalize_path(out_path)
            _new_output(out_path)
            print('Flow: Writer called on [%s]' % out_path)
            gevent.sleep()
            out = hdfs(out_path, *args, **kw)
            _set_output(out_path)
            return out
        return _inner

    hadoopy.launch_frozen = _patch_launch(hadoopy.launch_frozen)
    hadoopy.launch = _patch_launch(hadoopy.launch)
    hadoopy.launch_local = _patch_launch(hadoopy.launch_local)
    hadoopy.readtb = _patch_readers(hadoopy.readtb)
    hadoopy.writetb = _patch_writers(hadoopy.writetb)
    #hadoopy.rm = _patch_passive_hdfs(hadoopy.rm)
    #hadoopy.writetb = _patch_active_hdfs(hadoopy.writetb, 0, 'path')
    #hadoopy.exists = _patch_passive_hdfs(hadoopy.exists)
    #hadoopy.get = _patch_passive_hdfs(hadoopy.get)
    #hadoopy.ls = _patch_readers(hadoopy.ls)
    #hadoopy.put = _patch_active_hdfs(hadoopy.put, 1, 'hdfs_path')

patch_all()
atexit.register(joinall)
