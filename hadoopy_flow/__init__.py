
import gevent
import gevent.event
import gevent.monkey
gevent.monkey.patch_all()
import subprocess
import sys
import atexit
HADOOPY_OUTPUTS = {}  # Output paths (key is the path, value is an event).  A listener waits till the event is true.
GREENLETS = []


def Greenlet(func, *args, **kw):
    a = gevent.Greenlet(func, *args, **kw)
    GREENLETS.append(a)
    return a


def _wait_on_input(in_path):
    import hadoopy
    if not hadoopy.exists(in_path) and in_path not in HADOOPY_OUTPUTS:
        print('Flow: Path [%s] does not exist yet, we will wait for it but you must create it eventually.' % in_path)
    if not hadoopy.exists(in_path) or in_path in HADOOPY_OUTPUTS:
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

#    def __setattr__(self, name, value):
#        return setattr(self._greenlet.get(), name, value)
#
#    def __delattr__(self, name):
#        return delattr(self._greenlet.get(), name)


def patch_all():
    if 'hadoopy' in sys.modules:
        raise ImportError('You must import hadoopy_flow before hadoopy!')
    import hadoopy
    
    def _patch_launch(launch):
        def _inner(in_path, out_path, *args, **kw):
            _new_output(out_path)
            gevent.sleep()
            if isinstance(in_path, str):
                _wait_on_input(in_path)
            else:
                for x in in_path:
                    _wait_on_input(x)
            print('Flow: All inputs available [%s]' % str(in_path))
            p = launch(in_path, out_path, wait=False, *args, **kw)
            while p['process'].poll() is None:
                gevent.sleep(.1)
            if p['process'].returncode:
                raise subprocess.CalledProcessError(p['process'].returncode, p['hadoop_cmds'][0])
            _set_output(out_path)
            return p

        def _wrap(*x, **y):
            GREENLETS.append(gevent.Greenlet(_inner, *x, **y))
            GREENLETS[-1].start()
            return LazyReturn(GREENLETS[-1])
        return _wrap

    def _patch_passive_hdfs(hdfs):

        def _inner(*args, **kw):
            # Wait for everything to finish up until this point
            gevent.sleep()
            while 1:
                if all([y.isSet() for x, y in HADOOPY_OUTPUTS.items()]):
                    break
                for x in HADOOPY_OUTPUTS:
                    _wait_on_input(x)
            return hdfs(*args, **kw)
        return _inner

    def _patch_readers(hdfs):  # ls, readtb

        def _inner(path, *args, **kw):
            print('Reader called on [%s]' % path)
            # Wait for everything to finish up until this point
            gevent.sleep()
            _wait_on_input(path)
            return hdfs(path, *args, **kw)
        return _inner

    def _patch_active_hdfs(hdfs, path_arg_num, path_kw):

        def _inner(*args, **kw):
            # Wait for everything to finish up until this point
            gevent.sleep()
            while 1:
                if all([y.isSet() for x, y in HADOOPY_OUTPUTS.items()]):
                    break
                for x in HADOOPY_OUTPUTS:
                    _wait_on_input(x)
            out = hdfs(*args, **kw)
            try:
                _set_output(args[path_arg_num])
            except IndexError:
                _set_output(kw[path_kw])  # Put
            return out
        return _inner

    hadoopy.launch_frozen = _patch_launch(hadoopy.launch_frozen)
    hadoopy.launch = _patch_launch(hadoopy.launch)
    hadoopy.launch_local = _patch_launch(hadoopy.launch_local)
    #hadoopy.exists = _patch_passive_hdfs(hadoopy.exists)
    #hadoopy.get = _patch_passive_hdfs(hadoopy.get)
    #hadoopy.ls = _patch_readers(hadoopy.ls)
    #hadoopy.put = _patch_active_hdfs(hadoopy.put, 1, 'hdfs_path')
    hadoopy.readtb = _patch_readers(hadoopy.readtb)
    #hadoopy.rm = _patch_passive_hdfs(hadoopy.rm)
    #hadoopy.writetb = _patch_active_hdfs(hadoopy.writetb, 0, 'path')
patch_all()
atexit.register(joinall)
