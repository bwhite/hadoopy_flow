import gevent
import gevent.event
import subprocess
import sys
HADOOPY_OUTPUTS = {}  # Output paths (key is the path, value is an event).  A listener waits till the event is true.


def _wait_on_input(in_path):
    print('Flow: Waiting for [%s]' % in_path)
    HADOOPY_OUTPUTS.setdefault(in_path, gevent.event.Event()).wait()
    print('Flow: Obtained [%s]' % in_path)


def _set_output(out_path):
    print('Flow: Output [%s]' % out_path)
    HADOOPY_OUTPUTS.setdefault(out_path, gevent.event.Event()).set()


def patch_all():
    if 'hadoopy' in sys.modules:
        raise ImportError('You must import hadoopy_flow before hadoopy!')
    import hadoopy
    
    def _patch_launch(launch):
        def _inner(in_path, out_path, *args, **kw):
            if isinstance(in_path, str) and not hadoopy.exists(in_path):
                _wait_on_input(in_path)
            else:
                for x in in_path:
                    if not hadoopy.exists(x):
                        _wait_on_input(in_path)
            p = launch(in_path, out_path, wait=False, *args, **kw)
            while p['process'].poll() is None:
                gevent.sleep(.1)
            if p.returncode:
                raise subprocess.CalledProcessError(p['process'].returncode, p['hadoop_cmds'][0])
            _set_output(out_path)
        return lambda *x, **y: gevent.Greenlet(_inner, *x, **y).start()

    def _patch_passive_hdfs(hdfs):

        def _inner(*args, **kw):
            # Wait for everything to finish up until this point
            while 1:
                if all([y.isSet() for x, y in HADOOPY_OUTPUTS.items()]):
                    break
                for x in HADOOPY_OUTPUTS:
                    _wait_on_input(x)
            hdfs(*args, **kw)
        return lambda *x, **y: gevent.Greenlet(_inner, *x, **y).start()

    def _patch_active_hdfs(hdfs, path_arg_num, path_kw):

        def _inner(*args, **kw):
            # Wait for everything to finish up until this point
            while 1:
                if all([y.isSet() for x, y in HADOOPY_OUTPUTS.items()]):
                    break
                for x in HADOOPY_OUTPUTS:
                    _wait_on_input(x)
            hdfs(*args, **kw)
            try:
                _set_output(args[path_arg_num])
            except IndexError:
                _set_output(kw[path_kw])  # Put
        return lambda *x, **y: gevent.Greenlet(_inner, *x, **y).start()

    hadoopy.launch_frozen = _patch_launch(hadoopy.launch_frozen)
    hadoopy.launch = _patch_launch(hadoopy.launch)
    hadoopy.launch_local = _patch_launch(hadoopy.launch_local)
    hadoopy.exists = _patch_passive_hdfs(hadoopy.exists)
    hadoopy.get = _patch_passive_hdfs(hadoopy.get)
    hadoopy.ls = _patch_passive_hdfs(hadoopy.ls)
    hadoopy.put = _patch_active_hdfs(hadoopy.put, 1, 'hdfs_path')
    hadoopy.readtb = _patch_passive_hdfs(hadoopy.readtb)
    hadoopy.rm = _patch_passive_hdfs(hadoopy.rm)
    hadoopy.writetb = _patch_active_hdfs(hadoopy.writetb, 0, 'path')
patch_all()
