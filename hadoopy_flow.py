import gevent
import gevent.event
import subprocess
HADOOPY_INPUTS = set()  # Paths we didn't create (pre-existing)
HADOOPY_OUTPUTS = {}  # Output paths (key is the path, value is an event).  A listener waits till the event is true.


def patch_all():
    import hadoopy
    
    def _patch_launch(launch):
        def _inner(in_path, out_path, *args, **kw):
            if isinstance(in_path, str) and not hadoopy.exists(in_path):
                HADOOPY_OUTPUTS.setdefault(in_path, gevent.event.Event()).wait()
            else:
                for x in in_path:
                    if not hadoopy.exists(x):
                        HADOOPY_OUTPUTS.setdefault(in_path, gevent.event.Event()).wait()
            p = launch(in_path, out_path, wait=False, *args, **kw)
            while p['process'].poll() is None:
                gevent.sleep(.1)
            if p.returncode:
                raise subprocess.CalledProcessError(p['process'].returncode, p['hadoop_cmds'][0])
            try:
                HADOOPY_OUTPUTS.setdefault(out_path, gevent.event.Event()).set()
            except KeyError:
                pass
        return _inner

    hadoopy.launch_frozen = _patch_launch(hadoopy.launch_frozen)
    hadoopy.launch = _patch_launch(hadoopy.launch)
    hadoopy.launch_local = _patch_launch(hadoopy.launch_local)
