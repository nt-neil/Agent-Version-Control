"""Small process-level acceptance checks; all mutations use owned /tmp fixtures."""
import contextlib
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time

REPO = next(p for p in Path(__file__).resolve().parents if (p/'go.mod').is_file())
AVC = REPO / 'bin/avc'
AVC_HASH = hashlib.sha256(AVC.read_bytes()).hexdigest()
RESULTS = []

class Fixture:
    def __init__(self, root):
        self.root = Path(root)
        self.env = {k:v for k,v in os.environ.items() if not k.startswith(('GIT_', 'AVC_'))}
        self.env.update(AVC_ROOT=root, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_SYSTEM=os.devnull)
        self.proc = None
        self.clients = []
        self.log = open(self.root / 'daemon-log', 'ab')
        self.call('git', 'init', '-q')
    def call(self, *args, timeout=8, check=True):
        return subprocess.run(args, cwd=self.root, env=self.env, capture_output=True, text=True, timeout=timeout, check=check)
    def avc(self, *args, **kwargs):
        return self.call(str(AVC), *args, **kwargs)
    def start(self, load='100000'):
        self.proc = subprocess.Popen([str(AVC), 'daemon', '--root', str(self.root), '--max-jobs', '1', '--workers', '1', '--max-load', load, '--min-free-mb', '1'], cwd=self.root, env=self.env, stdout=self.log, stderr=self.log, start_new_session=True)
        end = time.monotonic() + 6
        while time.monotonic() < end:
            assert self.proc.poll() is None, 'daemon exited during startup'
            r = self.avc('stats', timeout=1, check=False)
            if r.returncode == 0:
                return
            time.sleep(.03)
        raise AssertionError('daemon readiness timeout')
    def client(self, *args):
        p = subprocess.Popen([str(AVC), *args], cwd=self.root, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
        self.clients.append(p)
        return p
    def stats(self):
        return json.loads(self.avc('stats').stdout)
    def stop(self, sig=signal.SIGTERM):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(sig)
            self.proc.wait(timeout=5)
    def cleanup(self):
        # Let the daemon cancel its own job process groups before forced cleanup.
        try:
            self.stop()
        except subprocess.TimeoutExpired:
            pass
        for p in self.clients + [self.proc]:
            if p is not None and p.poll() is None:
                os.killpg(p.pid, signal.SIGKILL)
                p.wait(timeout=3)
        self.log.close()

@contextlib.contextmanager
def fixture():
    with tempfile.TemporaryDirectory(prefix='avc-accept-', dir='/tmp') as root:
        f = Fixture(root)
        try:
            yield f
        finally:
            f.cleanup()

def snapshot():
    with fixture() as f, tempfile.TemporaryDirectory(prefix='avc-signals-', dir='/tmp') as signals:
        (f.root/'state.txt').write_text('v1')
        f.start()
        ready, release = Path(signals)/'ready', Path(signals)/'release'
        code = "from pathlib import Path; import time; p=Path('state.txt'); before=p.read_text(); Path(%r).write_text('ready'); deadline=time.monotonic()+8\nwhile not Path(%r).exists() and time.monotonic()<deadline: time.sleep(.01)\nprint(before+'|'+p.read_text())" % (str(ready), str(release))
        p = f.client('run', '--snapshot', '--', sys.executable, '-c', code)
        end = time.monotonic()+6
        while not ready.exists() and time.monotonic()<end:
            assert p.poll() is None, p.communicate()
            time.sleep(.02)
        assert ready.exists(), 'snapshot job did not reach barrier'
        f.avc('edit', 'state.txt', '--old', 'v1', '--new', 'v2')
        assert p.poll() is None, 'job ended before concurrent edit completed'
        release.write_text('go')
        out, err = p.communicate(timeout=5)
        assert p.returncode == 0 and out.strip() == 'v1|v1', (p.returncode, out, err)
        assert f.avc('cat', 'state.txt').stdout == 'v2'
        snaps = f.root/'.avc/snaps'
        assert not snaps.exists() or not list(snaps.iterdir()), 'snapshot leaked after run'
        for flag in ('--share', '--freeze'):
            r = f.avc('run', '--snapshot', flag, '--', 'true', check=False)
            assert r.returncode == 2, (flag, r.returncode)
        f.stop()
        RESULTS.append({'test':'snapshot_concurrent_edit', 'correct':True, 'job_version':'v1', 'live_version':'v2', 'remaining_snapshots':0})

def crash():
    with fixture() as f:
        (f.root/'state.txt').write_text('v0')
        f.start()
        journal_before_crash = []
        for i in range(1,4):
            f.avc('edit', 'state.txt', '--old', 'v'+str(i-1), '--new', 'v'+str(i))
            stats = f.stats()
            journal_before_crash.append(stats['journal_bytes'])
            f.stop(signal.SIGKILL)
            f.start()
            assert f.avc('cat', 'state.txt').stdout == 'v'+str(i)
        assert all(n>0 for n in journal_before_crash), journal_before_crash
        f.stop()
        RESULTS.append({'test':'acked_write_sigkill_recovery', 'correct':True, 'rounds':3, 'journal_bytes_before_kill':journal_before_crash, 'limit':'process crash only; not physical power loss'})

def load_gate():
    with fixture() as f:
        f.start('0.000001')
        p=f.client('run', '--', 'true')
        end=time.monotonic()+4
        stats=None
        while time.monotonic()<end:
            stats=f.stats()['governor']
            if stats.get('waits_for_load',0)>0:
                break
            time.sleep(.05)
        assert stats.get('waits_for_load',0)>0 and stats.get('started',0)==0, stats
        assert p.poll() is None
        f.stop()
        p.communicate(timeout=3)
        RESULTS.append({'test':'load_gate_before_first_job', 'correct':True, 'started':stats.get('started',0), 'waits_for_load':stats['waits_for_load']})

def partial_connections():
    with fixture() as f:
        f.start()
        paths=[p for p in (f.root/'.avc').rglob('*') if p.is_socket()]
        assert len(paths)==1, paths
        connections=[]
        try:
            for _ in range(33):
                s=socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.settimeout(2)
                s.connect(str(paths[0]))
                s.sendall(b'{')
                connections.append(s)
            time.sleep(.1)
            start=time.monotonic()
            f.stop()
            RESULTS.append({'test':'shutdown_with_partial_json_connections', 'correct':True, 'connections':len(connections), 'shutdown_ms':round((time.monotonic()-start)*1000,2)})
        finally:
            for s in connections: s.close()

def snapshot_crash():
    with fixture() as f, tempfile.TemporaryDirectory(prefix='avc-signals-', dir='/tmp') as signals:
        (f.root/'state.txt').write_text('v1')
        ready=Path(signals)/'ready'
        f.start()
        job_group=None
        try:
            code="import json,os,time; from pathlib import Path; Path(%r).write_text(json.dumps({'pid':os.getpid(),'pgid':os.getpgrp()})); time.sleep(10)" % str(ready)
            p=f.client('run','--snapshot','--',sys.executable,'-c',code)
            end=time.monotonic()+6
            while not ready.exists() and time.monotonic()<end:
                assert p.poll() is None, p.communicate()
                time.sleep(.02)
            assert ready.exists(), 'snapshot did not start'
            worker=json.loads(ready.read_text())
            assert worker['pgid'] in (worker['pid'],f.proc.pid), worker
            job_group=worker['pgid']
            assert len(list((f.root/'.avc/snaps').iterdir()))==1
            f.stop(signal.SIGKILL)
            # SIGKILL cannot run daemon cleanup. This test owns and stops the
            # worker explicitly; it makes no claim about orphan-job recovery.
            os.killpg(job_group,signal.SIGKILL)
            job_group=None
            p.communicate(timeout=3)
            assert len(list((f.root/'.avc/snaps').iterdir()))==1
            f.start()
            snaps=f.root/'.avc/snaps'
            assert not snaps.exists() or not list(snaps.iterdir())
            assert f.avc('cat','state.txt').stdout=='v1'
            f.stop()
            RESULTS.append({'test':'snapshot_sigkill_restart_cleanup','correct':True,'remaining_snapshots':0,'limit':'owned orphan worker explicitly terminated by harness before restart'})
        finally:
            if job_group is not None:
                try: os.killpg(job_group,signal.SIGKILL)
                except ProcessLookupError: pass

def snapshot_readonly():
    with fixture() as f:
        (f.root/'state.txt').write_text('v1')
        f.start()
        try:
            code="from pathlib import Path; p=Path('locked'); p.mkdir(); (p/'file').write_text('data'); p.chmod(0o555)"
            f.avc('run','--snapshot','--',sys.executable,'-c',code)
            assert not list((f.root/'.avc/snaps').iterdir())
            f.avc('run','--snapshot','--','true')
            assert not list((f.root/'.avc/snaps').iterdir())
            RESULTS.append({'test':'snapshot_readonly_directory_cleanup','correct':True,'directory_mode':'0555','remaining_snapshots':0})
        finally:
            for path in (f.root/'.avc/snaps').glob('*/locked'):
                path.chmod(0o700)

def snapshot_cleanup_failure():
    if sys.platform!='darwin':
        RESULTS.append({'test':'snapshot_cleanup_failure_fail_closed','skipped':'macOS uchg injection only'})
        return
    with fixture() as f:
        (f.root/'state.txt').write_text('v1')
        f.start()
        try:
            code="from pathlib import Path; import os,stat; p=Path('locked-file'); p.write_text('data'); os.chflags(p,stat.UF_IMMUTABLE)"
            f.avc('run','--snapshot','--',sys.executable,'-c',code,check=False)
            clones=list((f.root/'.avc/snaps').iterdir())
            assert len(clones)==1, clones
            refused=f.avc('run','--snapshot','--','true',check=False)
            assert refused.returncode!=0 and 'disabled until restart' in refused.stderr, (refused.returncode,refused.stdout,refused.stderr)
            assert list((f.root/'.avc/snaps').iterdir())==clones
            f.avc('edit','state.txt','--old','v1','--new','v2')
            assert f.avc('cat','state.txt').stdout=='v2'
            f.stop()
            failed=f.client('daemon','--root',str(f.root))
            out,err=failed.communicate(timeout=5)
            assert failed.returncode!=0 and 'cannot clear old snapshots' in err, (failed.returncode,out,err)
            for path in (f.root/'.avc/snaps').glob('*/locked-file'):
                os.chflags(path,0)
            f.start()
            f.avc('run','--snapshot','--','true')
            assert not list((f.root/'.avc/snaps').iterdir())
            RESULTS.append({'test':'snapshot_cleanup_failure_fail_closed','correct':True,'injection':'macOS UF_IMMUTABLE in owned temporary snapshot','next_snapshot_refused':True,'managed_edit_succeeded':True,'restart_refused_until_cleanup_possible':True})
        finally:
            for path in (f.root/'.avc/snaps').glob('*/locked-file'):
                os.chflags(path,0)

def state_directory_symlink():
    with fixture() as f, tempfile.TemporaryDirectory(prefix='avc-external-',dir='/tmp') as external:
        outside=Path(external)
        snaps=outside/'snaps'
        snaps.mkdir()
        sentinel=snaps/'must-remain.txt'
        sentinel.write_text('outside repository; preserve exactly')
        sentinel.chmod(0o444)
        snaps.chmod(0o555)
        (f.root/'.avc').symlink_to(outside,target_is_directory=True)
        try:
            failed=f.client('daemon','--root',str(f.root))
            out,err=failed.communicate(timeout=5)
            assert failed.returncode!=0, (out,err)
            assert sentinel.read_text()=='outside repository; preserve exactly'
            assert stat.S_IMODE(sentinel.stat().st_mode)==0o444
            assert stat.S_IMODE(snaps.stat().st_mode)==0o555
            assert sorted(p.name for p in outside.iterdir())==['snaps'], 'daemon wrote metadata through symlink'
            (f.root/'.avc').unlink()
            (f.root/'.avc').mkdir(mode=0o700)
            (f.root/'.avc/snaps').symlink_to(snaps,target_is_directory=True)
            failed=f.client('daemon','--root',str(f.root))
            out,err=failed.communicate(timeout=5)
            assert failed.returncode!=0, (out,err)
            assert sentinel.read_text()=='outside repository; preserve exactly'
            assert stat.S_IMODE(sentinel.stat().st_mode)==0o444
            assert stat.S_IMODE(snaps.stat().st_mode)==0o555
            assert sorted(p.name for p in outside.iterdir())==['snaps']
            RESULTS.append({'test':'state_directory_symlink_refused_before_io','correct':True,'paths_tested':['.avc','.avc/snaps'],'external_content_and_modes_preserved':True,'external_metadata_created':False})
        finally:
            if snaps.exists(): snaps.chmod(0o700)

for name in sys.argv[1:] or ['snapshot','crash','load_gate','partial_connections','snapshot_crash','snapshot_readonly','snapshot_cleanup_failure','state_directory_symlink']:
    globals()[name]()
assert hashlib.sha256(AVC.read_bytes()).hexdigest() == AVC_HASH, 'binary changed during acceptance run'
output={'avc_binary_sha256':AVC_HASH, 'timestamp_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()), 'results':RESULTS}
target=REPO/'bench/results/acceptance-smoke.json'
target.parent.mkdir(parents=True,exist_ok=True)
target.write_text(json.dumps(output,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(output,ensure_ascii=False,indent=2))
