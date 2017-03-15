import subprocess as sp
import sys
import re
import os
import time

from .DRM_Base import DRM

decode_lsf_state = dict([
    ('UNKWN', 'process status cannot be determined'),
    ('PEND', 'job is queued and active'),
    ('PSUSP', 'job suspended while pending'),
    ('RUN', 'job is running'),
    ('SSUSP', 'job is system suspended'),
    ('USUSP', 'job is user suspended'),
    ('DONE', 'job finished normally'),
    ('EXIT', 'job finished, but failed'),
])

class BSubException(Exception):
    pass

class BSubJobNotFound(BSubException):
    pass

class BJobsNotFound(Exception):
    pass

class DRM_LSF(DRM):
    name = 'lsf'
    poll_interval = 5

    def submit_job(self, task):
        ns = ' ' + task.drm_native_specification if task.drm_native_specification else ''
        bsub = 'bsub -o {stdout} -e {stderr}{ns} '.format(stdout=task.output_stdout_path,
                                                          stderr=task.output_stderr_path,
                                                          ns=ns)

        cmd = '{bsub} "{cmd_str}"'.format(cmd_str=task.output_command_script_path, bsub=bsub)
        task.log.info("BSUB CMD: {}".format(cmd))
        out = self._bsub(cmd)
        drm_jobID = self._get_job_id(out)
        task.log.info("BSUB JOB ID: {}".format(drm_jobID))
        time.sleep(2)
        return drm_jobID

    def filter_is_done(self, tasks):
        if len(tasks):
            bjobs = bjobs_all()

            def _update_bjobs(jid, status):
                if jid not in bjobs: bjobs[jid] = {}
                bjobs[jid]['STAT'] = status

            def _bmetrica_check(task, jid):
                from bmetrica.jobstats import JobStats
                js = JobStats()
                metrics = js.get_metrics([jid])
                if not metrics:
                    msg = ("Could not find historical metrics "
                           "for LSF job id: {}").format(jid)
                    task.log.warning(msg)
                    raise BJobsNotFound(msg)
                status = metrics[0]['stat']
                is_done = status in [ 'DONE', 'EXIT', 'UNKWN', 'ZOMBI' ]
                js.connection.close()
                return (is_done, status)

            def is_done(task):
                jid = str(task.drm_jobID)
                if jid not in bjobs:
                    # prob in history
                    # print 'missing %s %s' % (task, task.drm_jobID)
                    if 'BMETRICA_DSN' in os.environ:
                        msg = ('No recent bjobs stats for {}. '
                               'Accessing bmetrica.').format(jid)
                        task.log.info(msg)
                        (is_done, status) = _bmetrica_check(task, jid)
                        _update_bjobs(jid, status)
                        return is_done
                    else:
                        msg = ("Found no status metrics "
                               "for LSF job id: {}").format(jid)
                        task.log.warning(msg)
                        raise BJobsNotFound(msg)
                else:
                    return bjobs[jid]['STAT'] in ['DONE', 'EXIT', 'UNKWN', 'ZOMBI']

            def job_stats(task):
                jid = str(task.drm_jobID)
                status = bjobs[jid]['STAT']
                successful = True if status == 'DONE' else False
                noop = True if status == 'DONE' else False
                stats = { '_status' : status, 'successful' : successful, 'NOOP' : noop }
                return stats

            done_tasks = filter(is_done, tasks)
            done_stats = [ job_stats(t) for t in done_tasks ]
            return zip(done_tasks, done_stats)
        else:
            return ([], {})

    def drm_statuses(self, tasks):
        """
        :param tasks: tasks that have been submitted to the job manager
        :returns: (dict) task.drm_jobID -> drm_status
        """
        if len(tasks):
            bjobs = bjobs_all()

            def _bmetrica(task):
                jid = str(task.drm_jobID)

                from bmetrica.jobstats import JobStats
                js = JobStats()
                metrics = js.get_metrics([jid])
                js.connection.close()
                if not metrics:
                    msg = ("Could not find historical metrics "
                           "for LSF job id: {}").format(jid)
                    task.log.warning(msg)
                    return '???'
                status = metrics[0]['stat']
                return status

            def f(task):
                status = bjobs.get(str(task.drm_jobID), dict()).get('STAT', '???')
                if status == '???' and ('BMETRICA_DSN' in os.environ):
                    status = _bmetrica(task)
                return status

            return {task.drm_jobID: f(task) for task in tasks}
        else:
            return {}

    def kill(self, task):
        "Terminates a task"
        raise NotImplementedError
        # os.system('bkill {0}'.format(task.drm_jobID))

    def kill_tasks(self, tasks):
        for t in tasks:
            sp.check_call(['bkill', str(t.drm_jobID)])

    def _bsub(self, command, check_str="is submitted"):
        p = sp.Popen(command, shell=True,
                              stdout=sp.PIPE,
                              stderr=sp.PIPE,
                              env=os.environ,
                              preexec_fn=preexec_function())
        p.wait()

        res = p.stdout.read().strip().decode("utf-8", "replace")
        err = p.stderr.read().strip().decode("utf-8", "replace")

        if p.returncode == 255:
            raise BSubJobNotFound("{} : {}".format(command, err))
        elif p.returncode != 0:
            if (res): sys.stderr.write(res)
            if (err): sys.stderr.write(err)
            raise BSubException(command + "[" + str(p.returncode) + "]")

        if not (check_str in res and p.returncode == 0):
            raise BSubException(err)
        return res

    def _get_job_id(self, result_string):
        # parse the 'Job <(\d+)>' string
        job_id = result_string.split("<", 1)[1].split(">", 1)[0]
        return job_id


def bjobs_all():
    """
    returns a dict keyed by lsf job ids, who's values are a dict of bjob
    information about the job
    """
    try:
        lines = sp.check_output(['bjobs', '-a']).split('\n')
    except (sp.CalledProcessError, OSError):
        return {}
    bjobs = {}
    header = re.split("\s+", lines[0])
    for l in lines[1:]:
        items = re.split("\s+", l)
        bjobs[items[0]] = dict(zip(header, items))
    return bjobs


def preexec_function():
    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.  This allows Cosmos to cleanly
    # terminate jobs when there is a ctrl+c event
    os.setpgrp()
