"""Classes for disambiguation scheduling and monitoring."""

from invenio.webinterface_handler import wash_urlargd, WebInterfaceDirectory

from invenio.bibauthorid_config import CFG_BIBAUTHORID_ENABLED, WEDGE_THRESHOLD
from invenio.bibauthorid_templates import initialize_jinja2_environment
from invenio.bibauthorid_templates import load_named_template
from invenio.bibauthorid_dbinterface import update_disambiguation_task_status
from invenio.bibauthorid_dbinterface import get_status_of_task_by_task_id
from invenio.bibauthorid_dbinterface import add_new_disambiguation_task
from invenio.bibauthorid_dbinterface import get_disambiguation_task_data
from invenio.bibauthorid_dbinterface import get_task_id_by_cluster_name
from invenio.bibauthorid_dbinterface import set_task_start_time
from invenio.bibauthorid_dbinterface import set_task_end_time

from invenio.bibauthorid_dbinterface import TaskNotRegisteredError
from invenio.bibauthorid_dbinterface import TaskNotRunningError
from invenio.bibauthorid_dbinterface import TaskAlreadyRunningError

from invenio.webuser import page_not_authorized, get_session
import invenio.bibauthorid_webapi as webapi

from invenio.bibtask import task_low_level_submission
from invenio.bibsched import bibsched_send_signal
from invenio.config import CFG_SITE_LANG
from invenio.webpage import page

from datetime import datetime


from signal import SIGTERM


def _schedule_disambiguation(cluster, name_of_user, threshold):
    """
    Runs the tortoise algorirthm for the cluster using the wedge
    threshold as a parameter.
    """
    last_names = '--last-names=%s:%s' % (cluster, threshold)
    params = ('bibauthorid', name_of_user, '--disambiguate',
              '--single-threaded', '--monitored', last_names)
    return task_low_level_submission(*params)


class MonitoredDisambiguation(object):

    def __init__(self, func):
        self._func = func

    def __call__(self, *args, **kwargs):
        if not ('monitored' in kwargs and kwargs['monitored']):
            del kwargs['monitored']
            return self._func(*args, **kwargs)
        del kwargs['monitored']

        try:
            name = kwargs['last_names_thresholds'].keys()[0]
        except AttributeError:
            name = args[1].keys()[0]

        task_id = get_task_id_by_cluster_name(name, "SCHEDULED")

        set_task_start_time(task_id, datetime.now())
        update_disambiguation_task_status(task_id, 'RUNNING')
        try:
            value = self._func(*args, **kwargs)
        except Exception, e:
            set_task_end_time(task_id, datetime.now())
            update_disambiguation_task_status(task_id, 'FAILED')
            raise e
            # TODO get failure info.
        set_task_end_time(task_id, datetime.now())
        update_disambiguation_task_status(task_id, 'SUCCEEDED')
        return value


class DisambiguationTask(object):
    """
    A scheduled task to run disambiguation on a particular cluster
    (surname).
    """

    def __init__(self, task_id=None, cluster=None, name_of_user='admin',
                 threshold=WEDGE_THRESHOLD):
        try:
            assert task_id or cluster
        except AssertionError:
            raise Exception("A taskid or a surname should be specified.")

        self._task_id = task_id
        self._cluster = cluster
        self._name_of_user = name_of_user
        self._threshold = threshold

    def schedule(self):
        """
        Schedules a new disambiguation for a specified cluster after
        logging it to aidDISAMBIGUATIONLOG.
        """
        if self.running:
            msg = """A disambiguation for %s is already
                     scheduled.""" % self._cluster
            raise TaskAlreadyRunningError(msg)
        self.task_id = _schedule_disambiguation(self._cluster,
                                                self._name_of_user,
                                                self._threshold)

    def kill(self):
        if self.running:
            bibsched_send_signal(int(self.task_id), SIGTERM)
            update_disambiguation_task_status(self.task_id, 'FAILED')  # TODO  add killed status
        else:
            raise TaskNotRunningError()

    @property
    def running(self):
        try:
            status = get_status_of_task_by_task_id(self._task_id)
        except TaskNotRegisteredError:
            return False

        if status == 'RUNNING':
            return True

        return False

    @property
    def task_id(self):
        if self._task_id:
            return self._task_id

        task_id_in_db = get_task_id_by_cluster_name(self._cluster, "RUNNING")
        if task_id_in_db:
            self._task_id = task_id_in_db
            return self._task_id

        msg = """There is no running disambiguation task for cluster:
            %s.""" % self._cluster
        raise TaskNotRegisteredError(msg)

    @task_id.setter
    def task_id(self, task_id):
        self._task_id = task_id
        add_new_disambiguation_task(self._task_id, self._cluster,
                                    self._name_of_user, self._threshold)


class WebAuthorDashboard(WebInterfaceDirectory):
    """
    Handles /author/dashboard/ a web interface for administrators to run
    disambiguation.
    """

    _environment = initialize_jinja2_environment()

    def __init__(self):
        if not CFG_BIBAUTHORID_ENABLED:
            return

    def __call__(self, req, form):
        print form
        webapi.session_bareinit(req)
        session = get_session(req)
        if not session['personinfo']['ulevel'] == 'admin':
            msg = "To access the disambiguation facility you should login."
            return page_not_authorized(req, text=msg)

        argd = wash_urlargd(form, {'ln': (str, CFG_SITE_LANG),
                                   'surname': (str, None),
                                   'threshold': (str, None),
                                   'kill-task-id': (str, False)})

        surname = argd['surname']
        if surname:
            threshold = argd['threshold']
            username = session['user_info']['nickname']
            task = DisambiguationTask(cluster=surname, threshold=threshold,
                                      name_of_user=username)
            task.schedule()

        task_to_kill = argd['kill-task-id']
        if task_to_kill:
            DisambiguationTask(task_id=task_to_kill).kill()

        ln = argd['ln']

        running = get_disambiguation_task_data(status='RUNNING')
        completed = get_disambiguation_task_data(status='SUCCEEDED')
        failed = get_disambiguation_task_data(status='FAILED')

        content = {'running_disambiguation_tasks': running,
                   'completed_disambiguation_tasks': completed,
                   'failed_disambiguation_tasks': failed}

        page_title = "Disambiguation Dashboard"
        loaded_template = load_named_template("dashboard",
                                              WebAuthorDashboard._environment)
        body = loaded_template.render(content)

        return page(title=page_title,
                    metaheaderadd="",
                    body=body,
                    req=req,
                    language=ln,
                    show_title_p=False)