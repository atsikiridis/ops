"""Classes for disambiguation scheduling and monitoring."""

from invenio.webinterface_handler import wash_urlargd, WebInterfaceDirectory

from invenio.bibauthorid_config import CFG_BIBAUTHORID_ENABLED, WEDGE_THRESHOLD
from invenio.bibauthorid_templates import initialize_jinja2_environment
from invenio.bibauthorid_templates import load_named_template
from invenio.bibauthorid_dbinterface import update_disambiguation_task_status
from invenio.bibauthorid_dbinterface import get_status_of_task_by_task_id
from invenio.bibauthorid_dbinterface import get_task_id_by_cluster_name
from invenio.bibauthorid_dbinterface import add_new_disambiguation_task


from invenio.bibtask import task_low_level_submission
from invenio.bibsched import bibsched_send_signal
from invenio.shellutils import run_shell_command
from invenio.config import CFG_SITE_LANG, CFG_BASE_URL
from invenio.webpage import page

from signal import SIGTERM


def _schedule_disambiguation(cluster, name_of_user, threshold):
    '''
    Runs the tortoise algorirthm for the cluster using the wedge
    threshold as a parameter.
    '''
    last_names = '--last-names=%s:%s' % (cluster, threshold)
    params = ('bibauthorid', name_of_user, '--disambiguate', last_names)
    return task_low_level_submission(*params)


def _run_disambiguation(task_id):
    run_shell_command('/opt/invenio/bin/bibauthorid %s &', (str(task_id),))  # Something nicer here...


class DisambiguationTask(object):
    '''
    A scheduled task to run disambiguation on a particular cluster
    (surname).
    '''

    def __init__(self, cluster=None, name_of_user='admin',
                 threshold=WEDGE_THRESHOLD, task_id=None):
        self._cluster = cluster
        self._threshold = threshold
        self._name_of_user = name_of_user
        self.__task_id = None

    def schedule_and_run(self):
        self.schedule()
        self.run()

    def schedule(self):
        '''
        Schedules a new disambiguation for a specified cluster after
        logging it to aidDISAMBIGUATIONLOG.
        '''
        if self._running:
            msg = "A disambiguation for %s is already running." % self._cluster
            raise TaskAlreadyRunningError(msg)
        self._task_id = _schedule_disambiguation(self._cluster,
                                                 self._name_of_user,
                                                 self._threshold)

    def run(self):
        _run_disambiguation(self._task_id)
        update_disambiguation_task_status(self._task_id, 'RUNNING')

    def kill(self):
        if self._running:
            bibsched_send_signal(self._task_id, SIGTERM)
        else:
            raise TaskNotRunningError()

    @property
    def _running(self):
        try:
            status = get_status_of_task_by_task_id(self._task_id)
        except TaskNotRegisteredError:
            return False

        if status == 'RUNNING':
            return True

        return False

    @property
    def _task_id(self):
        if self.__task_id:
            return self.__task_id
            
        task_id_in_db = get_task_id_by_cluster_name(self._cluster,
                                                    running=True)
        if task_id_in_db:
            self.__task_id = task_id_in_db
            return self.__task_id

        msg = """There is no running disambiguation task for cluster:
            %s.""" % self._cluster
        raise TaskNotRegisteredError(msg)

    @_task_id.setter
    def _task_id(self, _task_id):
        self.__task_id = _task_id
        add_new_disambiguation_task(self.__task_id, self._cluster,
                                    self._name_of_user, self._threshold)


class TaskNotRegisteredError(Exception):
    '''
    To be raised when a task has not been registered to the
    aidDISAMBIGUATIONLOG table.
    '''
    pass


class TaskAlreadyRunningError(Exception):
    '''
    To be raised when task is already running.
    '''
    pass


class TaskNotRunningError(Exception):
    '''
    To be raised when task is not running.
    '''
    pass


class WebAuthorDashboard(WebInterfaceDirectory):
    '''
    Handles /author/dashboard/ a web interface for administrators to run
    disambiguation.
    '''

    _environment = initialize_jinja2_environment()

    def __init__(self):
        if not CFG_BIBAUTHORID_ENABLED:
            return

    def __call__(self, req, form):
        print req
        argd = wash_urlargd(form, {'ln': (str, CFG_SITE_LANG),
                                   'surname': (str, None),
                                   'threshold': (str, None)})

        surname = argd['surname']  # TODO Generalise for many / all clusters
        if surname:
            self._run_disambiguation([(surname, argd['threshold'])])

        ln = argd['ln']

        running = get_disambiguation_tasks(status='RUNNING')
        completed = get_disambiguation_tasks(status='COMPLETED')
        failed = get_disambiguation_tasks(status='FAILED')

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

    def _run_disambiguation(self, surnames_thresholds):
        """
        Runs the disambiguation daemon for the specified clusters via bibsched.
        If no wedge threshold is defined for a cluster,
        the default one is being used.
        """
        for surname, threshold in surnames_thresholds:  # TODO Do we really need multiple ?
            task = DisambiguationTask(surname, threshold=threshold)  # TODO add name.
            task.schedule_and_run()