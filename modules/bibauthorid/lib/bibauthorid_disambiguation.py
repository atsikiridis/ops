"""Classes for disambiguation scheduling and monitoring."""

from invenio.webinterface_handler import wash_urlargd, WebInterfaceDirectory

from invenio.bibauthorid_config import CFG_BIBAUTHORID_ENABLED, WEDGE_THRESHOLD
from invenio.bibauthorid_templates import initialize_jinja2_environment
from invenio.bibauthorid_templates import load_named_template
from invenio.bibtask import task_low_level_submission
from invenio.shellutils import run_shell_command

from invenio.config import CFG_SITE_LANG, CFG_BASE_URL
from invenio.webpage import page


def _run_disambiguation(cluster, name_of_user, threshold):
    '''
    Runs the tortoise algorirthm for the cluster using the wedge
    threshold as a parameter.
    '''
    last_names = '--last-names=%s:%s' % (cluster, threshold)
    params = ('bibauthorid', name_of_user, '--disambiguate', last_names)
    task_id = task_low_level_submission(*params)
    run_shell_command('/opt/invenio/bin/bibauthorid %s &', (str(task_id),))


class Task(object):
    '''
    A scheduled task to run disambiguation on a particular cluster
    (surname).
    '''

    def __init__(self, cluster, name_of_user='admin',
                 threshold=WEDGE_THRESHOLD):
        self._cluster = cluster
        self._threshold = threshold
        self._name_of_user = name_of_user
        self._task_id = get_disambiguation_task_id(self._cluster,
                                                   self._threshold)

    def run(self):
        '''
        Starts a new disambiguation for a specified cluster after
        logging it to aidDISAMBIGUATIONLOG.
        '''
        if self._is_running():
            raise TaskAlreadyRunningException()
        _run_disambiguation(self._cluster, self._name_of_user, self._threshold)

    def kill(self):
        if self._is_running():
            pass # TODO kill
        else:
            raise TaskNotRunningException()

    def _is_running(self):
        status = get_status_of_task(self._task_id)
        if status == 'RUNNING':
            return True
        return False



class TaskAlreadyRunningException(Exception):
    '''
    To be raised when task is already running.
    '''
    pass

class TaskNotRunningException(Exception):
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
        content = {'running_disambiguation_jobs': None,
                   'completed_disambiguation_jobs': None,
                   'failed_disambiguation_jobs': None}

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

    def _run_disambiguation(self, surnames_with_thresholds):
        """
        Runs the disambiguation daemon for the specified clusters via bibsched.
        If no wedge threshold is defined for a cluster,
        the default one is being used.
        """
        pass







