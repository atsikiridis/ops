"""Classes for disambiguation scheduling and monitoring."""

from invenio.webinterface_handler import wash_urlargd, WebInterfaceDirectory

from invenio.bibauthorid_config import CFG_BIBAUTHORID_ENABLED, \
    WEDGE_THRESHOLD, AID_VISIBILITY
from invenio.bibauthorid_templates import initialize_jinja2_environment
from invenio.bibauthorid_dbinterface import update_disambiguation_task_status
from invenio.bibauthorid_dbinterface import \
    get_papers_per_disambiguation_cluster
from invenio.bibauthorid_dbinterface import get_status_of_task_by_task_id
from invenio.bibauthorid_dbinterface import add_new_disambiguation_task
from invenio.bibauthorid_dbinterface import get_disambiguation_task_data
from invenio.bibauthorid_dbinterface import get_task_id_by_cluster_name
from invenio.bibauthorid_dbinterface import set_task_start_time
from invenio.bibauthorid_dbinterface import set_task_end_time
from invenio.bibauthorid_dbinterface import TaskNotRegisteredError
from invenio.bibauthorid_dbinterface import TaskNotRunningError
from invenio.bibauthorid_dbinterface import TaskAlreadyRunningError
from invenio.bibauthorid_dbinterface import TaskNotSuccessfulError
from invenio.bibauthorid_dbinterface import register_disambiguation_statistics
from invenio.bibauthorid_dbinterface import get_number_of_profiles
from invenio.bibauthorid_dbinterface import get_disambiguation_task_stats
from invenio.bibauthorid_dbinterface import get_average_ratio_of_claims
from invenio.bibauthorid_dbinterface import \
    get_most_modified_disambiguated_profiles as get_most_modified_profs
from invenio.bibauthorid_dbinterface import get_bibsched_task_id_by_task_name
from invenio.bibauthorid_dbinterface import get_cluster_info_by_task_id
from invenio.bibauthorid_merge import get_matched_claims
from invenio.bibauthorid_merge import get_unmodified_profiles
from invenio.bibauthorid_merge import get_abandoned_profiles
from invenio.bibauthorid_merge import get_new_clusters
from invenio.bibauthorid_merge import \
    merge_dynamic as merge_disambiguation_results

from invenio.bibauthorid_templates import WebProfilePage

from invenio.bibauthorid_webapi import get_person_redirect_link

from invenio.webuser import page_not_authorized, get_session
import invenio.bibauthorid_webapi as web_api

from invenio.bibtask import task_low_level_submission
from invenio.bibsched import bibsched_send_signal
from invenio.config import CFG_SITE_LANG, CFG_BASE_URL
from invenio.webpage import page
from invenio.urlutils import redirect_to_url

from datetime import datetime
from collections import OrderedDict

from signal import SIGTERM


def _schedule_disambiguation(cluster, name_of_user, threshold):
    """
    Runs the tortoise algorithm for the cluster using the wedge
    threshold as a parameter.
    """
    last_names = '--last-names=%s:%s' % (cluster, threshold)
    current_time = datetime.now().strftime('%Y_%m_%d_%I_%M')
    task_name = '_'.join(['disambiguate_author', cluster, current_time])
    params = ('bibauthorid', name_of_user, '--disambiguate',
              '--single-threaded', last_names,
              '--disambiguation-name=%s:%s' % ('bibauthorid', task_name),
              '--name', task_name)

    return task_low_level_submission(*params)


class MonitoredDisambiguation(object):

    def __init__(self, get_task_name):
        self._get_task_name = get_task_name

    def __call__(self, tortoise_func):

        def monitored_tortoise(*args, **kwargs):
            task_name = self._get_task_name()
            try:
                self._task_id = get_bibsched_task_id_by_task_name(task_name)
            except TaskNotRegisteredError:
                return tortoise_func(*args, **kwargs)

            cluster_info = get_cluster_info_by_task_id(self._task_id)
            self._name, self._threshold = cluster_info
            set_task_start_time(self._task_id, datetime.now())

            update_disambiguation_task_status(self._task_id, 'RUNNING')
            try:
                value = tortoise_func(*args, **kwargs)
            except Exception, e:
                set_task_end_time(self._task_id, datetime.now())
                update_disambiguation_task_status(self._task_id, 'FAILED')
                raise e
                # TODO get failure info.
            set_task_end_time(self._task_id, datetime.now())

            statistics = dict()
            statistics['stats'] = self._calculate_stats()
            statistics['rankings'] = self._calculate_rankings()

            register_disambiguation_statistics(self._task_id, statistics)

            update_disambiguation_task_status(self._task_id, 'SUCCEEDED')
            return value

        return monitored_tortoise

    def _calculate_stats(self):
        """
        Gets data for current data in aidRESULTS.
        """
        stats = OrderedDict()
        stats['Task ID'] = self._task_id
        stats['Name'] = self._name
        stats['Wedge Threshold'] = self._threshold

        profiles_before, profiles_after = get_number_of_profiles(self._name)
        stats['Number of Profiles Before'] = profiles_before
        stats['Number of Profiles After'] = profiles_after

        avg_p_title = 'Number of Papers per Disambiguation Cluster'
        papers_per_cluster = get_papers_per_disambiguation_cluster(self._name)
        stats[avg_p_title] = papers_per_cluster

        ratio_claims_title = 'Average Ratio of Claimed and Unclaimed Papers'
        ratio_claims = get_average_ratio_of_claims(self._name)
        stats[ratio_claims_title] = ratio_claims

        preserved_claims, total_claims = get_matched_claims(self._name)
        stats['Number of Preserved Claims'] = preserved_claims
        stats['Number of Total Claims'] = total_claims
        percentage = preserved_claims / float(total_claims) * 100
        stats['Percentage of Preserved Claims'] = '%s %%' % percentage

        return stats

    def _calculate_rankings(self):

        rankings = dict()

        most_mod_title = 'Most modified profiles'
        rankings[most_mod_title] = get_most_modified_profs(self._name)

        rankings['Unmodified Profiles'] = get_unmodified_profiles(self._name)

        rankings['Abandoned Profiles'] = get_abandoned_profiles(self._name)

        rankings['New clusters'] = get_new_clusters(self._name)  # Work, None for now

        return rankings


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
        self._statistics = None

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
            update_disambiguation_task_status(self.task_id, 'KILLED')
        else:
            raise TaskNotRunningError()

    @property
    def running(self):
        try:
            status = get_status_of_task_by_task_id(self.task_id)
        except TaskNotRegisteredError:
            return False

        if status == 'RUNNING':
            return True

        return False

    @property
    def task_id(self):  # TODO Make it a retrieve function for consistency.
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

    def retrieve_statistics(self):  # db interface

        if self._statistics:
            return self._statistics

        try:
            status = get_status_of_task_by_task_id(self.task_id)
        except TaskNotRegisteredError, e:
            raise e

        if not status == 'SUCCEEDED':
            msg = """The task with the id %s has not been completed
                     successfully. Cannot retrieve statistics."""\
                  % self.task_id
            raise TaskNotSuccessfulError(msg)

        self._statistics = get_disambiguation_task_stats(self.task_id)
        return self._statistics


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
        web_api.session_bareinit(req)
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
        web_page = WebProfilePage('dashboard', page_title)

        return page(title=page_title,
                    metaheaderadd=web_page.get_head().encode('utf-8'),
                    body=web_page.get_wrapped_body('dashboard', content),
                    req=req,
                    language=ln,
                    show_title_p=False)


class WebAuthorDisambiguationInfo(WebInterfaceDirectory):

    def __init__(self, bibsched_id=None):
        if not CFG_BIBAUTHORID_ENABLED:
            return
        self._task_id = bibsched_id

    def __call__(self, req, form):
        web_api.session_bareinit(req)
        session = get_session(req)
        if not session['personinfo']['ulevel'] == 'admin':
            msg = "To access the disambiguation facility you should login."
            return page_not_authorized(req, text=msg)
        argd = wash_urlargd(form, {'ln': (str, CFG_SITE_LANG),
                                   'should-merge': (str, None)})

        if self._task_id < 1:
            redirect_to_url(req, CFG_BASE_URL + '/author/dashboard')
        task = DisambiguationTask(task_id=self._task_id)
        try:
            stats = task.retrieve_statistics()
        except (TaskNotSuccessfulError, TaskNotRegisteredError):
            msg = """There is no successfully completed task with a task id
                     of %s.""" % task.task_id
            return page_not_authorized(req, text=msg)

        page_title = "Disambiguation Results for task %s" % task.task_id
        web_page = WebProfilePage('disambiguation', page_title)

        if argd['should-merge']:
            surname = get_cluster_info_by_task_id(self._task_id)[0]
            merge_disambiguation_results(surname)
            update_disambiguation_task_status(self._task_id, 'MERGED')

        merged = get_status_of_task_by_task_id(self._task_id) == 'MERGED'
        content = {'statistics': stats,
                   'merged': merged}

        return page(title=page_title,
                    metaheaderadd=web_page.get_head().encode('utf-8'),
                    body=web_page.get_wrapped_body('disambiguation', content),
                    req=req,
                    language=argd['ln'],
                    show_title_p=False)

    def _lookup(self, component, path):
        """
        Necessary for webstyle in order to be able to use
        /author/disambiguation/1 etc.
        """
        if len(path) == 0:
            return WebAuthorDisambiguationInfo(component), path
        elif len(path) == 1 and component == 'profile':
            return WebAuthorProfileComparision(path[0]), []


class WebAuthorProfileComparision(WebInterfaceDirectory):

    """
    Handles /author/disambiguation/profile/
    Allows admin to check changes in profile after the algorithm was run.
    """

    def __init__(self, person):
        if not CFG_BIBAUTHORID_ENABLED:
            return
        self.person = person

    def __call__(self, req, form):
        web_api.session_bareinit(req)
        session = get_session(req)
        if not session['personinfo']['ulevel'] == 'admin':
            msg = "To access the disambiguation facility you should login."
            return page_not_authorized(req, text=msg)

        argd = wash_urlargd(form, {'ln': (str, CFG_SITE_LANG)})

        full_name = get_person_redirect_link(int(self.person))

        page_title = "Disambiguation profile comparision for %s" % full_name
        web_page = WebProfilePage('profile_page', page_title)

        content = {
            'extended_template' : 'profile_comparision.html',
            'visible' : AID_VISIBILITY,
            'person_id': self.person,
            'element_width' : {
                'publication_box' : '12',
                'coauthors' : '12',
                'papers' : '12',
                'subject_categories' : '12',
                'frequent_keywords' : '12'
            }
        }

        return page(title=page_title,
                    metaheaderadd=web_page.get_head().encode('utf-8'),
                    body=web_page.get_wrapped_body('profile_page', content),
                    req=req,
                    language=argd['ln'],
                    show_title_p=False)
