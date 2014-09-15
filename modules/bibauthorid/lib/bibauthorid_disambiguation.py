"""Classes for disambiguation scheduling and monitoring."""

from collections import OrderedDict
from datetime import datetime
from signal import SIGTERM

from invenio.bibauthorid_config import CFG_BIBAUTHORID_ENABLED, \
    WEDGE_THRESHOLD, AID_VISIBILITY

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
from invenio.bibauthorid_dbinterface import get_clusters
from invenio.bibauthorid_dbinterface import get_matchable_name_by_pid
from invenio.bibauthorid_dbinterface import get_name_from_bibref
from invenio.bibauthorid_dbinterface import get_authors_of_paper
from invenio.bibauthorid_dbinterface import get_pid_to_canonical_name_map
from invenio.bibauthorid_dbinterface import get_title_of_paper

from invenio.bibauthorid_merge import get_matched_claims
from invenio.bibauthorid_merge import get_unmodified_profiles
from invenio.bibauthorid_merge import get_abandoned_profiles
from invenio.bibauthorid_merge import get_new_clusters
from invenio.bibauthorid_merge import \
    merge_dynamic as merge_disambiguation_results
from invenio.bibauthorid_merge import get_matched_clusters

from invenio.bibauthorid_templates import initialize_jinja2_environment
from invenio.bibauthorid_templates import WebProfilePage

import invenio.bibauthorid_webapi as web_api

from invenio.bibformat import format_records

from invenio.bibsched import bibsched_send_signal

from invenio.bibtask import task_low_level_submission

from invenio.config import CFG_SITE_LANG, CFG_BASE_URL

from invenio.intbitset import intbitset

from invenio.search_engine_summarizer import generate_citation_summary

from invenio.urlutils import redirect_to_url

from invenio.webauthorprofile_corefunctions import _get_institute_pubs_dict
from invenio.webauthorprofile_corefunctions import _get_collabtuples_bai
from invenio.webauthorprofile_corefunctions import _get_pubs_per_year_dictionary
from invenio.webauthorprofile_corefunctions import _get_kwtuples_bai
from invenio.webauthorprofile_corefunctions import _get_fieldtuples_bai_tup
from invenio.webauthorprofile_corefunctions import _get_summarize_records

from invenio.webauthorprofile_config import deserialize

from invenio.webinterface_handler import wash_urlargd, WebInterfaceDirectory

from invenio.webpage import page

from invenio.webuser import page_not_authorized
from invenio.webuser import get_session

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

        """Gets data for current data in aidRESULTS."""

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

    """A scheduled task to run disambiguation on a particular cluster
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
        """Schedules a new disambiguation for a specified cluster after
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

    """Handles /author/dashboard/ a web interface for administrators to run
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
            return WebAuthorProfileComparison(path[0]), []


class WebAuthorProfileComparison(WebInterfaceDirectory):

    """Handles /author/disambiguation/profile/
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

        full_name = web_api.get_person_redirect_link(int(self.person))

        page_title = "Disambiguation profile comparison for %s" % full_name
        web_page = WebProfilePage('fake_profile', page_title)

        content = {
            'extended_template' : 'profile_comparison.html',
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
                    body=web_page.get_wrapped_body('fake_profile', content),
                    req=req,
                    language=argd['ln'],
                    show_title_p=False)


def _get_disambiguation_matching(person):

    """Method returning the signatures matched with this personid. Accepts
    both 'real' pids ( from aidPERSONIDPAPERS ) and 'fake' pids
    (from aidRESULTS).
    :param personid: int if 'real' pid, str if 'fake' pid (e.g. ellis.0)
    :return: a list of signatures that are being matched
    """

    try:
        name = get_matchable_name_by_pid(int(person)).split(',', 1)[0]
        #To do: remove after fix
        name = 'cheng'
    except AttributeError:
        raise StandardError("There is no matchable name for pid=%s"
                            % person)
    except ValueError:
        return get_clusters(personid=False, cluster=person)

    clusters = get_matched_clusters(name)
    return [sigs for sigs, pid in clusters if pid == person]


class FakeProfile(WebInterfaceDirectory):

    """Holder for functions which provide generating of fake profile's boxes.
    """

    @classmethod
    def get_coauthors(cls, person_id):

        """Fetches coauthor data from aidRESULTS table.
        """

        names = cls.get_person_names_dicts(person_id)[0]['db_names_dict']

        personids = {}
        matching = cls._get_pubs_from_matching(person_id)
        for match in matching:
            authors = get_authors_of_paper(match)
            for author in authors:
                if author in names:
                    continue
                if author in personids:
                    personids[author] += 1
                else:
                    personids[author] = 1

        coauthors = []

        cname_map = get_pid_to_canonical_name_map()

        for person in personids:
            try:
                cname = cname_map[person[0]]
            except KeyError:
                cname = str(person)
            lname = str(cname)
            paps = personids[person]
            if paps:
                coauthors.append((cname, lname, paps))
        return coauthors, True

    @classmethod
    def get_collabtuples(cls, person_id):

        """Fetches collabtuples from aidRESULTS table.
        """

        recids = cls._get_pubs_from_matching(person_id)

        tup = _get_collabtuples_bai(recids, person_id)

        return tup, True

    @classmethod
    def get_fieldtuples(cls, person_id):

        """Fetches fieldtuples from aidRESULTS table.
        """

        pubs = cls._get_pubs_from_matching(person_id)

        return _get_fieldtuples_bai_tup(pubs, person_id), True

    @classmethod
    def get_summarize_records(cls, person_id):

        """Fetches citations summarision from aidRESULTS table.
        """

        pubs = cls._get_pubs_from_matching(person_id)

        citation_summary = generate_citation_summary(intbitset(pubs))

        for i in citation_summary[0].keys():
            citation_summary[0][i] = list(citation_summary[0][i])

        return ((citation_summary,), True)

    @classmethod
    def get_institute_pubs(cls, person_id):

        """Fetches institute pubs from aidRESULTS table."""

        namesdict, status = cls.get_person_names_dicts(person_id)
        if not status:
            return None, False

        recids = cls._get_pubs_from_matching(person_id)

        result = _get_institute_pubs_dict(recids, namesdict)

        return result, True

    @classmethod
    def get_internal_publications(cls, person_id):

        """Fetches internal publications from aidRESULTS table."""

        matching = _get_disambiguation_matching(person_id)
        result = {}

        for paper in matching[0]:
            int_paper = int(paper[2])
            result[int_paper] = get_title_of_paper(int_paper)

        return result, True

    @classmethod
    def get_kwtuples(cls, person_id):

        """Fetches kwtuples from aidRESULTS table."""

        pubs = cls._get_pubs_from_matching(person_id)

        return _get_kwtuples_bai(pubs, person_id), True

    @classmethod
    def get_pubs(cls, person_id):

        """Fetches publications from aidRESULTS table."""

        return cls._get_pubs_from_matching(person_id), True

    @classmethod
    def get_pubs_per_year(cls, person_id):

        """Fetches a dictionary from aidRESULTS table 
        with papers publications dates.
        """

        recids = cls._get_pubs_from_matching(person_id)

        formatted_recids = format_records(recids, 'WAPDAT')
        formatted_recids = [deserialize(p) for p in
             formatted_recids.strip().split('!---THEDELIMITER---!') if p]


        return _get_pubs_per_year_dictionary(formatted_recids), True

    @classmethod
    def get_person_names_dicts(cls, person_id):

        """Fetches a dictionary with person names from aidRESULTS table."""

        matching = _get_disambiguation_matching(person_id)

        result = {'db_names_dict': {}, 'names_dict': {},
                  'names_to_records': {}
                 }
        helper_dict = {}

        for tab in matching:
            for (bibref_table, bibref_value, bibrec) in tab:
                if (bibref_table, bibref_value) in helper_dict:
                    helper_dict[(bibref_table, bibref_value)][1].append(bibrec)
                else:
                    bibref = (int(bibref_table), bibref_value, )
                    name = get_name_from_bibref(bibref)
                    helper_dict[(bibref_table, bibref_value)] = \
                            [name, [bibrec]]

        longest_name = ''
        for _, value in helper_dict.iteritems():
            if len(value[0]) > len(longest_name):
                longest_name = value[0]
            if value[0] in result['db_names_dict']:
                result['db_names_dict'][value[0]] += len(value[1])
                result['names_dict'][value[0]] += len(value[1])
                result['names_to_records'][value[0]] += value[1]
            else:
                result['db_names_dict'][value[0]] = len(value[1])
                result['names_dict'][value[0]] = len(value[1])
                result['names_to_records'][value[0]] = value[1]

        result['longest_name'] = longest_name
        return result, True

    @classmethod
    def get_selfpubs(cls, person_id):

        """Fetches publications written only by this author and by nobody
        else from aidRESULTS table.
        """

        recids = cls._get_pubs_from_matching(person_id)

        return [pap for pap in recids if cls._is_selfpub(pap)], True

    @classmethod
    def _get_pubs_from_matching(cls, person_id):

        """Fetches publications from given matching. The matching is a result
        of merge algorithm simulation.
        """

        try:
            matching = _get_disambiguation_matching(person_id)[0]
        except IndexError:
            #TO DO
            matching = ()

        recids = [x for _, _, x in matching]

        return recids

    @classmethod
    def _is_selfpub(cls, recid):

        """Checks whether a publication doesn't have more authors than the one
        given as an argument.
        """

        authors = get_authors_of_paper(recid)

        return len(authors) == 1
