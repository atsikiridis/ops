"""Classes for disambiguation scheduling and monitoring."""

from collections import OrderedDict
from datetime import datetime
from signal import SIGTERM

from invenio.bibauthorid_config import CFG_BIBAUTHORID_ENABLED, \
    WEDGE_THRESHOLD, AID_VISIBILITY

from invenio.bibauthorid_dbinterface import update_disambiguation_task_status
from invenio.bibauthorid_dbinterface import \
    get_papers_per_disambiguation_cluster
from invenio.bibauthorid_dbinterface import get_papers_of_author
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
from invenio.bibauthorid_dbinterface import get_profiles_with_changes
from invenio.bibauthorid_dbinterface import get_bibsched_task_id_by_task_name
from invenio.bibauthorid_dbinterface import get_cluster_info_by_task_id
from invenio.bibauthorid_dbinterface import get_clusters
from invenio.bibauthorid_dbinterface import get_last_name_by_pid
from invenio.bibauthorid_dbinterface import get_name_from_bibref
from invenio.bibauthorid_dbinterface import get_authors_of_paper
from invenio.bibauthorid_dbinterface import get_pid_to_canonical_name_map
from invenio.bibauthorid_dbinterface import get_title_of_paper
from invenio.bibauthorid_dbinterface import get_collaborations_for_paper
from invenio.bibauthorid_dbinterface import get_coauthors_from_paperrecs

from invenio.bibauthorid_general_utils import memoized

from invenio.bibauthorid_merge import get_matched_claims
from invenio.bibauthorid_merge import get_abandoned_profiles
from invenio.bibauthorid_merge import \
    merge_dynamic as merge_disambiguation_results
from invenio.bibauthorid_merge import get_matched_clusters

from invenio.bibauthorid_templates import initialize_jinja2_environment
from invenio.bibauthorid_templates import WebProfilePage

import invenio.bibauthorid_webapi as web_api

from invenio.bibformat import format_records

from invenio.bibsched import bibsched_send_signal

from invenio.bibtask import task_low_level_submission

from invenio.config import CFG_SITE_LANG
from invenio.config import CFG_BASE_URL

from invenio.intbitset import intbitset

from invenio.search_engine_summarizer import generate_citation_summary

from invenio.urlutils import redirect_to_url

from invenio.webauthorprofile_corefunctions import _get_institute_pubs_dict
from invenio.webauthorprofile_corefunctions import _get_collabtuples_bai
from invenio.webauthorprofile_corefunctions import _get_pubs_per_year_dictionary
from invenio.webauthorprofile_corefunctions import _get_kwtuples_bai
from invenio.webauthorprofile_corefunctions import _get_fieldtuples_bai_tup

from invenio.webauthorprofile_config import deserialize

from invenio.webinterface_handler import wash_urlargd, WebInterfaceDirectory
import invenio.webinterface_handler_config as apache

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
        stats[avg_p_title] = round(papers_per_cluster, 2)

        ratio_claims_title = 'Average Ratio of Claimed and Unclaimed Papers'
        ratio_claims = get_average_ratio_of_claims(self._name)
        stats[ratio_claims_title] = round(ratio_claims, 2)

        preserved_claims, total_claims = get_matched_claims(self._name)
        stats['Number of Preserved Claims'] = preserved_claims
        stats['Number of Total Claims'] = total_claims
        try:
            percentage = preserved_claims / float(total_claims) * 100
        except:
            percentage = 100
        stats['Percentage of Preserved Claims'] = '%s %%' % percentage

        return stats

    def _calculate_rankings(self):

        task_name = self._task_id
        rankings = dict()

        all_modified_profiles = get_profiles_with_changes(self._name)

        abandoned_profiles = get_abandoned_profiles(self._name)

        all_clusters = get_matched_clusters(self._name)

        for name, changes in all_modified_profiles:

            clusters = get_disambiguation_matching(name, task_name)

            pid = ''
            bibrefs = []
            papers = []

            first_cluster = clusters[0]
            for _bibrefs, _pid in all_clusters:
                if not pid:
                    for bibref in _bibrefs:
                        if first_cluster == bibref:
                            pid = _pid
                            bibrefs = _bibrefs
                            break

            truly_changed = added = changes
            bibrecs = {(x,) for _, _, x in bibrefs}
            papers = get_papers_of_author(pid)
            removed = 0

            if pid:
                if bibrefs:
                    removed = len(papers.difference(bibrecs))
                    added = len(bibrecs.difference(papers))
                    truly_changed = removed + added

            if pid == '':
                status = 'new cluster'
            elif truly_changed == 0:
                status = 'unmodified'
            else:
                status = 'modified'

            rankings[(name, pid)] = {
                'status' : status,
                'removed' : removed,
                'added' : added,
                'cname' : web_api.get_person_redirect_link(pid) if pid else '',
                'after_disambiguation' : len(bibrecs),
                'before_disambiguation' :  len(papers)
            }

        for profile in abandoned_profiles:
            papers = get_papers_of_author(profile)
            rankings[('', profile)] = {
                'status' : 'abandoned',
                #All papers were removed from this poor guy
                'removed': len(papers),
                'added' : 0,
                'cname' : web_api.get_person_redirect_link(profile),
                'after_disambiguation' : 0,
                'before_disambiguation' : len(papers)
            }

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
            task = DisambiguationTask(cluster=surname.lower(),
                                      threshold=threshold,
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

    """Handles /author/disambiguation/(task_id) - an overwiew of a tortoise
    results.
    """

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

        disambiguate_suburl = \
                '/author/disambiguation/%s/profile/' % self._task_id

        content = {'statistics': stats,
                   'merged': merged,
                   'fake_profile_base_url' : CFG_BASE_URL + disambiguate_suburl
                  }

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

        if not CFG_BIBAUTHORID_ENABLED:
            raise apache.SERVER_RETURN(apache.HTTP_NOT_FOUND)

        if len(path) == 0:
            return WebAuthorDisambiguationInfo(component), path
        elif len(path) == 2 and path[0] == 'profile':
            #in form of /author/disambiguation/(task_id)/profile/(pid)
            return WebAuthorProfileComparison(path[1], component), []
        else:
            raise apache.SERVER_RETURN(apache.HTTP_NOT_FOUND)


class WebAuthorProfileComparison(WebInterfaceDirectory):

    """Handles /author/disambiguation/profile/
    Allows admin to check changes in profile after the algorithm was run.
    """

    def __init__(self, person, task_id):
        if not CFG_BIBAUTHORID_ENABLED:
            return
        self.person = person
        self.task_id = task_id

    def __call__(self, req, form):
        web_api.session_bareinit(req)
        session = get_session(req)
        if not session['personinfo']['ulevel'] == 'admin':
            msg = "To access the disambiguation facility you should login."
            return page_not_authorized(req, text=msg)

        argd = wash_urlargd(form, {'ln': (str, CFG_SITE_LANG)})

        content = {
            'extended_template' : 'profile_comparison.html',
            'visible' : AID_VISIBILITY,
            'person_id': self.person,
            'element_width' : self._get_full_width(),
            'fake' : True,
            'task_id': self.task_id
        }

        try:
            int_person = int(self.person)
            full_name = web_api.get_person_redirect_link(int_person)
            #The result is not used here, but it will be used in every box
            #rendering. Here it is used to decide if the new profile exists.
            computed_matching = get_disambiguation_matching(int_person,
                                                            int(self.task_id))
        except ValueError:
            #TO DO
            full_name = self.person
            computed_matching = get_disambiguation_matching(self.person,
                                                            int(self.task_id))
            content['no_old'] = True

        page_title = "Disambiguation profile comparison for %s" % full_name
        web_page = WebProfilePage('fake_profile', page_title)


        if len(computed_matching) == 0:
            content['no_new'] = True

        return page(title=page_title,
                    metaheaderadd=web_page.get_head().encode('utf-8'),
                    body=web_page.get_wrapped_body('fake_profile', content),
                    req=req,
                    language=argd['ln'],
                    show_title_p=False)

    @classmethod
    def _get_full_width(cls):

        """Get dictionary for profile page template which widens all boxes as
        much as possible.
        """

        return {
            'publication_box' : '12',
            'coauthors' : '12',
            'papers' : '12',
            'subject_categories' : '12',
            'frequent_keywords' : '12'
        }

#Memoization is needed as we are calling get_disambiguation_matching every time
#we render a box.

#We need also to pass task_id to memoizer. If not, a curator might see a
#fake profile from the previous algorithm run.

def no_m_disambiguation_matching(person, task_id):
    return _get_disambiguation_matching(person)

get_disambiguation_matching = \
        memoized(no_m_disambiguation_matching)

def _get_disambiguation_matching(person):

    """Method returning the signatures matched with this personid. Accepts
    both 'real' pids ( from aidPERSONIDPAPERS ) and 'fake' pids
    (from aidRESULTS).
    :param personid: int if 'real' pid, str if 'fake' pid (e.g. ellis.0)
    :return: a list of signatures that are being matched
    """

    try:
        name = get_last_name_by_pid(int(person))
    except AttributeError:
        raise StandardError("There is no matchable name for pid=%s" % person)
    except ValueError:
        return get_clusters(personid=False, cluster=person)

    clusters = get_matched_clusters(name)

    result = [sigs for sigs, pid in clusters if pid == int(person)]

    if not len(result):
        return []

    return result[0]

class FakeProfile(WebInterfaceDirectory):

    """Holder for functions which provide generating of fake profile's boxes.
    """

    @classmethod
    def get_coauthors(cls, person_id, task_id):

        """Fetches coauthor data from aidRESULTS table.
        """

        pubs = cls._get_pubs_from_matching(person_id, task_id)

        exclude_recs = []
        for pub in pubs:
            if get_collaborations_for_paper(pub):
                exclude_recs.append(pub)

        personids = get_coauthors_from_paperrecs(person_id, pubs, exclude_recs,
                                                 exclude_author=
                                                 isinstance(person_id, int))

        coauthors = []

        cname_map = get_pid_to_canonical_name_map()

        for person in personids:
            try:
                cname = cname_map[person[0]]
            except KeyError:
                cname = str(person[0])
            lname = str(cname)
            paps = person[1]
            if paps:
                coauthors.append((cname, lname, paps))
        return coauthors, True

    @classmethod
    def get_collabtuples(cls, person_id, task_id):

        """Fetches collabtuples from aidRESULTS table.
        """

        recids = cls._get_pubs_from_matching(person_id, task_id)

        tup = _get_collabtuples_bai(recids, person_id)

        return tup, True

    @classmethod
    def get_fieldtuples(cls, person_id, task_id):

        """Fetches fieldtuples from aidRESULTS table.
        """

        pubs = cls._get_pubs_from_matching(person_id, task_id)

        return _get_fieldtuples_bai_tup(pubs, person_id), True

    @classmethod
    def get_summarize_records(cls, person_id, task_id):

        """Fetches citations summarision from aidRESULTS table.
        """

        pubs = cls._get_pubs_from_matching(person_id, task_id)

        citation_summary = generate_citation_summary(intbitset(pubs))

        for i in citation_summary[0].keys():
            citation_summary[0][i] = list(citation_summary[0][i])

        return ((citation_summary,), True)

    @classmethod
    def get_institute_pubs(cls, person_id, task_id):

        """Fetches institute pubs from aidRESULTS table."""

        namesdict, status = cls.get_person_names_dicts(person_id, task_id)
        if not status:
            return None, False

        recids = cls._get_pubs_from_matching(person_id, task_id)

        result = _get_institute_pubs_dict(recids, namesdict)

        return result, True

    @classmethod
    def get_internal_publications(cls, person_id, task_id):

        """Fetches internal publications from aidRESULTS table."""

        matching = get_disambiguation_matching(person_id, task_id)
        result = {}

        if matching:
            for paper in matching:
                int_paper = int(paper[2])
                result[int_paper] = get_title_of_paper(int_paper)

        return result, True

    @classmethod
    def get_kwtuples(cls, person_id, task_id):

        """Fetches kwtuples from aidRESULTS table."""

        pubs = cls._get_pubs_from_matching(person_id, task_id)

        return _get_kwtuples_bai(pubs, person_id), True

    @classmethod
    def get_pubs(cls, person_id, task_id):

        """Fetches publications from aidRESULTS table."""

        return cls._get_pubs_from_matching(person_id, task_id), True

    @classmethod
    def get_pubs_per_year(cls, person_id, task_id):

        """Fetches a dictionary from aidRESULTS table
        with papers publications dates.
        """

        recids = cls._get_pubs_from_matching(person_id, task_id)

        formatted_recids = format_records(recids, 'WAPDAT')
        formatted_recids = [deserialize(p) for p in \
                formatted_recids.strip().split('!---THEDELIMITER---!') if p]


        return _get_pubs_per_year_dictionary(formatted_recids), True

    @classmethod
    def get_person_names_dicts(cls, person_id, task_id):

        """Fetches a dictionary with person names from aidRESULTS table."""

        matching = get_disambiguation_matching(person_id, task_id)

        result = {'db_names_dict': {}, 'names_dict': {},
                  'names_to_records': {}
                 }
        helper_dict = {}

        for (bibref_table, bibref_value, bibrec) in matching:
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
    def get_selfpubs(cls, person_id, task_id):

        """Fetches publications written only by this author and by nobody
        else from aidRESULTS table.
        """

        recids = cls._get_pubs_from_matching(person_id, task_id)

        return [pap for pap in recids if cls._is_selfpub(pap)], True

    @classmethod
    def _get_pubs_from_matching(cls, person_id, task_id):

        """Fetches publications from given matching. The matching is a result
        of merge algorithm simulation.
        """

        try:
            matching = get_disambiguation_matching(person_id, task_id)
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
