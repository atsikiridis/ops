from invenio.bibauthorid_name_utils import generate_last_name_cluster_str_old
from invenio.bibauthorid_name_utils import generate_last_name_cluster_str
from invenio.dbquery import run_sql



names = [element[0] for element  in run_sql('select distinct name from aidPERSONIDPAPERS')]

buckets = dict()
new_buckets = dict()
test_file = open('bucketing_evaluation', 'w')
for name in names:

    cluster = generate_last_name_cluster_str_old(name)

    new_cluster = generate_last_name_cluster_str(name)



    try:
        buckets[cluster].append(name)
    except KeyError:
        buckets[cluster] = list()
        buckets[cluster].append(name)

    try:
        new_buckets[new_cluster].append(name)
    except KeyError:
        new_buckets[new_cluster] = list()
        new_buckets[new_cluster].append(name)

    #if cluster != new_cluster:
        #print '    ||   '.join([name, m_name, cluster, new_cluster])
        #print sum_of_surname, index_of_surnames


list_of_items = dict( new_buckets.items() + buckets.items() ).items()
ascending = sorted(list_of_items, key = lambda x: x[0])
ascending_2 = sorted(ascending, key = lambda x: len(x[1]), reverse=True)




test_file.write("{: >50} {: >10} {: >10}".format("generate_last_name_cluster_str (old)" , '-->' , str(len(buckets.keys())) + '\n\n'))
test_file.write("{: >50} {: >10} {: >10}".format("create_matchable_name last_name only (new)" , '-->' , str(len(new_buckets.keys())) + '\n\n'))
test_file.write("{: >50} {: >10} {: >10}".format("name" , '-->' , "old / new " + "\n\n"))


for key, _ in ascending_2:
    try:
        test_file.write("{: >50} {: >10} {: >10} {: >50}".format(key, '-->' ,str(len(buckets[key])) + ',' + str(len(new_buckets[key])) ,str([elem for elem in buckets[key] if elem not in new_buckets[key]  ])  + ' ' +  str([elem for elem in new_buckets[key] if elem not in buckets[key]  ]) + '\n'))
    except KeyError:
        if key in buckets:
            test_file.write("{: >50} {: >10} {: >10}".format(key , '-->' , str(len(buckets[key])) + ', NO'  + '\n'))
        else:
            test_file.write("{: >50} {: >10} {: >10}".format(key , '-->' , 'NO, ' + str(len(new_buckets[key]))   + '\n'))

test_file.close()

