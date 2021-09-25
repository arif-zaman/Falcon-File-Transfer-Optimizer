import os, time,sys
from optparse import OptionParser

import itertools
import numpy as np
import pkg_resources
from itertools import cycle
from operator import add
from scipy.optimize import minimize
from sklearn import linear_model
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import PolynomialFeatures
from sklearn.cluster import MeanShift, estimate_bandwidth

from transfer_experiment import TransferExperiment

discarded_group_counter = 0
maxcc = -1
maximums = []


def parseArguments():
    parser = OptionParser()
    parser.add_option("-f", "--file",
                      action="store", type="string", dest="filename")
    parser.add_option("-p", "--paralelism", action="store", type="int",
                      dest="sample_parallelism", help="Parallelism level used in sample transfer")
    parser.add_option("-c", "--basicClientControlChannel", "--concurrency", action="store", type="int",
                      dest="sample_concurrency", help="Concurrency level used in sample transfer")
    parser.add_option("-q", "--pipelining", "--ppq", action="store", type="int",
                      dest="sample_pipelining", help="Pipelining level used in sample transfer")
    parser.add_option("-t", "--throughput", action="store", type="float",
                      dest="sample_throughput", help="Transfer throughput obtained in sample transfer")
    parser.add_option("-x", "--basicClientControlChannel-rate", action="store", type="float",
                        dest="cc_rate")
    parser.add_option("-y", "--p-rate", action="store", type="float",
                        dest="p_rate")
    parser.add_option("-z", "--ppq-rate", action="store", type="float",
                        dest="ppq_relax_ratio")
    parser.add_option("-m", "--maxcc", action="store", type="int",
                        dest="maxcc")
    return parser.parse_args()


def read_data_from_file(file_id):
    """"
     reads parameter value-throughput data from file
     file is structured to hold metadata followed by entries
     sg5-25M.csv 10
     entry1
     .
     entry10
     sg1G.csv 432
    """
    try:
        name, size, similarity_ = file_id.next().strip().split()  # skip first line
        data = np.genfromtxt(itertools.islice(file_id, int(size)), delimiter=' ')
        similarity = float(similarity_)
        print len(data), similarity
        return data, name, size, similarity*100
    except:
        return None, None, None, None


def run_modelling(data, name, first_row):
    global maximums
    maximums = data.max(axis=0)
    global maxcc
    if maxcc > 0:
        maximums[0] = maxcc
        #maximums[0] = min(maxcc, maximums[0])
        #if maximums[0] < 32:
        #    print name, maximums[0]
    min_training_score = 0.7
    min_test_score = 0.7
    for degree in range(2, 5):
        polynomial_features = PolynomialFeatures(degree=degree)
        regression, train_score, test_score = run_regression(polynomial_features, data)
        optimal_point = find_optimal_point(polynomial_features, regression, maximums)
        optimal_point_thr = -1 * optimal_point.fun
        #print name, first_row, degree, train_score, test_score,optimal_point.x
        if optimal_point_thr < maximums[-1]*2 and train_score > min_training_score and test_score > min_test_score:
            #print degree, train_score, test_score,optimal_point.x
            return regression, degree, optimal_point
    #print "Skipped:", train_score, test_score, optimal_point_thr
    return None, None, None


def run_regression(poly, data):
    regression_model = linear_model.LinearRegression()

    np.random.shuffle(data)
    train_data, test_data = np.vsplit(data, [int(data.shape[0]*.8)])

    # Training score
    train_params = train_data[:, 0:3]
    train_thr = train_data[:, -1]
    train_params_ = poly.fit_transform(train_params)
    # preform the actual regression
    regression_model.fit(train_params_, train_thr)
    train_score = regression_model.score(train_params_, train_thr)

    # Test score
    test_params = test_data[:, 0:3]
    test_thr = test_data[:, -1]
    testing_params_ = poly.fit_transform(test_params)
    test_score = regression_model.score(testing_params_, test_thr)
    # print train_score, test_score

    return regression_model, train_score, test_score


def find_optimal_point(poly, regression, maximums):
    func = convert_to_equation(poly, regression)
    bounds = (1, maximums[0]), (1, maximums[1]), (0, maximums[2])
    guess = [1, 1, 1]
    return minimize(func, guess, bounds=bounds, method='L-BFGS-B')


# Converts polynomial equation parameters found by PolynomialFeatures to an lambda equation
def convert_to_equation(poly, clf):
    features = []
    for i in range(3):
        features.append('x[' + str(i) + ']')
    coefficients = cycle(clf.coef_)
    # skip first coefficient as it is always 0, not sure why
    coefficients.next()
    equation = ''
    for entry in poly.powers_:
        new_feature = []
        for feat, coef in zip(features, entry):
            if coef > 0:
                new_feature.append(feat+'**'+str(coef))
        if not new_feature:
            equation = str(clf.intercept_)
        else:
            equation += ' + '
            equation += '*'.join(new_feature)
            equation += '*' + str(coefficients.next())
    return lambda x: eval('-1 * (' + equation + ')')


def main():
    (options, args) = parseArguments()
    chunk_name = options.filename
    sample_transfer_params = np.array([options.sample_concurrency, options.sample_parallelism,
                                            options.sample_pipelining])
    sample_transfer_throughput = options.sample_throughput
    if options.maxcc is not None:
        global maxcc
        maxcc = options.maxcc
    
    file_name = os.path.join(os.getcwd(), 'target', chunk_name)

    resource_package = __name__  # Could be any module/package name
    resource_path = '/' + chunk_name  # Do not use os.path.join(), see below
    print resource_package, resource_path
    #fin = pkg_resources.resource_stream(resource_package, resource_path)

    print file_name
    #sys.exit()
    discarded_data_counter = 0
    all_experiments = []
    fin = open(file_name, 'r')
    data, name, size, similarity = read_data_from_file(fin)
    while data is not None:
        data_copy = np.array(data)
        regression, degree, optimal_point = run_modelling(data_copy, name, data[0,:])
        if regression is None:
            #print "Skipped", name, size
            discarded_data_counter += 1
        #elif name.startswith("SB") or name.startswith("sg"):
        #    discarded_data_counter += 1
        else:
            all_experiments.append(TransferExperiment(name, size, similarity, regression, degree, optimal_point, data[0,:]))
            #print "Read data point ", name, data[0,:], data
            #sys.exit(-1)
        data, name, size, similarity = read_data_from_file(fin)
    #print "Skipped:", discarded_data_counter,  "/", (len(all_experiments) + discarded_data_counter)
    fin.close()


    for experiment in all_experiments:
        poly = PolynomialFeatures(degree=experiment.poly_degree)
        experiment.estimated_troughput = experiment.regression.predict(poly.fit_transform(sample_transfer_params.reshape(1, -1)))
        experiment.set_closeness(abs(experiment.estimated_troughput-sample_transfer_throughput))

    all_experiments.sort(key=lambda x: x.closeness, reverse=True)
    for experiment in all_experiments:
        experiment.run_parameter_relaxation(options.cc_rate, options.p_rate, options.ppq_rate)
        print experiment.name, experiment.estimated_troughput, " diff:", experiment.closeness, experiment.similarity, experiment.relaxed_params

    all_experiments.sort(key=lambda x: x.closeness, reverse=True)
    attrs = [experiment.closeness for experiment in all_experiments]

    #print attrs
    X = np.array(attrs, dtype=np.float)
    bandwidth = estimate_bandwidth(X, quantile=0.2)
    ms = MeanShift(bandwidth=bandwidth, bin_seeding=True)
    ms.fit(X)
    labels = ms.labels_
    cluster_centers = ms.cluster_centers_
    labels_unique = np.unique(labels)
    n_clusters_ = len(labels_unique)


    label = labels[0]

    weight = 0
    #for experiment in all_experiments:


    sorted_centers = sorted(cluster_centers[:,0], reverse=True)
    #print cluster_centers, sorted_centers

    #print "Sorted indexes"
    #for cluster_center in cluster_centers:
    #    print sorted_centers.index(cluster_center)

    #print labels
    for k in range(n_clusters_):
        my_members = labels == k
        #print my_members
        #print "cluster {0}: {1}".format(k, X[my_members, 0])


    my_members = labels == labels[-1]
    #similar_experiments = [experiment for experiment in all_experiments if experiment.closeness in  X[my_members]]
    #all_experiments = similar_experiments

    for experiment, label in zip(all_experiments, labels):
        rank = sorted_centers.index(cluster_centers[label])
        # print "Traffic similar:", experiment.name, experiment.closeness, rank
        experiment.closeness_weight = 2 ** rank


    all_experiments.sort(key=lambda x: x.similarity)
    attrs = [experiment.similarity for experiment in all_experiments]
    db1 = DBSCAN(eps=2, min_samples=1).fit(attrs)
    similarity_labels = db1.labels_
    # print attrs, similarity_labels
    for experiment, similarity_label in zip(all_experiments, similarity_labels):
        experiment.similarity_weight = 2 ** similarity_label
    # print attrs, similarity


    all_experiments.sort(key=lambda x: x.closeness)

    for experiment in all_experiments:
        experiment.run_parameter_relaxation(options.cc_rate, options.p_rate, options.ppq_rate)

    total_weight = 0
    total_thr = 0
    total_params = [0, 0, 0]

    for experiment in all_experiments:
        if experiment.similarity_weight < 1:
            # print experiment.name, experiment.closeness, experiment.closeness_weight, experiment.similarity, experiment.similarity_weight
            continue
        weight = experiment.similarity_weight * experiment.closeness_weight
        total_weight += weight
        weighted_params = [param * weight for param in experiment.relaxed_params]
        #print "HEYYY", experiment.name, experiment.closeness, experiment.closeness_weight, experiment.similarity_weight, weight,  experiment.relaxed_params, experiment.first_row
        total_params = map(add, total_params, weighted_params)
        total_thr += weight * experiment.relaxed_throughput

    final_params = [x / (total_weight*1.0) for x in total_params]
    final_throughput = total_thr/total_weight
    return final_params, final_throughput

if __name__ == "__main__":
    start_time = time.time()
    total_thr = 0
    total_params = [0, 0, 0]
    repeat = 3
    for i in range(repeat):
        params, thr = main()
        total_thr += thr
        total_params[:] = [x + y for x, y in zip(total_params, params)]
        print params, thr
    average_thr = total_thr/(repeat*1.0)
    average_params = [round(x / (repeat*1.0)) for x in total_params]
    average_params = map(int, average_params)
    print ' '.join(str(x) for x in average_params), str(average_thr[0])
    #print("--- %s seconds ---" % (time.time() - start_time))
