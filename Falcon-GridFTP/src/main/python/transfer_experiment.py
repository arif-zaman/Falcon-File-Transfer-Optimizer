import numpy as np
from sklearn.preprocessing import PolynomialFeatures
import sys


class TransferExperiment(object):

    def __init__(self, name, count, similarity, regression=None, poly_degree=None, optimal_point=None, first_row = None):
        """

        :param name: str
        :param count:
        :param regression:
        :param degree:
        :param optimal_point:
        """
        self.name = name
        self.count = count
        self.similarity = [similarity]
        self.regression = regression
        self.optimal_point = optimal_point
        self.poly_degree = poly_degree
        self.extimated_throughput = -1
        self.closeness = None
        self.closeness_weight = -1
        self.similarity_weight = -1
        self.relaxed_params = None
        self.relaxed_throughput = None
        self.first_row = first_row

    def set_regression(self, regression):
        """

        :param regression:
        :return:
        """
        self.regression = regression

    def set_optimal_point_result (self, optimal_point):
        self.optimal_point = optimal_point

    def set_closeness(self, closeness):
        self.closeness = closeness

    def run_parameter_relaxation(self, cc_rate, p_rate, ppq_rate):

        #print self.optimal_point
        poly = PolynomialFeatures(degree=self.poly_degree)
        optimal_cc = int(self.optimal_point.x[0])
        optimal_p = int(self.optimal_point.x[1])
        optimal_ppq = self.optimal_point.x[2]
        optimal_throughput_revised = -1 * self.optimal_point.fun
        print self.name, self.poly_degree, "Optimal basicClientControlChannel:", optimal_cc, " p ", optimal_p, "ppq", self.optimal_point.x[2], "Thr:", optimal_throughput_revised

        throughput = optimal_throughput_revised

        
        #print "Optimal CC ", optimal_cc, "Thr:", optimal_throughput_revised
        relaxed_cc = optimal_cc
        prev_thr = 0
        for relaxed_cc in range(optimal_cc-1, 0, -1):
            new_params = np.array([relaxed_cc, optimal_p, self.optimal_point.x[2]])
            throughput = self.regression.predict(poly.fit_transform(new_params.reshape(1, -1)))
            #print "basicClientControlChannel", relaxed_cc, throughput, prev_thr, (prev_thr-throughput)
            prev_thr = throughput
            if throughput < cc_rate * optimal_throughput_revised:
                relaxed_cc += 1
                new_params = np.array([relaxed_cc, optimal_p, self.optimal_point.x[2]])
                throughput = self.regression.predict(poly.fit_transform(new_params.reshape(1, -1)))
                optimal_throughput_revised = throughput
                break

        print optimal_cc, optimal_throughput_revised, "relaxed", relaxed_cc, throughput
        relaxed_p = optimal_p
        prev_thr = 0
        for relaxed_p in range(optimal_p-1, 0, -1):
            new_params = np.array([relaxed_cc, relaxed_p, optimal_ppq])
            throughput = self.regression.predict(poly.fit_transform(new_params.reshape(1, -1)))
            #print "p", relaxed_p, throughput, prev_thr, (prev_thr-throughput)
            prev_thr = throughput
            if throughput < p_rate * optimal_throughput_revised:
                relaxed_p += 1
                new_params = np.array([relaxed_cc, relaxed_p, self.optimal_point.x[2]])
                throughput = self.regression.predict(poly.fit_transform(new_params.reshape(1, -1)))
                optimal_throughput_revised = throughput
                break
        print optimal_p, -1 * self.optimal_point.fun, "relaxed", relaxed_p, throughput, prev_thr, (prev_thr-throughput)
        optimal_throughput_revised = throughput

        optimal_throughput_revised = throughput
        estimated_optimal_ppq = int(self.optimal_point.x[2])
        relaxed_ppq = estimated_optimal_ppq
        #print self.name, self.similarity, "Optimal ppq", estimated_optimal_ppq, "Thr:", optimal_throughput_revised
        for relaxed_ppq in range(estimated_optimal_ppq-1, -1, -1):
            new_params = np.array([relaxed_cc, relaxed_p, relaxed_ppq])
            throughput = self.regression.predict(poly.fit_transform(new_params.reshape(1, -1)))
            #print "ppq", relaxed_ppq, throughput
            if throughput < ppq_rate * optimal_throughput_revised:
                relaxed_ppq += 1
                new_params = np.array([relaxed_cc, relaxed_p, relaxed_ppq])
                throughput = self.regression.predict(poly.fit_transform(new_params.reshape(1, -1)))
                optimal_throughput_revised = throughput
                break
        throughput = optimal_throughput_revised
        self.relaxed_params = [relaxed_cc, relaxed_p, relaxed_ppq]
        self.relaxed_throughput = optimal_throughput_revised
