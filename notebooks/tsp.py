#  -*- coding: utf-8 -*-
import math  # math
import random  # random
import numpy as np  # np  np YouCans
import matplotlib.pyplot as plt  # matplotlib.pyplot  plt
from sys import maxsize
from itertools import permutations


def getDistMat(nCities, coordinates):
    # custom function getDistMat(nCities, coordinates):
    # computer distance between each 2 Cities
    distMat = np.zeros((nCities, nCities))  #
    for i in range(nCities):
        for j in range(i, nCities):
            # np.linalg.norm   ij
            distMat[i][j] = distMat[j][i] = round(np.linalg.norm(coordinates[i] - coordinates[j]))
    return distMat  #


class AnnealingSolver:
    def __init__(self):
        pass

    def initParameter(self):
        # custom function initParameter():
        # Initial parameter for simulated annealing algorithm
        tInitial = 10.0  # (initial temperature)
        tFinal = 0.01  # (stop temperature)
        nMarkov = 100  # Markov
        # increase alfa for more accuracy
        alfa = 0.99  # T(k)=alfa*T(k-1)

        return tInitial, tFinal, alfa, nMarkov

    # TSPLib
    def read_TSPLib(self, fileName):
        # custom function read_TSPLib(fileName)
        # Read datafile *.dat from TSPlib
        # return coordinates of each city by YouCans, XUPT

        res = []
        with open(fileName, 'r') as fid:
            for item in fid:
                if len(item.strip()) != 0:
                    res.append(item.split())

        loadData = np.array(res).astype('int')  # i Xi Yi
        coordinates = loadData[:, 1::]
        return coordinates

    #  TSP
    def calTourMileage(self, tourGiven, nCities, distMat):
        # custom function caltourMileage(nCities, tour, distMat):
        # to compute mileage of the given tour
        mileageTour = distMat[tourGiven[nCities - 1], tourGiven[0]]  # dist((n-1),0)
        for i in range(nCities - 1):  # dist(0,1),...dist((n-2)(n-1))
            mileageTour += distMat[tourGiven[i], tourGiven[i + 1]]
        return round(mileageTour)  #

    #  TSP
    def plot_tour(self, tour, value, coordinates):
        # custom function plot_tour(tour, nCities, coordinates)

        num = len(tour)
        x0, y0 = coordinates[tour[num - 1]]
        x1, y1 = coordinates[tour[0]]
        plt.scatter(int(x0), int(y0), s=15, c='r')  # C(n-1)
        plt.plot([x1, x0], [y1, y0], c='b')  # C(n-1)~C(0)
        for i in range(num - 1):
            x0, y0 = coordinates[tour[i]]
            x1, y1 = coordinates[tour[i + 1]]
            plt.scatter(int(x0), int(y0), s=15, c='r')  # C(i)
            plt.plot([x1, x0], [y1, y0], c='b')  # C(i)~C(i+1)

        plt.xlabel("Total mileage of the tour:{:.1f}".format(value))
        plt.title("Optimization tour of TSP{:d}".format(num))  #
        plt.show()

    #
    def mutateSwap(self, tourGiven, nCities):
        # custom function mutateSwap(nCities, tourNow)
        # produce a mutation tour with 2-Swap operator
        # swap the position of two Cities in the given tour

        #  [0,n)  2 i,j
        i = np.random.randint(nCities)  # [0,n)
        while True:
            j = np.random.randint(nCities)  # [0,n)
            if i != j: break  # i, j

        tourSwap = tourGiven.copy()  # tourSwap
        tourSwap[i], tourSwap[j] = tourGiven[j], tourGiven[i]  # i  j

        return tourSwap

    def solve_tsp(self, coordinates, df_rtt=None):
        tInitial, tFinal, alfa, nMarkov = self.initParameter()  #

        nCities = coordinates.shape[0]  # nCities
        if df_rtt is not None:
            distMat = df_rtt.values
        else:
            distMat = getDistMat(nCities, coordinates)  #
        nMarkov = nCities  # Markov
        tNow = tInitial  # (current temperature)

        #
        tourNow = np.arange(nCities)  # 01n
        valueNow = self.calTourMileage(tourNow, nCities, distMat)  # valueNow
        tourBest = tourNow.copy()  # tourNow
        valueBest = valueNow  # valueNow
        recordBest = []  #
        recordNow = []  #

        #
        iter = 0  #
        while tNow >= tFinal:  #
            # (nMarkov)

            for k in range(nMarkov):  # Markov
                #
                tourNew = self.mutateSwap(tourNow, nCities)  #
                # tourNew,deltaE = mutateSwapE(tourNow,nCities,distMat)   #    deltaE
                valueNew = self.calTourMileage(tourNew, nCities, distMat)  #
                deltaE = valueNew - valueNow

                #  Metropolis
                if deltaE < 0:  #
                    accept = True
                    if valueNew < valueBest:  #
                        tourBest[:] = tourNew[:]
                        valueBest = valueNew
                else:  #
                    pAccept = math.exp(-deltaE / tNow)  #
                    if pAccept > random.random():
                        accept = True
                    else:
                        accept = False

                #
                if accept == True:  #
                    tourNow[:] = tourNew[:]
                    valueNow = valueNew

            #  0,n-1
            tourNow = np.roll(tourNow, 2)  #

            #
            recordBest.append(valueBest)  #
            recordNow.append(valueNow)  #
            if iter % 10 == 0:
                print('i:{}, t(i):{:.2f}, valueNow:{:.1f}, valueBest:{:.1f}'.format(iter, tNow, valueNow, valueBest))

            #
            iter = iter + 1
            tNow = tNow * alfa  # T(k)=alfa*T(k-1)
        return tourBest, valueBest, nCities, recordBest, recordNow


class BruteForceSolver:
    def solve_tsp(self, coordinates):
        s = 0
        nCities = coordinates.shape[0]  # nCities
        distMat = getDistMat(nCities, coordinates)
        print(distMat)

        # store all vertex apart from source vertex
        vertex = []
        for i in range(len(coordinates)):
            vertex.append(i)

        # store minimum weight Hamiltonian Cycle
        min_path = maxsize
        next_permutation = permutations(vertex)
        min_route = None
        for i in next_permutation:

            # store current Path weight(cost)
            current_pathweight = 0

            # compute current path weight
            k = s
            for j in i:
                current_pathweight += distMat[k][j]
                k = j
            current_pathweight += distMat[k][s]

            # update minimum
            min_path = min(min_path, current_pathweight)

            if min_path == current_pathweight:
                min_route = i

        return min_route, min_path
