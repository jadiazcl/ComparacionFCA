import numpy as np
import ray
from time import time
from itertools import combinations, product
import sys
from ej_lib import soporte, close_intent, derive, derive_attributes, derive_objects
from functools import reduce, partial
import os
from collections import defaultdict
from boolean_tree import BooleanTree
import json
from datetime import datetime
# ray.init(local_mode=True)

sys.setrecursionlimit(10**6)

global NCLOSURES
fmtset = lambda X: ','.join(map(str, sorted(X)))

def memory_usage_psutil():
    # return the memory usage in MB
    import psutil
    import os
    process = psutil.Process(os.getpid())
    #print(process.memory_info())
    mem = process.memory_info().rss / float(2 ** 20)
    return mem

def next_lectic_set(X, M, g_prime, m_prime, stack):  
    n_aux_clouses=0
    
    for m in M:#sorted(M, reverse=True):
        # print(m, stack)
        if m in X:
            X = np.delete(X, np.where(X == m))
            if stack[-1][0] == m:
                stack.pop()
        else:

            Yp = np.intersect1d(stack[-1][1], m_prime[m], assume_unique=True)
            Y = derive_objects(Yp, g_prime, m_prime, flag=1)# derive(Yp, m_prime, flag=1)
            n_aux_clouses += 1
            D = np.setdiff1d(Y, X, assume_unique=True)
            if D.size == 0 or m <= D[0]:#min(D):
                stack.append( (m, Yp) ) 
                # print('\t G',X, m, Y, stack[-1])
                return Y , n_aux_clouses
            # else:
            #     print('\t B',X, m, Y, stack[-1])
    return None,n_aux_clouses

@ray.remote
def lectic_enum(X, M, g_prime, m_prime):
    M = sorted(M, reverse=True)
    maxm=max(np.setdiff1d(X, M, assume_unique=True)) if X.size >0 else None
    # print(X[-15:], len(M), maxm)# len(M))
    n_aux_clouses=0
    result = [X]
    stack = [(maxm, derive_attributes(X, g_prime, m_prime, 2))]# derive(X, g_prime, 2))]
    
    # print(stack)
    while X is not None:
        # print('\r',X, end='')
        # sys.stdout.flush()
        X, n_aux = next_lectic_set(X, M, g_prime, m_prime, stack)
        n_aux_clouses += n_aux
        if X is not None and X.size > 0:
            # print('\t',X)
            result.append(sorted(X))
    return result, n_aux_clouses

if __name__ == '__main__':
    print("INIT")
    NCLOSURES = 0
    start_time = time() 
    path = sys.argv[1]

    M = set()
    g_prime = []
    m_count = defaultdict(lambda: 0)
    with open(path, 'r') as fin:
        for line in fin:
            g_prime.append(sorted(map(int, line.split())))# for line in lines]# {gi: set(map(int, line.split())) for gi, line in enumerate(lines)}
            for m in g_prime[-1]:
                m_count[m] += 1
            M = M.union(g_prime[-1])
    M = sorted(M)
    

    # Sort by number of appearances desc
    # m_map = {} # actual_id -> correlative_id
    m_map = {i:j for j, i in enumerate(sorted(M, key=lambda m: m_count[m], reverse = True))}
    
    m_prime = [[] for  i in range(len(M))]# defaultdict(set)
    for gi in range(len(g_prime)):
        for m in g_prime[gi]:
            correlative_m = m_map.setdefault(m, len(m_map))
            m_prime[correlative_m].append(gi)
    
    for gi in range(len(g_prime)):
        g_prime[gi] = np.array( sorted([m_map[m] for m in g_prime[gi]]) )

    M = np.array(sorted(m_map.values()))
    # print(m_map)

    g_prime = np.array(g_prime, dtype=object)
    m_prime = np.array(list(map(np.array, map(sorted, m_prime))), dtype=object)
    
    g_proxy = ray.put(g_prime)
    m_proxy = ray.put(m_prime)
    
    carga = int(sys.argv[2])

    if carga == 0:
        M_fd = set([])
        M_par = list(M)
    else:
        M_fd = M[carga:]
        M_par = M[:carga]
        #M_fd = set(M[-1*carga:])#  carga=0 M[0:], M_par[:0] | carga=8 M[-8:0] M_par[:-8]
        #M_par = M[:-1*carga]
    
    all_result = []
    blevels = defaultdict(lambda: BooleanTree())

    X = close_intent(np.array([]), g_prime, m_prime, flag=3)
    x_len = np.intersect1d(X, M_par, assume_unique=True).size
    blevels[x_len].append(np.isin(M, X, assume_unique=True))

    #first_block = lectic_enum.remote(X, M_fd, g_prime, m_prime)
    #R,nc = ray.get(first_block)
    #all_result.extend(R)
    #NCLOSURES += nc
    #print(len(all_result), NCLOSURES)
    #levels = defaultdict(list)
    
    for m in M_par:
        # print('\r',m,end='')
        # sys.stdout.flush()
        X = np.array([m])
        X = close_intent(X, g_prime, m_prime, flag=4)
        x_len = np.intersect1d(X, M_par, assume_unique=True).size
        blevels[x_len].append(np.isin(M, X, assume_unique=True))
    # print(levels)
    # print(blevels)
    for i in range(len(M_par)+1):
        t1 = time()
        #print('\r{}/{}::{} bloques'.format(i, len(M_par), len(blevels[i])), end='')
        sys.stdout.flush() 
        seeds = [np.where(bX)[0] for bX in blevels[i]]
        # print('Seeds', seeds.read())
        blocks = [lectic_enum.remote(X, M_fd, g_proxy, m_proxy) for X in seeds]
        #blocks = [lectic_enum.remote(X, M_fd, g_prime, m_prime) for X in levels[i]]
        #print()
        for block in blocks:
            R, nc = ray.get(block)
            all_result.extend(R)
            NCLOSURES += nc
        #print('\t RESULTS: {}, # CLOSURES:{}'.format(len(all_result), NCLOSURES))
        #print(len(all_result), NCLOSURES)
            # print('\t->',len(R), nc, NCLOSURES)
        #print('\t PREPARING NEXT LEVEL: {} seeds'.format(len(seeds)))
        itera=0
        # for A, B in product(levels[i], M_par):
        for A in seeds:#blevels[i]:
            #A = np.where(bA)[0]
            for B in np.setdiff1d(M_par, A, assume_unique=True):
                # print('\r',B,end='')
                # sys.stdout.flush()
                itera+=1
                NCLOSURES += 1
                X = close_intent(np.sort(np.append(A, B)), g_prime, m_prime, flag=5)
                x_len = np.intersect1d(X, M_par, assume_unique=True).size
                blevels[x_len].append(np.isin(M, X, assume_unique=True))
                
        #print('\t ITERACIONES: {}'.format(itera))
    memoria=memory_usage_psutil()
    #print(len(all_result))    
    #print("# CLOSURES: ", NCLOSURES)
    time=time()-start_time
    print("END")
    print(time)
    #print(len(results))
    #print("# CLOSURES:", NCLOSURES)            
    results = {
        'n_results' : len(all_result),
        'n_closures' : NCLOSURES,
        'exec_time' : time,   
        'memory_info': memoria                             
    }
    
    d = datetime.now()
    timestamp = '{}{}{}-{}-{}-{}'.format(d.year, d.month, d.day, d.hour, d.minute, d.second)
    fname = path[path.rfind('/'):path.rfind('.')]
    fname=fname[1:]
    with open('results/{}-{}-{}-{}.json'.format("ParalecticalV1",carga,fname, timestamp) , 'w') as fout:
        json.dump(results, fout)      
    

    
    
