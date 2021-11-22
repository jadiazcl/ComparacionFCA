from time import time
from itertools import combinations, product
import  multiprocessing as mp
import sys
from ej_lib import soporte, close_intent, derive
from functools import reduce
from functools import partial
import os
from collections import defaultdict
import json
from datetime import datetime

fmtset = lambda X: ','.join(map(str, sorted(X)))

def next_closure(X, ctx, M, stack):  
    n_aux_clouses=0
    for m in sorted(M, reverse=True):
        if m in X:
            X.remove(m)
            if stack[-1][0] == m:
                stack.pop()
        else:
            Yp = stack[-1][1].intersection(ctx[1][m])
            Y = derive(Yp, ctx[1])
            n_aux_clouses += 1
            if m <= min(Y-X):
                stack.append( (m, Yp) )  
                return Y, n_aux_clouses
    return M, n_aux_clouses

def all_closures(ctx, M):
    results = []
    # First closure
    X = set([])
    Xp = derive(X, ctx[0])
    X = derive(Xp, ctx[1])
    results.append(sorted(X))
    n_aux_clouses = 1
    # Stacked version
    stack = [(None, Xp)]   
    
    while len(X) < len(M):
        #print('\r',n_aux_clouses, end='')
        sys.stdout.flush()
        X, nc = next_closure(X, ctx, M, stack)
        n_aux_clouses += nc
        results.append(sorted(X))
    #print()
    return results, n_aux_clouses


if __name__ == '__main__':
    start_time=time()
    NCLOSURES = 0
    path = sys.argv[1]
    m_count = defaultdict(lambda: 0)
    with open(path, 'r') as fin:
        lines = fin.readlines()
        g_prime = {gi: set(map(int, line.split())) for gi, line in enumerate(lines)}

    M = set([])
    for gp in g_prime.values():
        M.update(gp)
        for m in gp:
            m_count[m] += 1
    M = sorted(M)
    m_map = {i:j for j, i in enumerate(sorted(M, key=lambda m: m_count[m], reverse = True))}
    m_prime = defaultdict(set)

    for gi in g_prime:
        for m in g_prime[gi]:
            m = m_map[m]
            m_prime[m].add(gi)
        g_prime[gi] = set([m_map[m] for m in g_prime[gi]])    
        

    ctx = (g_prime, m_prime)

    M = sorted(reduce(set.union, ctx[0].values()))
    results, NCLOSURES = all_closures(ctx, M)
    time=time()-start_time
    #print(len(results))
    #print("# CLOSURES:", NCLOSURES)            
    results = {
        'n_results' : len(results),
        'n_closures' : NCLOSURES,
        'exec_time' : time,                                
    }
    
    d = datetime.now()
    timestamp = '{}{}{}{}{}{}'.format(d.year, d.month, d.day, d.hour, d.minute, d.second)
    fname = path[path.rfind('/'):path.rfind('.')]
    fname=fname[1:]
    with open('results/{}-{}-{}.json'.format("next_closure",fname, timestamp) , 'w') as fout:
        json.dump(results, fout)            
