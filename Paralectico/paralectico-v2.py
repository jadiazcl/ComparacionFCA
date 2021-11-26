# import numpy as np
import ray
from time import time
import sys
import json
from datetime import datetime
from set_lib import soporte, close_intent, derive
import os
from collections import defaultdict

# ray.init(local_mode=True)


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
    
    for m in M:
        if m in X:
            X.remove(m)
            if stack[-1][0] == m:
                stack.pop()
        else:
            Yp = stack[-1][1].intersection(m_prime[m])
            Y = derive(Yp, m_prime)
            n_aux_clouses += 1
            if m <= min(Y-X):   
                stack.append( (m, Yp) )  
                return Y, n_aux_clouses
    return None, n_aux_clouses

def mplus(X, m, g_prime, m_prime):
    return close_intent(X.intersection(range(m)).union([m]), g_prime, m_prime)

def lec_leq(A, B):
    # print('\t {} < {}'.format(A,B))
    if A.issubset(B) or min(B-A) < min(A-B):
        return True
    return False

def local_lectic_enum(X, Xp, M, g_prime, m_prime, depth=1, learner=[50000, 1.2, 1.3]):
    #print('--'*100)
    #print("CALL", X, M, learner)
    #print('--'*100)
    
    di = 0
    t0 = time()
    # depth = len(X)
    n_aux_clouses = 0
    
    Minv = sorted(M, reverse=True)
    # X = set([])
    # Xp = derive(X, g_prime)
    # cX = derive(Xp, m_prime)
    # if not (cX.issubset(M) and lec_leq(X, cX)):
    #     return [[[], 1, 0, depth]]
    # X = cX
    result = [sorted(X)]
    # X = derive(Xp, m_prime)
    OX = set(X)
    stack = [(None, Xp)]# derive(X, g_prime, 2))]
    cycle = 0
    blocks = []
    entries = []
    t_loop = t0
    learn = True
    while True:
        
        X, nc = next_lectic_set(X, Minv, g_prime, m_prime, stack)
        # print(':: ', X, OX, M, Minv)
        if X is None:
            break
        result.append(sorted(X))
        
        n_aux_clouses += nc

        # # LEARN
        # if learn and n_aux_clouses >= 1000:
        #     learn = False
        #     d_time = time() - t0
        #     learner[0] = 10*n_aux_clouses//d_time
        #     print(int(learner[0]))

        # SPLIT
        d_time = time() - t_loop
        if d_time >= learner[0]*learner[2]**cycle and len(X) < len(M):# and ray.available_resources().get('CPU', 0) > 0:
            t_loop = time()
            
            # if d_time < 5:
            #     learner[0] *= learner[1]
            # elif d_time > 100:
            #     learner[0] *= learner[2]
            # else:
            # t_loop = time()
            # cycle += 1
            # print(ray.available_resources())
            leg = 'left'
            # LEFT LEG BY DEFAULT
            ml = min(set(M)-OX)
            nX = OX.union([ml])
            if lec_leq(nX, X): #RIGHT LEG
                leg = 'right'
                ml = min(set(M)-X)
                nX = set(M).intersection(range(ml+1))
            nXp = derive(nX, g_prime)
            nCX = derive(nXp, m_prime)

            if nCX.issubset(M) and lec_leq(nX, nCX):
                blocks.append(lectic_enum.remote(nCX, nXp, M, g_prime, m_prime, depth+1, learner=learner))
            #else:
                #print("NOT SENT", leg, OX, X, nX, nCX, M)
            #print(learner, d_time, learner[0]*learner[2]**cycle, cycle, ml)
            M = sorted(X.union(range(ml+1, Minv[0]+1)))
            Minv = sorted(M, reverse=True)
            # print()
            
        # if bool(blocks):
            
        #     ready_jobs, blocks  = ray.wait(blocks)
        #     res = ray.get(ready_jobs[0])
        #     entries.extend(res)
        #     print("Collecting", OX, res[-1][-2])
        #     if res[-1][-2] < 1:
        #         learner[0] *= learner[1]
        #     if res[-1][-2] > 100:
        #         learner[0] *= learner[2]
    
    t1 = time() - t0
    #print("END PROCESSING", OX, M, t1, n_aux_clouses)

    while bool(blocks):
        ready_jobs, blocks  = ray.wait(blocks)
        res = ray.get(ready_jobs[0])
        entries.extend(res)

    
    
    
        # result.extend(R)
        # n_aux_clouses += N   
            # print('\t\t->',split(X, sorted(M), g_prime, m_prime))

    
    # for X in generator_lectic_enum(X, M, g_prime, m_prime):
    #     di +=1
    #     result.append(sorted(X))
    
    # print(n_aux_clouses)
    # print('#'*100)
    entries.append(
        [result, n_aux_clouses, t1, depth]
    )
    return entries

def split(X, M, g_prime, m_prime):
    # print(M)
    # print('@@'*50)
    # print(X, M)
    # print('=='*50)
    y = min(set(M)-X)
    Y = set([y])
    # print( [] )
    # Mizq = M[M.index(y)+1:]
    Y = close_intent(X.union(Y), g_prime, m_prime)
    Mizq = sorted(X.union(range(y+1, M[-1]+1)))
    # print([(Y, set(M)-Y), (X, M)])
    
    # print('\t',str([(X, Mizq), (Y, M)]))
    
    # print('@@'*50)
    return [(X, Mizq), (Y, M)]

if __name__ == '__main__':
    print("INIT")
    init_time = time()
    lectic_enum = ray.remote(local_lectic_enum)

    NCLOSURES = 0
    start_time = time() 
    path = sys.argv[1]
    order = 'D'
    block_size = 50000

    if len(sys.argv) > 2:
        order = sys.argv[2]
    if len(sys.argv) > 3:
        block_size = int(sys.argv[3])

    M = set()
    g_prime = []
    m_count = defaultdict(lambda: 0)
    with open(path, 'r') as fin:
        for line in fin:
            g_prime.append(set(map(int, line.split())))
            for m in g_prime[-1]:
                m_count[m] += 1
            M = M.union(g_prime[-1])
    M = sorted(M)
    
    def descendent(s):
        return sorted(s, key=lambda m: m_count[m], reverse = True)
    def ascendent(s):
        return sorted(s, key=lambda m: m_count[m], reverse = False)
    def descendent_crossed(s):
        Morder = sorted(s, key=lambda m: m_count[m], reverse = True)
        return Morder[-carga:] + Morder[:-carga]
    def crossover(s):
        Morder = sorted(s, key=lambda m: m_count[m], reverse = True)
        toggle = False
        for i in range(min(carga, len(Morder[carga:]))):
            if toggle:
                aux = Morder[i]
                Morder[i] = Morder[carga+i]
                Morder[carga+i] = aux
            toggle = not toggle
        return Morder

    order_strategy = {
        'D': descendent,
        'I': ascendent,
        'DI': descendent_crossed,
        'X': crossover
    }

    Morder = order_strategy[order](M)
    #print(Morder)
    m_map = {i:j for j, i in enumerate(Morder)}
    
    m_prime = [set([]) for  i in range(len(M))]# defaultdict(set)
    for gi in range(len(g_prime)):
        for m in g_prime[gi]:
            correlative_m = m_map.setdefault(m, len(m_map))
            m_prime[correlative_m].add(gi)
    
    for gi in range(len(g_prime)):
        g_prime[gi] = set([m_map[m] for m in g_prime[gi]]) 

    M = sorted(m_map.values())
    
    m_prime = [set(mp) for mp in m_prime]# [set(map(np.array, map(sorted, m_prime)))]

    
    g_proxy = ray.put(g_prime)
    m_proxy = ray.put(m_prime)

    all_result = []

    blocks = []
    i = 1

    #divide in 4
    # M_par_sorted = sorted(M_par)


    # seeds = 

    # # First Closure
    # X0 = set([])
    # Xp0 = derive(X0, g_prime)
    # X0 = derive(Xp0, m_prime)
    # print(ray.available_resources())
    # exit()
    learner = [block_size, 2, 0.9]
    
    # blocks.append(lectic_enum.remote(X0, Xp0, M, g_proxy, m_proxy, learner=learner))

    MODE = 1
    M0 = sorted(M)
    if MODE == 0:
        #print(split(X, M_par_sorted))
        # exit()
        level = 0
        max_level = 2 # 2**2 processes
        X0 = close_intent(set([]), g_prime, m_prime)
        while level <= max_level:
            (X0, M0), (X1, M1) = split(X0, M0, g_prime, m_prime)
            #print('**'*50)
            #print(len(X1), X1, M1)
            blocks.append(lectic_enum.remote(X1, M1, g_proxy, m_proxy))
            level += 1
        #print('**'*50)
        X0 = close_intent(X0, g_prime, m_prime)
        #print(len(X0), X0, M0)
        # exit()
        blocks.append(lectic_enum.remote(X0, M0, g_proxy, m_proxy))
    else:
        # EVEN STRATEGY
        X0 = close_intent(set([]), g_prime, m_prime)
        for X1, M1 in split(X0, M0, g_prime, m_prime):
            # print(X0, M0)

            for X2, M2 in split(X1, M1, g_prime, m_prime):
                #print('**'*50)
                #print('\t',len(X2), X2, M2)
                Xp2 = derive(X2, g_prime)
                X2 = derive(Xp2, m_prime)
                blocks.append(lectic_enum.remote(X2, Xp2, M2, g_proxy, m_proxy, learner=learner))
    # exit()
    # for X in generator_lectic_enum(set([]), M, g_prime, m_prime):
    #     #print(X)
    #     print('\r{}'.format(i), end='')
    #     sys.stdout.flush()
    #     i+=1
    

    nresults = []
    closures = []
    times = []
    depths = []
    while bool(blocks):
        #print('\r{}'.format(len(blocks)), end='')
        sys.stdout.flush()
        ready_jobs, blocks  = ray.wait(blocks)
        # print(ready_jobs)
        entries = ray.get(ready_jobs[0])
        for R, nc, t, d in entries:
            all_result.extend(R)
            nresults.append(len(R))
            closures.append(nc)
            times.append(t)
            depths.append(d)
            NCLOSURES += nc
    time=time()-init_time
    print("END")
    print(time)
    memoria=memory_usage_psutil()
    #print()

    results = {
        'n_results' : nresults,
        'n_closures' : closures,
        'exec_time' : time,
        'order': order,
        'times': times,
        'depths':depths,
        'block_size': block_size,
        'memory_info': memoria
    }
    
    d = datetime.now()
    timestamp = '{}{}{}-{}-{}-{}'.format(d.year, d.month, d.day, d.hour, d.minute, d.second)

    fname = path[path.rfind('/'):path.rfind('.')]
    fname=fname[1:]
    with open('results/{}-{}-{}.json'.format("Paralecticalv2",fname, timestamp) , 'w') as fout:
        json.dump(results, fout)

    #print(len(all_result))
    
    #print("# CLOSURES: ", NCLOSURES)

    #all_result.sort()
    #with open('out1.txt', 'w') as fout:
    #    for r in all_result:
            # print(r)
    #        fout.write('{}\n'.format( ','.join(map(str, r)) ))
    ray.shutdown()
    
    