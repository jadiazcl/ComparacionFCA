#import pandas as pd

# El siguiente es un algoritmo eficiente para el cálculo de items cerrados frecuentes

import sys
from itertools import chain, combinations
from functools import reduce
from time import time
from datetime import datetime
import json
def operador_clausura(P, T, I):
  Pp = [t for t in T if P.issubset(t)] #P'
  if not bool(Pp):
    return set(I)
  return set(reduce(set.intersection, Pp))

def genClosedCandidates(L, k, T, I,NCLOSURES):
  Lk = L.get(k-1, {})
  for P in Lk:
    for i in I:
      Pc = operador_clausura(P.union(set([i])), T, I)
      NCLOSURES+=1
      if len(Pc) not in L:
        L[len(Pc)] = set([])
      L[len(Pc)].add(frozenset(Pc))
  if k in L:
    return {i:0 for i in L[k]},NCLOSURES
  else:
    return {},NCLOSURES


def a_priori_closed(T, I, sigma,NCLOSURES):
  tid = {i:set() for i in I}
  for ti, t in enumerate(T):
    for i in t:
      tid[i].add(ti)

  L = {0:set([frozenset([])])} # Agregamos el conjunto vacío que siempre es frecuente

  for i, it in tid.items():
    if len(it) >= sigma:
      P = set([i])
      Pc = operador_clausura(P, T, I)
      NCLOSURES+=1
      if len(Pc) not in L:
        L[len(Pc)] = set([])
      L[len(Pc)].add(frozenset(Pc))
  
  C = {}
  i = 1
  while i < len(I):
    
    C[i+1],NCLOSURES = genClosedCandidates(L, i+1, T, I,NCLOSURES)
    for t in T:
      for candidate in C[i+1]:
        if candidate.issubset(t):
          C[i+1][candidate] += 1    
    i += 1
    memoria=memory_usage_psutil()
  return sorted(list(chain(*L.values())), key=len),NCLOSURES,memoria

def memory_usage_psutil():
    # return the memory usage in MB
    import psutil
    import os
    process = psutil.Process(os.getpid())
    #print(process.memory_info())
    mem = process.memory_info().rss / float(2 ** 20)
    return mem

print("INIT")
NCLOSURES = 0
start_time = time()     
path = sys.argv[1]
with open(path, 'r') as fin:
    lines = fin.readlines()
    ctx = [set(map(int, line.split())) for line in lines]
M = set(reduce(set.union, ctx))

FC_sigma,NCLOSURES,memoria = a_priori_closed(ctx, M, 0,NCLOSURES)
time=time()-start_time
print("END")
print(time)

results = {
    'n_results' : len(FC_sigma),
    'n_closures' : NCLOSURES,
    'exec_time' : time,   
    'memory_info': memoria
}

d = datetime.now()
timestamp = '{}{}{}-{}-{}-{}'.format(d.year, d.month, d.day, d.hour, d.minute, d.second)
fname = path[path.rfind('/'):path.rfind('.')]
fname=fname[1:]
with open('results/{}-{}-{}.json'.format("apriori",fname, timestamp) , 'w') as fout:
    json.dump(results, fout)
