
import sys
from itertools import chain, combinations
from functools import reduce
from time import time
from datetime import datetime
import json
import ray
import  multiprocessing as mp

@ray.remote(num_cpus=2)
class diccionario(object):
    def __init__(self):
        self.dic=dict()
        self.cont=0
    def add(self,llave,valor):
        if valor not in self.dic[llave]:
            self.dic[llave].add(valor)
        self.cont+=1
    def add_all(self,dics):
        self.dic.update(dics)
    def suma_1(self):
        self.cont+=1
    def get_value(self,llave):        
        if llave in self.dic.keys():
            type(self.dic[llave])
            return self.dic[llave]
        else:
            return {}
    def get_value_2(self,llave):
      if llave in self.dic.keys():            
            return {i:0 for i in self.dic[llave]},
      else:
          return dict()
    def all(self):
        return self.dic
    def get_intent(self):
        return self.cont
    def verificar_agregar(self,llave,valor):
        if llave not in self.dic:
          self.dic[llave]=set([])        
        self.dic[llave].add(valor)                  

def trabajo_paralelo(Lk,T,I,levels,flag):
  n_aux=0
  if flag:
    aux_dic=dict()
    for P in Lk:
      for i in I:
        Pc = operador_clausura(P.union(set([i])), T, I)
        n_aux+=1
        if len(Pc) not in aux_dic:
          aux_dic[len(Pc)] = set([])
        aux_dic[len(Pc)].add(frozenset(Pc))          
        #levels.verificar_agregar.remote(len(Pc),frozenset(Pc))      
    return n_aux,aux_dic
    
  else: 
    for P in Lk:
      for i in I:
        Pc = operador_clausura(P.union(set([i])), T, I)
        n_aux+=1
        if len(Pc) not in levels:
          levels[len(Pc)] = set([])
        levels[len(Pc)].add(frozenset(Pc))
    return n_aux,levels


def operador_clausura(P, T, I):
  Pp = [t for t in T if P.issubset(t)] #P'
  if not bool(Pp):
    return set(I)
  return set(reduce(set.intersection, Pp))




def genClosedCandidates(levels, k, T, I,test_remote,NCLOSURES):
  #Lk =ray.get(levels.get_value.remote(k-1))
  Lk = levels.get(k-1, {})
  largo=len(Lk)
  Lk=list(Lk)
  nucleos=mp.cpu_count()-1
  procesos=[]
  alpha=int(largo/nucleos)
  modulo=largo%nucleos      

  if alpha <1:    
    n_aux,levels=trabajo_paralelo(Lk,T,I,levels,False)
    NCLOSURES+=n_aux
          
  else:
    j=0    
    blocks_ids=[]      
    for i in range(0,modulo):        
        blocks_ids.append(test_remote.remote(Lk[j:j+alpha+1],T,I,levels,True))
        j=j+alpha+1
    for i in range(0,nucleos-modulo):           
        blocks_ids.append(test_remote.remote(Lk[j:j+alpha],T,I,levels,True))
        j=j+alpha
    result=ray.get(blocks_ids)
    for i,j in result:      
      NCLOSURES+=i
      for w,t in j.items():
        if w not in levels.keys():
          levels[w] = set([])
        levels[w].update(t)

  #value=ray.get(levels.get_value_2.remote(k))  
  if k in levels:
    return {i:0 for i in levels[k]},NCLOSURES
  else:
    return {},NCLOSURES
  
  



def a_priori_closed(T, I, sigma,test_remote):
  tid = {i:set() for i in I}
  for ti, t in enumerate(T):
    for i in t:
      tid[i].add(ti)

  #levels = diccionario.remote()
  #levels.verificar_agregar.remote(0,frozenset([]))
  NCLOSURES=0
  levels = {0:set([frozenset([])])} # Agregamos el conjunto vacío que siempre es frecuente
  for i, it in tid.items():
    if len(it) >= sigma:
      P = set([i])
      Pc = operador_clausura(P, T, I)      
      NCLOSURES+=1
      #levels.verificar_agregar.remote(len(Pc),frozenset(Pc))
      if len(Pc) not in levels:
        levels[len(Pc)] = set([])
      levels[len(Pc)].add(frozenset(Pc))


  C = {}
  i = 1
  while i < len(I):
    C[i+1], NCLOSURES= genClosedCandidates(levels, i+1, T, I,test_remote,NCLOSURES)    
    i += 1
  return levels,NCLOSURES


test_remote= ray.remote(trabajo_paralelo)
start_time = time()     
path = sys.argv[1]
with open(path, 'r') as fin:
    lines = fin.readlines()
    ctx = [set(map(int, line.split())) for line in lines]
M = set(reduce(set.union, ctx))

FC_sigma,NCLOSURES = a_priori_closed(ctx, M, 0,test_remote)
time=time()-start_time
#resultados=ray.get(FC_sigma.all.remote())
resultados=FC_sigma
largo=0        
for i,j in resultados.items():
  #print("nivel ",i)
  for w in j:
    #print('\t', sorted(w))
    largo+=1
#print("Largo: ",largo)

results = {
    'n_results' : largo,
    'n_closures' : NCLOSURES,
    'exec_time' : time,                                
}

d = datetime.now()
timestamp = '{}{}{}{}{}{}'.format(d.year, d.month, d.day, d.hour, d.minute, d.second)
fname = path[path.rfind('/'):path.rfind('.')]
fname=fname[1:]
with open('results/{}-{}-{}.json'.format("apriori_ray_resurrection",fname, timestamp) , 'w') as fout:
    json.dump(results, fout)
