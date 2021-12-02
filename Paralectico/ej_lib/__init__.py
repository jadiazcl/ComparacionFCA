from functools import reduce
import numpy as np

def soporte(P, T):
  '''
    Calcula el soporte del conjunto P sobre el conjunto de transacciones T
  '''
  resultado = 0
  for t in T:
    if P.issubset(t):
      resultado += 1
  return resultado

def operador_clausura(P, T):
  '''
  Returns P''
  '''
  I = sorted(reduce(set.union, T))
  Pp = [t for t in T if P.issubset(t)] #P'
  # Si el soporte de P es 0, su clausura es el conjunto I - Considere demostrar que esto es cierto
  if not bool(Pp):
    return set(I)
  return set(reduce(set.intersection, Pp))

def issubset(X,Y):# check X \subseteq Y
  # print('\t\t\t\t <=', X, Y, end='')
  m = len(X)
  n = len(Y)
  if n < m:
    # print(False)
    return False
  i = 0
  j = 0
  
  while i < m:
    while X[i] > Y[j]:
      j += 1
      if j >= n:
        # print(False)
        return False
    if X[i] == Y[j] and m-i <= n-j:
      i += 1
    else:
      # print(False)
      return False
  # print(True)
  return True
  
def derive_attributes(X, g_prime, m_prime, flag=0):
  if X.size==0:
    return np.array(range(len(g_prime)))
  return reduce(lambda x,y: np.intersect1d(x,y, assume_unique=True), (m_prime[m] for m in X)) 

def derive_attributesX(X, g_prime, m_prime, flag=0):
  if X.size==0:
    return np.array(range(len(g_prime))) #set(ctx[1].keys())
  Z = m_prime[X[0]]#
  for m in X[1:]:
    aux = np.concatenate((Z, m_prime[m]))#
    aux.sort()
    mask = aux[:-1] == aux[1:]
    Z = aux[:-1][mask]
  return Z


def derive_objectsg(X, g_prime, m_prime, flag=0):
  if X.size==0:
    return np.array(range(len(m_prime)))
  Z = np.array([m for m, mp in enumerate(m_prime) if np.isin(X, mp, assume_unique=True).all()])
  return Z

def derive_objectsc(X, g_prime, m_prime, flag=0):
  if X.size==0:
    return np.array(range(len(m_prime))) #set(ctx[1].keys())
  Z = g_prime[X[0]]#
  for g in X[1:]:
    aux = np.concatenate((Z, g_prime[g]))#
    aux.sort()
    mask = aux[:-1] == aux[1:]
    Z = aux[:-1][mask]
  return Z

def derive_objectsX(X, g_prime, m_prime, flag=0):
  if X.size==0:
    return np.array(range(len(m_prime))) #set(ctx[1].keys())
  Z = np.concatenate([g_prime[g] for g in X])
  Z.sort()
  offset=len(X)-1
  mask = np.ones((1,len(Z)-offset), dtype=bool).ravel()
  
  for i in range(offset):
    mask &= Z[i:len(Z)-(offset-i)] == Z[i+1:len(Z)-(offset-i)+1]
  return Z[:len(Z)-offset][mask]

def derive_objects(X, g_prime, m_prime, flag=0):
  if X.size==0:
    return np.array(range(len(m_prime))) #set(ctx[1].keys())
  return reduce(lambda x,y: np.intersect1d(x,y, assume_unique=True), (g_prime[g] for g in X))


def derive(X, E, flag=0):
  '''
  Returns P'
  '''
  # R = [] 
  # for ei, Y in enumerate(E):
  #   A =  issubset(X, Y)
  #   B = np.isin(X,Y, assume_unique=True).all()
  #   if A!= B:
  #     print(flag, X, Y)
  #   if issubset(X, Y):# np.isin(X,Y).all():
  #     R.append(ei)
  return np.array([ ei for ei, Y in enumerate(E) if np.isin(X,Y, assume_unique=True).all() ]) # issubset(X, Y) ])
    
def close_intent(X, g_prime, m_prime, flag=0):
    '''
    Returns X''
    '''
    # Xp = derive(X, g_prime, flag=flag)
    Xp = derive_attributes(X, g_prime, m_prime, flag=flag)
    return derive_objects(Xp, g_prime, m_prime, flag=flag)
    # if Xp.size == 0:
    #     return np.array(range(len(m_prime))) #set(ctx[1].keys())
    # return reduce(lambda x,y: np.intersect1d(x,y, assume_unique=True), (g_prime[g] for g in Xp))

    #return set([ti for ti, t in T.items() if P.issubset(t)])