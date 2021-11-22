from functools import reduce

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

def derive(P, E):
    '''
    Returns P'
    '''
    return set([ei for ei, e in E.items() if P.issubset(e)])

def close_intent(X, ctx):
    '''
    Returns X''
    '''
    Xp = derive(X, ctx[0])
    if not bool(Xp):
        return set(ctx[1].keys())
    return set(reduce(set.intersection, [ctx[0][g] for g in Xp]))

    #return set([ti for ti, t in T.items() if P.issubset(t)])