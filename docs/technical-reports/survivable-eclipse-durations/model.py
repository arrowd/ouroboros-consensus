# This script merely needs the numpy and scipy packages.
#
# See the model in the adjacent `./model.md` file. For given Delta and CUTOFF
# parameters, this script finds the X and Y pairs that minimize EPSILON.

import numpy as np
from scipy.stats import nbinom

import sys

# Every function in this module that takes multiple parameters is declared to
# take a first parameter of *. The only reason for that is to force all
# call-sites to use named parameters.

ar = 1/3
hr = 1 - ar

ascRecip = 20
asc      = 1 / ascRecip
k        = 2160

phi = lambda alpha: 1 - (1 - asc) ** alpha

mu = lambda *, Delta, W: W/( Delta + 1/phi(hr) )

# Find X, Y, CUTOFF, and EPSILON such that Pr(t1 >= X || d >= Y) + Pr(BAD | t1 < X, d < Y) <= EPSILON

# u = e^(-2 (1 + mu(X) - k)^2 / X), assuming mu(X) >= k
# v = e^(-0.0716 - 0.0523 Y), assuming ar=1/3
# Pr(t1 >= X || d >= Y) <= 1 - (1 - u)(1 - v)

minX = lambda *, Delta: np.ceil(k * ( Delta + 1/phi(hr) ))   # the least X such that mu(X) >= k

u = lambda *, Delta, X: np.exp(-2 * (1 + mu(Delta=Delta, W=X) - k)**2 / X)   # this requires mu(X) >= k
v = lambda *, Y: np.exp(-0.0716 - 0.0523 * Y)   # this requires ar=1/3 

def part1(*, Delta, X, Y):
    us = u(Delta=Delta, X=X)
    vs = v(Y=Y)
    return us + vs - us * vs

# Pr(BAD | t1 < X, d < Y) <= Pr(BAD | t1=X-1, d=Y-1), ie NegativeBinomialTrials(k - Y + 1, phi(ar)).cdf(CUTOFF + X - Y)

def part2(*, X, Y, CUTOFF):
  successes = k - Y + 1
  trials    = CUTOFF + X - Y
  failures  = trials - successes
  return nbinom(successes, phi(ar)).cdf(failures)

def search(*, Delta, CUTOFF):
    megaX = np.arange(minX(Delta=Delta), 1 + 3 * k * ascRecip)   # all possible X values
    megaY = np.arange(0, 1 + k)[np.newaxis,:]   # all possible Y values

    # each iteration of this loop requires about 1G of memory at once
    minEpsilon = np.inf
    stride     = 10
    for offset in range(stride):
        X = megaX[offset::stride,np.newaxis]
        Y = megaY

        epsilon       = part1(Delta=Delta, X=X, Y=Y) + part2(X=X, Y=Y, CUTOFF=CUTOFF)
        newMinEpsilon = epsilon.min()

        if newMinEpsilon < minEpsilon:
            minEpsilon = newMinEpsilon

            xindices, yindices = np.where(epsilon == minEpsilon)

            xys      = np.empty((2, len(xindices)))
            xys[0,:] = X[xindices,0]
            xys[1,:] = Y[0,yindices]

            del xindices, yindices
    
    del megaX, megaY, X, Y, stride

    print(f"ar=1/3 Delta={Delta} CUTOFF={CUTOFF} allows a minimum of EPSILON={minEpsilon}.")

    print("X and Y pairs with that EPSILON value (first row is X, second is Y):")
    print(xys)

    parts      = np.empty((3, len(xys[0])))
    parts[0,:] = u(Delta=Delta, X=xys[0,:])
    parts[1,:] = v(Y=xys[1,:])
    parts[2,:] = part2(X=xys[0,:], Y=xys[1,:], CUTOFF=CUTOFF)

    print("The three first-order summands of the corresponding EPSILON calculations (first row is u, second is v, third is Pr(BAD | t1=X-1, d=Y-1):")
    print(parts)

if __name__ == "__main__":
    search(Delta=int(sys.argv[1]), CUTOFF=int(sys.argv[2]))
