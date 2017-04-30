
# Finding Frequent Item
Reference: Mining of Massive Dataset by Jure Leskovec, Anand Rajaraman, Jeff Ullman.
Chapter 6
Book Url: http://infolab.stanford.edu/~ullman/mmds/ch6.pdf


```python
import numpy as np
```

## A-Priori Algorithm
Suppose we have transactions that satisfy the following assumptions:
s, the support threshold, is 10,000.

There are one million items, which are represented by the integers 0,1,...,999999.

There are N frequent items, that is, items that occur 10,000 times or more.

There are one million pairs that occur 10,000 times or more.

There are 2M pairs that occur exactly once. M of these pairs consist of two frequent items, the other M each have at least one nonfrequent item.

No other pairs occur at all.

Integers are always represented by 4 bytes.

Suppose we run the a-priori algorithm to find frequent pairs and can choose on the second pass between the triangular-matrix method for counting candidate pairs (a triangular array count[i][j] that holds an integer count for each pair of items (i, j) where i < j) and a hash table of item-item-count triples. Neglect in the first case the space needed to translate between original item numbers and numbers for the frequent items, and in the second case neglect the space needed for the hash table. Assume that item numbers and counts are always 4-byte integers.

As a function of N and M, what is the minimum number of bytes of main memory needed to execute the a-priori algorithm on this data? Demonstrate that you have the correct formula by selecting, from the choices below, the triple consisting of values for N, M, and the (approximate, i.e., to within 10%) minumum number of bytes of main memory, S, needed for the a-priori algorithm to execute with this data.


```python
#Number of frequent items
N = 10000
#Number of frequent pairs
nfp = 10**6 # One million
#Number of other pairs with counts
M = 50*10**6
##################################
#Calculation
##################################
#Number of candidate pairs (approximation of N choose 2)
c2 = N**2/2
#Memory needed for triangular array
tri_array_size = c2*4
#Memory needed for triples 
triple_size = (nfp+M)*12
```


```python
#output the less memory
min(tri_array_size,triple_size)
```




    200000000.0



## PCY Algorithm
Suppose we perform the PCY algorithm to find frequent pairs, with market-basket data meeting the following specifications:

s, the support threshold, is 10,000.

There are one million items, which are represented by the integers 0,1,...,999999.

There are 250,000 frequent items, that is, items that occur 10,000 times or more.

There are one million pairs that occur 10,000 times or more.

There are P pairs that occur exactly once and consist of 2 frequent items.

No other pairs occur at all.

Integers are always represented by 4 bytes.

When we hash pairs, they distribute among buckets randomly, but as evenly as possible; i.e., you may assume that each bucket gets exactly its fair share of the P pairs that occur once.

Suppose there are S bytes of main memory. In order to run the PCY algorithm successfully, the number of buckets must be sufficiently large that most buckets are not frequent. In addition, on the second pass, there must be enough room to count all the candidate pairs. As a function of S, what is the largest value of P for which we can successfully run the PCY algorithm on this data? Demonstrate that you have the correct formula by indicating which of the following is a value for S and a value for P that is approximately (i.e., to within 10%) the largest possible value of P for that S.


```python
# For PCY Algorithm
#Main Memory in Bytes
S = 300000000
#Number of frequent pairs
nfp = 10**6 # One million
##################################
#Calculation
##################################
#Number of basket
nbasket = S/4.
#fraction of frequent baskets (probability)
frac_fb = nfp/nbasket
```


```python
#Maximun P
P = S/(12*frac_fb)
```


```python
num_digits = np.floor(np.log10(P))
round_num = int(10**(num_digits-1))
```


```python
int((P//round_num)*round_num)
```




    1800000000



## Multi-stage
Suppose we perform the 3-pass multistage algorithm to find frequent pairs, with market-basket data meeting the following specifications:

s, the support threshold, is 10,000.

There are one million items, which are represented by the integers 0,1,...,999999.

All one million items are frequent; that is, they occur at least 10,000 times.

There are one million pairs that occur 10,000 times or more.

There are P pairs that occur exactly once.

Integers are always represented by 4 bytes.

When we hash pairs, they distribute among buckets randomly, but as evenly as possible; i.e., you may assume that each bucket gets exactly its fair share of the P pairs that occur once.

The hash functions on the first two passes are completely independent.

Suppose there are S bytes of main memory. As a function of S and P, what is the exected number of candidate pairs on the third pass of the multistage algorithm? Demonstrate the correctness of your formula by dentifying which of the following triples of values for S, P, and N is N approximately (i.e., to within 10%) the expected number of candidate pairs for the third pass.


```python
#Input here
S = '300,000,000'
P = '100,000,000,000'
```


```python
# For Multi-stage Algorithm
#Main Memory in Giga Bytes
S = int(S.replace(',',''))
#Number of frequent pairs
nfp = 10**6 # One million
#Number of other pairs with counts
P = int(P.replace(',',''))
##################################
#Calculation
##################################
#Number of basket
nbasket = S/4.
#fraction of frequent baskets (Probability that candiate pairs hashed to frequent basket in a pass)
frac_fb = nfp/nbasket
#Candidate pair for counting
N = P*(frac_fb**2)+nfp
```


```python
num_digits = np.floor(np.log10(N))
round_num = int(10**(num_digits-2))
```


```python
int((N//round_num)*round_num)
```




    18700000



## Binary Search or Triangular Matrix?
In this problem, assume all integers and pointers occupy 4 bytes. The assumption that we count represent pair-counts with triples (i,j,c) for the pair i, j with count c does not account for the space needed to build an efficient data structure to find i-j pairs when we need them. Suppose we use a binary search tree, where each node is a quintuple (i,j,c,leftChild,rightChild). Suppose also that there are I items, and P pairs that actually appear in the data. Under what circumstances does it save space to use the above binary-search tree rather than a triangular matrix?


```python
I = 20000; P = 30000000
```


```python
2*I**2 > P*20
```




    True


