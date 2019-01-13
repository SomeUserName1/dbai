# Assignment 06, Group 05 (Suckut, Klopfer)

## Status
Not implemented: 
	- Some test cases: Sorting on different fields, descending order, multi-field compatator  
The rest should be working. We implemented both advanced versions (replacement sort, merging with a tree of losers). 

## Refinements
The ExternalSort Class extends AbstractOperator and implements TupleIterator where the iterator is mainly used in open in order to peek new elements from the input in order to replace an element that gets written out or be written out themselves.  
The main logic, thus the tounament, is implemented in the LoserNode, whereas the tree itself is only for building the nodes. The next calls initiate the actual "duels".   
Testing is very basic, sorting is currently only done on the index/ byte-wise. New comparators were to be implemented, but it's my flatmates birthday, so ...  

## Time spent
Klopfer: 16h
Suckut 5h
