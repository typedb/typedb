The idea we want is:
for all permutation answers (a,b,c...) in Answer–Permutations(Q), we want to form a group of sets (A, B, C...) such that all a's are in A, b's are in B, and c's are in C etc. Here A, B, C are the sets of types for the first, second third variables that the execution visits

the other way of stating this is:
we want a group of sets (A, B, C... ) such that all a's in A satisfy that there is some (a, , , ...) in Answer-Permutations(Q), all b's in B satisfy...

so if we had a tree structure without any closures/graphy-ness you can just take all the answers from the first vertex, then get all the vertices of all the neighbors in DFS or BFS way to generate the sets A, B, C

so I had already implemented this and it kinda works, until I hit closures, for example, let's say you have a triangular query: A - B - C - A
- let's start with the first a1 in A
- let's take a1, and try to expand B, and we get b1
- expand C: take b1 and get c1
- we require c1 to have a closure with a1 to satisfy our query

1. if it does, it's easy - we have a valid permutation so a1, b1, c1 should be in our answer sets
2. if it does not, it may still be in the answer sets we're looking for

so let's say c1 connects to a99 instead of a1. Well, we only want to accept the answer (a1, b1, c1) in our answer sets if we can also show that (a99, ?, ?) is a valid permutation now.
The key point is that we should be only including a type in the answer set IF there's a valid permutation that contains that type in the answer set. But to do this with closures is hard, because a closure can eliminate a permutation
which you can't really check without computing a permutation


## Ideas

- there is some structure in the problem (eg we are operating over of map of sets, not permutations of maps of singletons)
- memoisation seems like a valid approach: mark types that have been explored successfully or unsuccessfully explored, so we don't re-explore them
   this is a halfway to computing permutations, but we can stop early when we visit a partial exploration that has failed or succeeded
- can we perform a forward-backward algorithm? find the first valid permutation (forward). Using the types that are now known to work, work backward somehow
    to find a variations on this permutation that are also valid
    open questions: how do you work backward given things like closures, and also when the initial seed permutation is no longer providing answers,
    how do we generate the next valid permutation that will generate new type assignments?
- forward-backward structure:
    to go backward from a known permutation, we could substitute all alternative types into a variable, and keep the other found types fixed. 
    Then, we only have to locally check each direct neighbor to see whether this new type is valid in the fixed context. This is much faster than
    re-exploring the entire graph with the substituted label.
- naiive approach: for each variable, assign a possible type and find a single permutation that works to prove it, or find none to disprove it. Perform for each variable
    note: can still be costly: finding a single answer (or proving there are none) can check many intermediate answers. Not sure this is avoidable though!
- what is the real cost of the type resolver: probably an explosion in the number of valid permutations that overlap strongly.
    in this case, we don't care about the cost of finding a single permutation, just want to avoid finding many permutations. The naiive approach
    may then work out ok? Or something derived from it.
  

    