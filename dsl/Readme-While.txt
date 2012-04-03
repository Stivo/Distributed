This is a description of this experiments-while branch.

I tried to get iterative algorithms to work, at least for spark and scoobi.
Then my current assumption, that all the vector nodes are in the top scope,
does not hold anymore. Dependencies are pulled in and replicated in a new scope.
I will need a way to see what ttps belong in which scope. 

Other than that it was complaining a lot about effect order violations,
and even with hardcoding what to pull in it did not generate correct code yet.
