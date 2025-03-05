# User Behavior Model

This model is created as a result of many trials and errors that won't be all documented in this iteration.

I present you hear straight-to-the-point thinking and results with enough documentation that can enable you to replicate results on your own data.

## Approachable model

Data manipulation was written in SQL, which means you could implement it in pure BI tool, like Tableau, if you are not a python or R user.

## Testing the model

Run `requirements.txt` and provide your dataset in the correct form.

`fit.py` implements brute-force grid search for parameters; it is advised to use one of the standard libraries for parameter fitting.

`queries_and_graphs.py` has three queries for data transformations which are well structured and commented and has auxiliary functions for drawing specific metrics.