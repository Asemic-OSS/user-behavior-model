import duckdb
import pandas as pd
from numpy import arange
from queries_and_graphs import *


def error(df):
    """
    Calculate the error between pairs of metrics in the provided DataFrame.

    This function computes the error between payer retention and flagged retention,
    as well as between cohort conversion and cohort conversion flagged. For each pair,
    it sums the normalized absolute differences and returns the maximum of these sums.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the columns 'payer_retention', 'flagged_retention',
        'cohort_conversion', and 'cohort_conversion_flagged'.
        
    Returns
    -------
    float
        The maximum of the two calculated error sums, representing the larger 
        discrepancy between the two pairs of metrics.
        
    Notes
    -----
    The error for each pair of metrics is calculated as the sum of 
    |a1 - a2| / (a1 + a2) for corresponding values in the two columns.
    This provides a normalized measure of difference between the metrics.

    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'payer_retention': [90, 80, 70],
    ...     'flagged_retention': [85, 75, 65],
    ...     'cohort_conversion': [10, 15, 20],
    ...     'cohort_conversion_flagged': [12, 17, 22]
    ... })
    >>> error(df)
    0.06914985590778098
    """

    y1 = df['payer_retention']
    y2 = df['flagged_retention']
    error_ret = sum([abs(a1 - a2) / (a1+a2)   for a1, a2 in zip(y1, y2)])

    y1 = df['cohort_conversion']
    y2 = df['cohort_conversion_flagged']
    error_con = sum([abs(a1 - a2) / (a1+a2)   for a1, a2 in zip(y1, y2)])
    return max(error_ret, error_con)

def grid_search(correlation = [0, 1], cutoff_points = [0, 1], resolution = 0.1, nfold=1, silent=False):
    """
    Perform a grid search to find optimal correlation and cutoff parameters for user retention modeling.

    This function creates a grid of correlation and cutoff values, evaluates model performance
    at each grid point, and returns error metrics for all combinations.

    Parameters
    ----------
    correlation : list of float, default=[0, 1]
        Range of correlation values to search, specified as [min, max].
        The correlation parameter controls how strongly engagement scores are related to payment propensity.

    cutoff_points : list of float, default=[0, 1]
        Range of cutoff values to search, specified as [min, max].
        The cutoff parameter determines the threshold for considering a user as a potential payer.

    resolution : float, default=0.5
        Step size for the search grid. Smaller values create a finer grid but increase computation time.

    nfold : int, default=1
        Number of evaluation repetitions for each parameter combination.
        Higher values reduce random variation but increase computation time.

    silent : bool, default=False
        If True, suppresses progress output. Set to True for non-interactive execution.

    Returns
    -------
    tuple
        A tuple containing three elements:
        - results : list of lists
            A 2D list where results[i][j] is the error for correlation x[i] and cutoff y[j].
        - x : numpy.ndarray
            Array of correlation values used in the grid search.
        - y : numpy.ndarray
            Array of cutoff values used in the grid search.

    Notes
    -----
    This function:
    1. Creates an in-memory DuckDB database and loads the dataset
    2. Generates a grid of parameter values based on the specified ranges and resolution
    3. For each parameter combination, evaluates the model nfold times and averages the error
    4. Returns the full grid of results for visualization and analysis

    The function filters data to include only records with cohort_day between 1 and 179 inclusive.

    Examples
    --------
    >>> results, x, y = grid_search(correlation=[0.3, 0.6], cutoff_points=[0.7, 0.9], resolution=0.1)
    >>> fig = heatmap(x, y, results)
    >>> fig.show()
    """

    # Connect to an in-memory database
    con = duckdb.connect(':memory:')

    # Read the CSV file
    con.execute("CREATE TABLE dataset AS SELECT * FROM read_csv('data/dataset.csv')")

    x = arange(correlation[0], correlation[1], resolution)
    y = arange(cutoff_points[0], cutoff_points[1], resolution)
    results = []
    for corr in x:
        subres = []
        for cutoff in y:
            err = 0
            for _ in range(nfold):
                con.execute(engagement_score.format(corr=corr, days = 60))

                df = con.execute(model_query.format(cutoff=cutoff)).df()
                df = df[(df['cohort_day'] > 0) & (df['cohort_day'] < 180)]
                err += error(df)
            err /= nfold
            subres.append(err)
            if not silent:
                print(corr, cutoff, err)
        results.append(subres)
    return results, x, y

def heatmap(x, y, z):
    """
    Draws output of grid search as a heatmap
    """
    text = []
    for sub in z:
        text.append([str(round(a)) for a in sub])
    fig = go.Figure(data=go.Heatmap(
                    z=z,
                    x=x,
                    y=y,
                    text = text,
                    texttemplate="%{text}",
                    textfont={"size":12},
                    hoverongaps = False))
    return fig
