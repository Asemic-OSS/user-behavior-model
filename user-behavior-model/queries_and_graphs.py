import plotly.graph_objs as go
# retention query
selection_bias_query = """
-- Randomly flag users
WITH 
-- This many spends every cohort day
daily_purchase_rate AS (
    select
        registration_day,
        cohort_day,
        sum(daily_payers) / count(*) as daily_purchase_rate
    from dataset
    group by registration_day, cohort_day
),
daily_flagged AS (
    select
        dataset.*,
        if(random() < daily_purchase_rate, 1, 0) as daily_flagged
    from dataset
        inner join daily_purchase_rate using (registration_day, cohort_day)
),
flagged_data AS (
    with flagged AS (
        select
            *,
            max(daily_flagged) over (partition by user_id order by cohort_day) as flagged
        from daily_flagged
    )
    select
        *,
        flagged - lag(flagged, 1, 0) over (partition by user_id order by cohort_day) as new_flagged
    from flagged
),

-- cohorts are complete only on cohort day 0, calculate it separately
cohort_size AS (
    select
        registration_day,
        count(*) as cohort_size
    from dataset
    where cohort_day = 0
    group by registration_day
),
-- payers are another special case for calculating for each day
payers AS (
    WITH new_payers AS (
        select
            registration_day,
            cohort_day,
            sum(new_payers) as new_payers
        from dataset
        group by registration_day, cohort_day
    )
    select
        registration_day,
        cohort_day,
        sum(new_payers) over (partition by registration_day order by cohort_day) as payers
    from new_payers
),
-- 
daily_active AS (
    select
        registration_day,
        cohort_day,
        count(*) as dau,
        sum(daily_payers) as daily_payers,
        sum(payer) as mDAU,
        sum(flagged) as fDAU,
        sum(daily_flagged) as daily_flagged
    from flagged_data
    group by registration_day, cohort_day
),

-- total flagged is similar beast as payers
total_flagged AS (
    WITH daily AS (
        select
            registration_day,
            cohort_day,
            sum(new_flagged) as total_flagged
        from flagged_data
        group by registration_day, cohort_day
    )
    select
        registration_day,
        cohort_day,
        sum(total_flagged) over (partition by registration_day order by cohort_day) as total_flagged
    from daily
),

all_data AS (
    select
        registration_day,
        cohort_day,
        dau,
        mDAU,
        daily_payers,
        fDAU,
        cohort_size,
        payers,
        daily_flagged,
        total_flagged
    from daily_active
        inner join payers using (registration_day, cohort_day)
        inner join total_flagged using (registration_day, cohort_day)
        inner join cohort_size using (registration_day)
)
select
    cohort_day,
    sum(dau) / sum(cohort_size) * 100 as retention,
    sum(mDAU) / sum(payers) * 100 as payer_retention,
    sum(payers) / sum(cohort_size) * 100 as cohort_conversion,
    sum(daily_payers) / sum(dau) * 100 as daily_purchase_rate,

    sum(total_flagged) as total_flagged,
    sum(fDAU) / sum(total_flagged) * 100 as flagged_retention,
    sum(total_flagged) / sum(cohort_size) * 100 as cohort_conversion_flagged,
    sum(daily_flagged) / sum(dau) * 100 as daily_flagged_rate
from all_data
group by cohort_day
order by cohort_day
"""

engagement_score = """
drop table if exists propensity;
CREATE TABLE propensity AS 
WITH
data AS (
    select
        user_id,
        sum(active_time) as active_time
    from dataset
    where cohort_day <= {days}
    group by user_id
),
engagement_score AS (
    select
        user_id,
        cume_dist(order by active_time) over () as engagement
    from data
)
select
    user_id,
    engagement,
    random() * (1 - {corr}) + {corr} * engagement as payment_propensity
from engagement_score
"""

model_query = """
-- Flag specific users
WITH 
-- add propensity scores to each row of data
full_dataset AS (
    select
        *
    from dataset
        inner join propensity using(user_id)
),
-- figure out the pool size
pool AS (
    select
        registration_day,
        cohort_day,
        sum(if(payment_propensity >= {cutoff}, 1, 0)) as pool,
        sum(if(payment_propensity >= {cutoff}, 1, 0)) / count(*) as pool_percent
    from full_dataset
    group by registration_day, cohort_day
),

-- This many spends every cohort day
daily_purchase_rate AS (
    select
        registration_day,
        cohort_day,
        sum(daily_payers) / count(*) as daily_purchase_rate
    from dataset
    group by registration_day, cohort_day
),
daily_purchase_rate_corrected AS (
    select
        *
    from daily_purchase_rate
        inner join pool using (registration_day, cohort_day)
),
daily_flagged AS (
    select
        full_dataset.*,
        if(payment_propensity >= {cutoff}, 1, 0) *
            if(random() * pool_percent < daily_purchase_rate, 1, 0) as daily_flagged
    from full_dataset
        inner join daily_purchase_rate_corrected using (registration_day, cohort_day)
),
flagged_data AS (
    with flagged AS (
        select
            *,
            max(daily_flagged) over (partition by user_id order by cohort_day) as flagged
        from daily_flagged
    )
    select
        *,
        flagged - lag(flagged, 1, 0) over (partition by user_id order by cohort_day) as new_flagged
    from flagged
),

-- cohorts are complete only on cohort day 0, calculate it separately
cohort_size AS (
    select
        registration_day,
        count(*) as cohort_size
    from dataset
    where cohort_day = 0
    group by registration_day
),
-- payers are another special case for calculating for each day
payers AS (
    WITH new_payers AS (
        select
            registration_day,
            cohort_day,
            sum(new_payers) as new_payers
        from dataset
        group by registration_day, cohort_day
    )
    select
        registration_day,
        cohort_day,
        sum(new_payers) over (partition by registration_day order by cohort_day) as payers
    from new_payers
),
-- 
daily_active AS (
    select
        registration_day,
        cohort_day,
        count(*) as dau,
        sum(daily_payers) as daily_payers,
        sum(payer) as mDAU,
        sum(flagged) as fDAU,
        sum(daily_flagged) as daily_flagged
    from flagged_data
    group by registration_day, cohort_day
),

-- total flagged is similar beast as payers
total_flagged AS (
    WITH daily AS (
        select
            registration_day,
            cohort_day,
            sum(new_flagged) as total_flagged
        from flagged_data
        group by registration_day, cohort_day
    )
    select
        registration_day,
        cohort_day,
        sum(total_flagged) over (partition by registration_day order by cohort_day) as total_flagged
    from daily
),

all_data AS (
    select
        registration_day,
        cohort_day,
        dau,
        mDAU,
        daily_payers,
        fDAU,
        cohort_size,
        payers,
        daily_flagged,
        total_flagged
    from daily_active
        inner join payers using (registration_day, cohort_day)
        inner join total_flagged using (registration_day, cohort_day)
        inner join cohort_size using (registration_day)
)
select
    cohort_day,
    sum(dau) / sum(cohort_size) * 100 as retention,
    sum(mDAU) / sum(payers) * 100 as payer_retention,
    sum(payers) / sum(cohort_size) * 100 as cohort_conversion,
    sum(daily_payers) / sum(dau) * 100 as daily_purchase_rate,

    sum(total_flagged) as total_flagged,
    sum(fDAU) / sum(total_flagged) * 100 as flagged_retention,
    sum(total_flagged) / sum(cohort_size) * 100 as cohort_conversion_flagged,
    sum(daily_flagged) / sum(dau) * 100 as daily_flagged_rate
from all_data
group by cohort_day
order by cohort_day
"""

def show_retention(df, metric_list=['retention', 'payer_retention', 'flagged_retention']):
    # Create the Plotly figure
    fig = go.Figure()

    color = {'retention': 'blue', 'payer_retention': 'red', 'flagged_retention': 'orange'}

    for metric in metric_list:
        # Add Overall Retention trace
        fig.add_trace(go.Scatter(
            x=df['cohort_day'], 
            y=df[metric],
            mode='lines',
            name=_pretty_name(metric),
            line=dict(color=color[metric], width=2),
            marker=dict(size=8, symbol='circle')
        ))

    # Customize the layout
    fig.update_layout(
        title='Retention',
        xaxis_title='Days Since Cohort Start',
        yaxis_title='Retention Rate (%)',
        xaxis=dict(range=[0,180]),
        template='plotly_white',
        width=600,
        height=400,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99
        )
    )
    return fig

def show_cohort_conversion(df, metric_list=['cohort_conversion', 'cohort_conversion_flagged']):
    # Create the Plotly figure
    fig = go.Figure()

    color = {'cohort_conversion': 'red', 'cohort_conversion_flagged': 'orange'}

    x_right = 0
    y_top = 0
    for metric in metric_list:
        y_top = max(y_top, max(df[metric]))
        x_right = max(x_right, max(df['cohort_day']))
    
    for metric in metric_list:
        # Add Cohort Conversion trace
        fig.add_trace(go.Scatter(
            x=df['cohort_day'], 
            y=df[metric],
            mode='lines',
            name=_pretty_name(metric),
            line=dict(color=color[metric], width=2),
            marker=dict(size=8, symbol='circle')
        ))

    # Customize the layout
    fig.update_layout(
        title='Cohort Conversion',
        xaxis_title='Days Since Cohort Start',
        yaxis_title='Cohort Conversion (%)',
        xaxis=dict(range=[0,180]),
        template='plotly_white',
        width=600,
        height=400,
        legend=dict(
            yanchor="bottom",
            y=0.01,
            xanchor="right",
            x=0.99
        )
    )
    return fig

def show_daily_purchase(df, metric_list=['daily_purchase_rate', 'daily_flagged_rate']):
    # Create the Plotly figure
    fig = go.Figure()

    color = {'daily_purchase_rate': 'red', 'daily_flagged_rate': 'orange'}

    for metric in metric_list:
        # Add Daily Purchase trace
        fig.add_trace(go.Scatter(
            x=df['cohort_day'], 
            y=df[metric],
            mode='lines',
            name=_pretty_name(metric),
            line=dict(color=color[metric], width=2),
            marker=dict(size=8, symbol='circle')
        ))

    # Customize the layout
    fig.update_layout(
        title='Daily Purchase',
        xaxis_title='Days Since Cohort Start',
        yaxis_title='Cohort Conversion (%)',
        xaxis=dict(range=[0,180]),
        template='plotly_white',
        width=600,
        height=400,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99
        )
    )
    return fig

def show_error(df, metric1, metric2):
    # Create the Plotly figure
    fig = go.Figure()

    color = {metric1: 'blue', 'error': 'orange', metric2: 'green'}

    y1 = df[metric1]
    y2 = df[metric2]
    error = [abs(a1 - a2) / (a1+a2)   for a1, a2 in zip(y1, y2)]

    # Add Overall Retention trace
    fig.add_trace(go.Scatter(
        x=df['cohort_day'], 
        y=error,
        mode='lines',
        name=_pretty_name('error'),
        line=dict(color=color['error'], width=2),
        marker=dict(size=8, symbol='circle')
    ))

    # Customize the layout
    fig.update_layout(
        title='Error',
        xaxis_title='Days Since Cohort Start',
        yaxis_title='Error',
        xaxis=dict(range=[0,180]),
        template='plotly_white',
        width=600,
        height=400,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99
        )
    )
    return fig

def _pretty_name(s):
    s = " ".join([x.capitalize() for x in s.split("_")])
    return s


