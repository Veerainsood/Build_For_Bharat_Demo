import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

def safe(df):
    """Return a defensive copy of df."""
    return df.copy() if isinstance(df, pd.DataFrame) else df

# --- Filtering & Cleaning -------------------------------------------------
def parseDf(df):
    if isinstance(df,str):
        return df[1::-2]
    return df

def filter_rows(df, condition):
    try:
        return safe(df).query(condition)
    except Exception:
        return df

def drop_missing(df, cols=None):
    return safe(df).dropna(subset=cols)

def fill_missing(df, cols=None, method="mean"):
    df = safe(df)
    if cols is None:
        cols = df.columns
    for c in cols:
        if method == "mean" and np.issubdtype(df[c].dtype, np.number):
            df[c] = df[c].fillna(df[c].mean())
        elif method == "median":
            df[c] = df[c].fillna(df[c].median())
        else:
            df[c] = df[c].fillna(0)
    return df

def rename_columns(df, mapping):
    return safe(df).rename(columns=mapping)

def select_columns(df, cols):
    return safe(df)[[c for c in cols if c in df.columns]]

def sort_rows(df, by, ascending=True):
    return safe(df).sort_values(by=by, ascending=ascending)

def remove_duplicates(df, subset=None):
    return safe(df).drop_duplicates(subset=subset)

def filter_date_range(df, col, start, end):
    if col not in df.columns:
        return df
    df = safe(df)
    df[col] = pd.to_datetime(df[col], errors="coerce")
    return df[(df[col] >= start) & (df[col] <= end)]

# --- Aggregation & Grouping ----------------------------------------------

def group_by_mean(df, key, cols):
    return safe(df).groupby(key)[cols].mean().reset_index()

def group_by_sum(df, key, cols):
    return safe(df).groupby(key)[cols].sum().reset_index()

def group_by_median(df, key, cols):
    return safe(df).groupby(key)[cols].median().reset_index()

def group_by_count(df, key):
    return safe(df).groupby(key).size().reset_index(name="count")

def aggregate_multiple(df, key, agg_map):
    return safe(df).groupby(key).agg(agg_map).reset_index()

def pivot_table(df, index, columns, values, aggfunc="mean"):
    return safe(df).pivot_table(index=index, columns=columns, values=values, aggfunc=aggfunc)

def flatten_multiindex(df):
    df = safe(df)
    df.columns = ['_'.join(map(str, col)).strip() for col in df.columns.values]
    return df.reset_index(drop=True)

# --- Joining & Combining --------------------------------------------------

def merge_dfs(df1, df2, on, how="inner"):
    return pd.merge(safe(df1), safe(df2), on=on, how=how)

def concat_dfs(dfs, axis=0):
    return pd.concat([safe(d) for d in dfs], axis=axis, ignore_index=True)

def align_columns(df1, df2):
    common = list(set(df1.columns) & set(df2.columns))
    return safe(df1)[common], safe(df2)[common]

def lookup_value(df, key_col, key_val, target_col):
    match = safe(df).loc[df[key_col] == key_val, target_col]
    return match.iloc[0] if len(match) else None

def add_computed_column(df, new_col, expr):
    df = safe(df)
    df[new_col] = df.eval(expr)
    return df

# --- Trend, Correlation & Stats ------------------------------------------

def compute_correlation(df1, df2, cols1, cols2):
    common1 = [c for c in cols1 if c in df1.columns]
    common2 = [c for c in cols2 if c in df2.columns]
    merged = pd.concat([df1[common1].reset_index(drop=True),
                        df2[common2].reset_index(drop=True)], axis=1)
    return merged.corr()

def yearly_trend(df, year_col, value_col):
    df = safe(df).dropna(subset=[year_col, value_col])
    X = df[[year_col]].values.reshape(-1, 1)
    y = df[value_col].values.reshape(-1, 1)
    model = LinearRegression().fit(X, y)
    df["trend"] = model.predict(X)
    return df

def moving_average(df, col, window=3):
    df = safe(df)
    df[f"{col}_ma{window}"] = df[col].rolling(window).mean()
    return df

def percentage_change(df, col):
    df = safe(df)
    df[f"{col}_pct_change"] = df[col].pct_change() * 100
    return df

def normalize_column(df, col):
    df = safe(df)
    df[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())
    return df

def standardize_column(df, col):
    df = safe(df)
    df[col] = (df[col] - df[col].mean()) / df[col].std()
    return df

def describe_stats(df):
    return safe(df).describe()

def detect_outliers(df, col, z_thresh=3):
    df = safe(df)
    z = np.abs((df[col] - df[col].mean()) / df[col].std())
    return df[z > z_thresh]

def aggregate_trend(df, group_col, value_col):
    df = safe(df)
    trends = []
    for name, group in df.groupby(group_col):
        if len(group) > 1:
            X = np.arange(len(group)).reshape(-1, 1)
            y = group[value_col].values
            slope = LinearRegression().fit(X, y).coef_[0]
            trends.append((name, slope))
    return pd.DataFrame(trends, columns=[group_col, "trend_slope"])

def compare_means(df, group_col, value_col):
    return safe(df).groupby(group_col)[value_col].mean().reset_index()
