import pandas as pd
import numpy as np

class DataAnalyzer:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def summarize(self):
        return {
            "rows": len(self.df),
            "cols": list(self.df.columns),
            "dtypes": self.df.dtypes.astype(str).to_dict(),
            "numeric_cols": list(self.df.select_dtypes(include=[np.number]).columns),
            "sample": self.df.head(3).to_dict(orient="records")
        }

    def filter(self, **conditions):
        q = " & ".join([f"`{k}` == @{k}" for k in conditions])
        return self.df.query(q, local_dict=conditions)

    def join(self, other: 'DataAnalyzer', on, how="inner"):
        return DataAnalyzer(self.df.merge(other.df, on=on, how=how))

    def group_stat(self, group_col, agg_map):
        """Example: {'rainfall':'mean','production':'sum'}"""
        grouped = self.df.groupby(group_col).agg(agg_map).reset_index()
        return DataAnalyzer(grouped)

    def correlation(self, col_x, col_y):
        if col_x not in self.df or col_y not in self.df:
            raise KeyError("columns missing")
        return self.df[col_x].corr(self.df[col_y])

    def rank(self, col, ascending=False, n=5):
        ranked = self.df.sort_values(by=col, ascending=ascending).head(n)
        return ranked

    def trend(self, value_col, time_col):
        return self.df.groupby(time_col)[value_col].mean().reset_index()

    def export(self):
        """For LLM to inspect / for logging."""
        return self.df.to_dict(orient="records")