import os
import pandas as pd
import ipdb
from datetime import datetime, time

# CONVERT


def convert(fn):
    libs = sorted(pd.read_csv("libraries.csv").full_name.tolist())
    xl = pd.ExcelFile(fn)
    sheets = xl.sheet_names
    res = []
    for sheet in sheets:
        df = xl.parse(sheet)
        date = datetime.strptime(df.columns[0], "%d %B %Y")
        df = (
            df.loc[df["Counter Name"].isin(libs)]
            .rename(columns={df.columns[0]: "dt"})
            .fillna("x")
        )
        # times = df.loc[df.dt != 'x']
        df.dt = df.dt.replace(to_replace="x", method="ffill")
        dt = list(
            map(lambda x: datetime.combine(date, time(int(x[:2]), 0)), df.dt.tolist())
        )
        df.dt = dt
        res.append(df)

    return pd.concat(res).rename(columns={"Counter Name": "Library"})


def to_hours(x):
    return pd.to_datetime(x).hour


# ADD OPENING TYPE


def get_ot(dt, lib):
    oh = pd.read_csv(
        "opening hours.csv",
        converters={"start": lambda x: to_hours(x), "finish": lambda x: to_hours(x)},
    )
    in_df = pd.concat([dt, lib], axis=1)
    res = []

    for row in in_df.itertuples():
        day = row.dt.day_name()[:3]
        mask = (
            (oh.library == row.Library)
            & (oh.day == day)
            & (oh.start <= row.dt.hour)
            & (oh.finish > row.dt.hour)
        )
        if oh.loc[mask].empty:
            res.append("closed")
        else:
            ot = oh.loc[mask].opening_type.values[0]
            res.append(ot)
    return pd.Series(res)


def get_df_with_opening_hours(fn):
    df = convert(fn)
    ots = get_ot(df.dt, df.Library)
    df["opening_type"] = ots.values
    return df
