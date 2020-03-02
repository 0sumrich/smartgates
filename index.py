import os
import pandas as pd
import ipdb
from datetime import datetime, time

# CONVERT
# df.rename(columns={'dt':'datetime', 'Library':'library', 'People In':'people_in', 'People Out':'people_out'})


def date_from_ff_excel(s):
    if s.rfind(",") > -1:
        return datetime.strptime(s, "%A, %B %d, %Y").date()
    else:
        return datetime.strptime(s, "%d %B %Y").date()


def time_from_ff_excel(s):
    if len(s) > 5:
        return datetime.strptime(s, "%I:%M %p").time()
    else:
        return datetime.strptime(s, "%H:%M").time()


def convert(fn):
    libs = sorted(pd.read_csv("libraries.csv").full_name.tolist())
    xl = pd.ExcelFile(fn)
    sheets = xl.sheet_names
    res = []
    for sheet in sheets:
        df = xl.parse(sheet)
        date = date_from_ff_excel(df.columns[0])
        # datetime.strptime(df.columns[0], "%d %B %Y")
        df = (
            df.loc[df["Counter Name"].isin(libs)]
            .rename(columns={df.columns[0]: "dt"})
            .fillna("x")
        )
        # times = df.loc[df.dt != 'x']
        df.dt = df.dt.replace(to_replace="x", method="ffill")
        dt = list(
            map(lambda x: datetime.combine(date, time_from_ff_excel(x)), df.dt.tolist())
        )
        df.dt = dt
        df.rename(
            columns={
                "dt": "datetime",
                "Counter Name": "library",
                "People In": "people_in",
                "People Out": "people_out",
            },
            inplace=True,
        )
        res.append(df)

    return pd.concat(res)


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
        day = row.datetime.day_name()[:3]
        mask = (
            (oh.library == row.library)
            & (oh.day == day)
            & (oh.start <= row.datetime.hour)
            & (oh.finish > row.datetime.hour)
        )
        if oh.loc[mask].empty:
            res.append("closed")
        else:
            ot = oh.loc[mask].opening_type.values[0]
            res.append(ot)
    return pd.Series(res)


def get_df_with_opening_hours(fn):
    df = convert(fn)
    ots = get_ot(df.datetime, df.library)
    df["opening_type"] = ots.values
    return df


def add_xl_to_csv(xl_fn, csv_fn, to_csv=False):
    curr = pd.read_csv(csv_fn)
    df_to_add = get_df_with_opening_hours(xl_fn)
    res = pd.concat([curr, df_to_add], sort=True).drop_duplicates()
    df = res.astype({"datetime": "datetime64"})
    if to_csv is True:
        df.to_csv(csv_fn, index=False)
    return df
