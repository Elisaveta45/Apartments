from datetime import datetime
from typing import Optional, Set, cast
import pandas as pd
from argparse import ArgumentParser, Namespace
from pathlib import Path

DATE_FORMAT = r"%Y%m%dT%H%M%S"
FD = None
TD = None
MONTH_NAMES = [
    "януари",
    "февруари",
    "март",
    "април",
    "май",
    "юни",
    "юли",
    "август",
    "септември",
    "октомври",
    "ноември",
    "декември",
]
MONTH_NAME_TO_IDX = {
    month_name: (idx + 1) for idx, month_name in enumerate(MONTH_NAMES)
}
MONTH_NAME_TO_STR_IDX = {
    month_name: str(idx + 1) for idx, month_name in enumerate(MONTH_NAMES)
}

KNOWN_APARTMENT_FIELDS: Set[str] = {
    "Вода:",
    "Газ:",
    "Двор:",
    "Етаж:",
    "Категория:",
    "Квадратура:",
    "Начин на ползване:",
    "Площ:",
    "Регулация:",
    "Строителство:",
    "ТEЦ:",
    "Ток:",
}

APARTMENT_FIELD_TO_DICT_KEY = {
    "Вода:": "water",
    "Газ:": "gas",
    "Двор:": "garden",
    "Етаж:": "floor",
    "Категория:": "category",
    "Квадратура:": "sqm",
    "Начин на ползване:": "usage",
    "Площ:": "space",
    "Регулация:": "regulations",
    "Строителство:": "built",
    "ТEЦ:": "tec",
    "Ток:": "electricity",
}


def try_read_json(json_path: Path, for_date: pd.Timestamp) -> pd.DataFrame:
    try:
        df: pd.DataFrame = cast(pd.DataFrame, pd.read_json(json_path))
        df["crawled_at"] = for_date
        return df
    except Exception as e:
        print(json_path)
        print(repr(e))
        return pd.DataFrame()


def drop_duplicates(df):
    # TODO: Drop duplicates after merging insert update time
    df.drop_duplicates(subset=["link", "entry_time"], inplace=True)

    return df


def merge_insert_update_time(df):
    df["entry_time"] = df["last_updated"].combine_first(df["created_at"])
    df.drop(["created_at", "last_updated"], axis=1, inplace=True)
    cond = df.entry_time.str.contains("Коригирана", na=False)

    # TODO: Change to True/False
    df.loc[cond, "is_creation"] = True
    df.loc[~cond, "is_creation"] = False

    return df


def floor_to_sqm(df):
    cond = df.floor.str.contains(r"\d+ кв.м", na=False)

    df.loc[cond, "sqm"] = df.loc[cond, "floor"]
    df.loc[cond, "floor"] = None

    return df


def floor_to_built(df):
    cond = df.floor.str.contains(r".* \d+ г.", na=False)

    df.loc[cond, "built"] = df.loc[cond, "floor"]
    df.loc[cond, "floor"] = None

    return df


def swap_space_sqm(df):
    # TODO: Can change `isnull` to `notna`
    cond = (df["space"].notna()) & (df["sqm"].isnull())
    df.loc[cond, ["space", "sqm"]] = df.loc[cond, ["sqm", "space"]].values
    return df


def extract_floor(df):
    new = df.floor.str.extract(
        r"(?P<new_floor>\d+|Партер)((?P<help>\D+)(?P<max_floor>\d+))?"
    )
    # TODO: No need to replace in the whole data frame. "Партер" is seen in 
    # only one of the columns
    new_replace = new.replace("Партер", 0)
    df[["floor", "max_floor"]] = new_replace[["new_floor", "max_floor"]]

    return df


def extract_area(df):
    new = df.sqm.str.split(" ", n=1, expand=True)
    df["apartment_area"], df["area_type"] = new[0], new[1]
    df["apartment_area"].astype(float)
    df.drop(["sqm"], axis=1, inplace=True)

    return df


def extract_price(df):
    new = df.price.str.rsplit(" ", n=1, expand=True)
    new[0] = new[0].str.replace(" ", "")
    df["price"], df["price_currency"] = new[0], new[1]
    df["price"].astype(float)

    return df


def readall(
    from_date: Optional[pd.Timestamp] = None,
    to_date: Optional[pd.Timestamp] = None,
):
    # TODO: Change path
    path_str = "/".join(
        [
            "C:",
            "Users",
            "ElisavetaPopova",
            "Desktop",
            "workspace",
            "data_engineer_internship_apartments_training",
            "crawl_runs",
        ]
    )
    crawl_runs_folder = Path(path_str)
    crawl_runs_iter = crawl_runs_folder.iterdir()

    filtered_runs_iter = crawl_runs_iter
    if from_date is not None and isinstance(from_date, datetime):
        filtered_runs_iter = (
            run
            for run in filtered_runs_iter
            if pd.to_datetime(run.stem, format=DATE_FORMAT) >= from_date
        )

    if to_date is not None and isinstance(to_date, datetime):
        filtered_runs_iter = (
            run
            for run in filtered_runs_iter
            if pd.to_datetime(run.stem, format=DATE_FORMAT) <= to_date
        )

    dfs = (
        try_read_json(
            run / "apartments.json",
            cast(pd.Timestamp, pd.to_datetime(run.stem, format=DATE_FORMAT)),
        )
        for run in filtered_runs_iter
    )
    df = pd.concat(dfs).reset_index(drop=True)
    df.index.name = "id"

    return df


def input_param_parser() -> Namespace:
    arg_parser = ArgumentParser()

    arg_parser.add_argument(
        "-fd", required=True, help=f"From Date: {DATE_FORMAT}."
    )
    arg_parser.add_argument(
        "-td", required=True, help=f"To Date: {DATE_FORMAT}."
    )

    args = arg_parser.parse_args()
    return args


if __name__ == "__main__":
    args = input_param_parser()

    FD = pd.to_datetime(args.fd, format=DATE_FORMAT)
    TD = pd.to_datetime(args.td, format=DATE_FORMAT)

    df = readall(FD, TD)

    # TODO: For some reason `area_type` has "кв.м" and "кв.м ".
    # Strip the column values before doing additional transformations.
    
    # TODO: Make columns into correct types. Ex apartment_area should be float.
    # Currently if you do `df.dtypes` you see that everything aside from
    # the `crawled_at` column is an object.
    df = (
        df.pipe(merge_insert_update_time)
        .pipe(drop_duplicates)
        .pipe(floor_to_sqm)
        .pipe(floor_to_built)
        .pipe(swap_space_sqm)
        .pipe(extract_floor)
        .pipe(extract_area)
        .pipe(extract_price)
    )

    print(df)
