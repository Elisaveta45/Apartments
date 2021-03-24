from datetime import datetime
import glob
from typing import Optional, Set, cast
from sqlalchemy import create_engine
import pandas as pd
from argparse import ArgumentParser, Namespace
from pathlib import Path
from pandas.core.tools.datetimes import to_datetime

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
KNOWN_APARTMENT_FIELDS: Set[str] = set(
    [
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
    ]
)
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
    df = df.drop_duplicates(subset=["link", "created_at"])
    df = df.drop_duplicates(subset=["link", "last_updated"])

    return df


def readall(
    from_date: Optional[pd.Timestamp] = None,
    to_date: Optional[pd.Timestamp] = None,
):
    # TODO: Change path
    crawl_runs_folder = Path("<path_to_crawled_runs>")
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
            pd.to_datetime(run.stem, format=DATE_FORMAT),
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

    print(df)
