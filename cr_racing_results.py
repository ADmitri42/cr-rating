import os
from pathlib import Path
from collections import defaultdict
from itertools import chain, repeat, islice
from copy import deepcopy
import requests

import pandas as pd
import yaml


from typing import Self
from dataclasses import dataclass
from enum import Enum


COLUMNS = ['bib', 'name', 'gender', 'category', 'status', 'rank_abs', 'result', 'result_time', 'team', 'club']


class RaceType(Enum):
    Group = 'Group'
    TT = 'TT'


@dataclass
class RaceResults:
    " Description of the race and its results "
    name: str
    order: int
    race_type: RaceType
    results: dict[str, pd.DataFrame] | pd.DataFrame

    @classmethod
    def from_config(cls, race_config: dict, cluster_distribution: dict[str, tuple[str]] | None = None) -> Self:
        " Create race results from config "
        if isinstance(race_config['results'], dict):
            return cls.group_from_config(race_config)
        if not isinstance(race_config['results'], str):
            raise ValueError(f"Unsupported type {type(race_config['results'])}")
        if not cluster_distribution:
            cluster_distribution = {}
        return cls.tt_from_config(race_config, cluster_distribution)

    @classmethod
    def group_from_config(cls, race_config: dict) -> Self:
        " Parse race config and load data "
        results = {}
        for cluster, result_link in race_config['results'].items():
            results[cluster] = _get_results(result_link + '.json')
        return cls(race_config['name'], race_config['order'], RaceType.Group, results)

    @classmethod
    def tt_from_config(cls, race_config: dict, official_clusters: dict[str, tuple[str]]) -> Self:
        " Parse race config and load data for TT race "
        results = _get_results(race_config['results'] + '.json')
        _set_tt_cluster(results, official_clusters)
        return cls(race_config['name'], race_config['order'], RaceType.TT, results)


def _shorten_name(name: str) -> str:
    splited_name = name.split(" ")
    return " ".join(chain(splited_name[:-1], splited_name[-1][0]))


def update_clusters(current_clusters: dict[str, tuple], race_results: RaceResults) -> dict[str, tuple]:
    " Update clusters with the list from the race "
    cluster_from_race = set(
        chain(*(map(_shorten_name, results.name.unique()) for results in race_results.results.values()))
    )
    new_clusters = {}
    for cluster, current_list in current_clusters.items():
        new_clusters[cluster] = tuple((set(current_list) - cluster_from_race) | current_list)
    return new_clusters


def _get_results(results_url: str, full: bool = False, timeout: int = 1000) -> pd.DataFrame:
    r = requests.get(results_url, params={"page": 1}, timeout=timeout)
    r.raise_for_status()
    results = r.json()

    full_results = results['items']
    for page in range(2, results['page_info']['totalPages'] + 1):
        r = requests.get(results_url, params={'page': page}, timeout=timeout)
        r.raise_for_status()
        full_results.extend(r.json()['items'])

    if full:
        return full_results
    results_df = pd.DataFrame(full_results, columns=COLUMNS)
    return results_df


def _set_tt_cluster(tt_results: pd.DataFrame, clusters: dict[str, tuple[str]]) -> None:
    tt_results['cluster'] = 'C'
    tt_results.loc[tt_results.gender == 'female', 'cluster'] = 'F'
    for cluster, racers in clusters.items():
        tt_results.loc[tt_results.name.str.startswith(racers), 'cluster'] = cluster
