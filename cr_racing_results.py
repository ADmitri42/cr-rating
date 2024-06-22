from itertools import chain, repeat, islice
from typing import Self
from dataclasses import dataclass
import requests
import pandas as pd


COLUMNS = ['bib', 'name', 'gender', 'category', 'status', 'rank_abs', 'result', 'result_time', 'team', 'club']
POINTS = [
    100, 98, 96, 94, 92, 90, 88, 86, 84, 82,
    80,  78, 76, 74, 72, 70, 68, 66, 64, 62,
    60,  58, 56, 54, 52, 50, 48, 46, 44, 42,
    40,  38, 36, 34, 32, 30, 28, 26, 24, 22,
    20,  20, 20, 20, 20, 20, 20, 20, 20, 20
]


@dataclass
class RaceResults:
    " Description of the race and its results "
    name: str
    tt_name: str
    order: int
    group: dict[str, pd.DataFrame]
    tt: pd.DataFrame | None
    clusters: dict[str, tuple]
    tt_first: bool = True

    def get_race_points(self) -> dict[str, pd.DataFrame]:
        cluster_points = {
            cluster: get_points_for_race(results, self.name) for cluster, results in self.group.items()
        }
        for cluster in cluster_points:
            cluster_results = self.tt[self.tt['cluster'] == cluster]
            race_points = get_points_for_race(cluster_results, self.tt_name)
            left_table = race_points if self.tt_first else cluster_points[cluster]
            right_table = cluster_points[cluster] if self.tt_first else race_points
            cluster_points[cluster] = pd.merge(left_table, right_table, how='outer', on='name')

        return cluster_points

    @classmethod
    def from_config(cls, race_config: dict, cluster_distribution: dict[str, tuple[str]] | None = None) -> Self:
        " Create race results from config "
        name = race_config.get('group_name') or race_config.get('name')
        tt_name = race_config.get('tt-name') or f"{race_config['name']} ITT"
        group_results = cls.get_group_results(race_config['group'])
        updated_cluster = cls.update_clusters(cluster_distribution, group_results)
        if 'tt' in race_config:
            tt_results = cls.get_tt_results(race_config['tt'], updated_cluster)
        else:
            tt_results = None
        return cls(
            name=name, tt_name=tt_name,
            order=race_config['order'],
            tt_first=race_config.get('tt-first', True),
            clusters=updated_cluster,
            group=group_results,
            tt=tt_results
        )

    @staticmethod
    def get_group_results(group_results_links: dict[str, str]) -> dict[str, pd.DataFrame]:
        " Get races "
        results = {}
        for cluster, result_link in group_results_links.items():
            results[cluster] = _get_results(result_link + '.json')
        return results

    @classmethod
    def get_tt_results(cls, results_link: dict, clusters: dict[str, tuple[str]]) -> Self:
        " Parse race config and load data for TT race "
        results = _get_results(results_link + '.json')
        _set_tt_cluster(results, clusters)
        return results

    @staticmethod
    def update_clusters(current_clusters: dict[str, tuple] | None, group_results: dict[str, pd.DataFrame]) -> dict[str, tuple]:
        " Update clusters with the list from the race "
        if not group_results:
            group_results = {cluster: () for cluster in group_results}
        cluster_from_race = set(
            chain(*(map(_shorten_name, results.name.unique()) for results in group_results.values()))
        )
        new_clusters = {}
        for cluster, current_list in current_clusters.items():
            cluster_racers = set(
                map(_shorten_name, group_results[cluster].name.unique())
            )
            new_clusters[cluster] = tuple((set(current_list) - cluster_from_race).union(cluster_racers))
        return new_clusters


def _shorten_name(name: str) -> str:
    splited_name = name.split(" ")
    return " ".join(chain(splited_name[:-1], splited_name[-1][0]))


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


def _pad_infinite(iterable, padding=None):
    return chain(iterable, repeat(padding))


def _pad(iterable, size, padding=None):
    return islice(_pad_infinite(iterable, padding), size)


def _generate_points(n_places: int):
    return list(_pad(POINTS, n_places, 0))


def get_points_for_race(race_results: pd.DataFrame, race_name: str | None = None) -> pd.DataFrame:
    " Create dataframe with points of the racers"
    points_column = 'points'
    if race_name:
        points_column = race_name
    sorted_results = race_results \
        .dropna(subset=['rank_abs']) \
        .astype({'rank_abs': int}) \
        .sort_values('rank_abs') \
        .set_index('rank_abs')
    sorted_results[points_column] = _generate_points(sorted_results.shape[0])
    return sorted_results.loc[:, ['name', points_column]]
