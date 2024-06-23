from typing import Self
from itertools import chain, repeat, islice
from pathlib import Path
import re
import json
from dataclasses import dataclass
import requests
import pandas as pd
from bs4 import BeautifulSoup


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
    clusters: pd.DataFrame
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
            cluster_points[cluster] = pd.merge(left_table, right_table, how='outer', on=['name', 'year_of_birth'])

        return cluster_points

    def save(self, race_dir: Path | str):
        " Save race info into "
        if isinstance(race_dir, str):
            race_dir = Path(race_dir)
        race_dir.mkdir(parents=True, exist_ok=True)
        metainfo = self.__dict__.copy()
        metainfo['group'] = {}
        metainfo['clusters'].to_csv(race_dir.joinpath('clusters.csv'))
        metainfo['clusters'] = 'clusters.csv'
        for cluster, results in self.group.items():
            results.to_csv(race_dir.joinpath(f'group_cluster_{cluster}.csv'))
            metainfo['group'][cluster] = f'group_cluster_{cluster}.csv'

        if self.tt is not None:
            self.tt.to_csv(race_dir.joinpath('time_trial.csv'))
            metainfo['tt'] = 'time_trial.csv'

        with open(race_dir.joinpath('meta.json'), 'w', encoding='utf8') as fp:
            json.dump(metainfo, fp, indent=2)

    @classmethod
    def load(cls, race_dir: Path | str) -> Self:
        if isinstance(race_dir, str):
            race_dir = Path(race_dir)
        if not race_dir.exists():
            raise FileNotFoundError(f'Path {str(race_dir)} does not exists.')
        if not race_dir.is_dir():
            raise NotADirectoryError(f'{str(race_dir)} not a directory')

        with open(race_dir.joinpath('meta.json'), encoding='utf8') as fp:
            race_info = json.load(fp)
        race_info['clusters'] = pd.read_csv(race_dir.joinpath(race_info['clusters']))
        for cluster, file in race_info['group'].items():
            race_info['group'][cluster] = pd.read_csv(race_dir.joinpath(file))
        if race_info['tt']:
            race_info['tt'] = pd.read_csv(race_dir.joinpath(race_info['tt']))
        return cls(**race_info)

    @classmethod
    def from_config(cls, race_config: dict, previous_clusters: pd.DataFrame | None, official_clusters: dict[str, tuple]) -> Self:
        " Create race results from config "
        name = race_config.get('group_name') or race_config.get('name')
        tt_name = race_config.get('tt-name') or f"{race_config['name']} ITT"
        group_results = cls.get_group_results(race_config['group'])
        if 'tt' in race_config:
            tt_results = cls.get_tt_results(race_config['tt'])
            updated_cluster = cls.update_clusters(group_results, tt_results, previous_clusters, official_clusters)
            tt_results = pd.merge(tt_results, updated_cluster, how='left', on=('name', 'year_of_birth'))
        else:
            tt_results = None
            updated_cluster = cls.update_clusters(group_results, tt_results, previous_clusters, official_clusters)
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
        " Get group race results "
        results = {}
        for cluster, results_link in group_results_links.items():
            results[cluster] = _get_results(results_link + '.json')
            yob = RaceResults._get_year_of_birth(results[cluster], results_link)
            results[cluster] = pd.merge(results[cluster], yob, on='bib', how='left')
        return results

    @classmethod
    def get_tt_results(cls, results_link: dict) -> Self:
        " Parse race config and load data for TT race "
        results = _get_results(results_link + '.json')
        yob = RaceResults._get_year_of_birth(results, results_link)
        results = pd.merge(results, yob, on='bib', how='left')
        return results

    @staticmethod
    def _get_year_of_birth(race_results: pd.DataFrame, race_link: str, timeout: int = 1000) -> pd.DataFrame:
        " Get year of birth of the racers "
        yob = []
        for bib in race_results.bib:
            r = requests.get(race_link + f'/{bib}', timeout=timeout)
            page_info = BeautifulSoup(r.text, features="html.parser")
            participatn_info = page_info.find_all('section', {'class': 'card-body'})
            founded_year = list(
                chain(*(card.find_all('dd', string=re.compile(r'\d{4}')) for card in participatn_info)),
            )
            yob.append((bib, int(founded_year[-1].text) if founded_year else -1))
        return pd.DataFrame(yob, columns=['bib', 'year_of_birth'])

    @staticmethod
    def update_clusters(
            group_results: dict[str, pd.DataFrame],
            tt_results: pd.DataFrame | None,
            previous_clusters: pd.DataFrame | None,
            official_clusters: dict[str, tuple]
            ) -> pd.DataFrame:
        " Update clusters with the list from the race "
        clusters = []
        for cluster in group_results:
            df = group_results[cluster].loc[:, ['name', 'year_of_birth']]
            df['cluster'] = cluster
            clusters.append(df)
        clusters = pd.concat(clusters)
        if previous_clusters is not None:
            clusters = pd.concat([clusters, previous_clusters], axis='index') \
                .drop_duplicates(('name', 'year_of_birth'), keep='first')

        if tt_results is None:
            return clusters

        extended_clusters = pd.merge(
            tt_results.loc[tt_results.status == 'Q', ['name', 'year_of_birth', 'gender', 'category']],
            clusters,
            how='outer',
            on=['name', 'year_of_birth']
        )

        extended_clusters.loc[extended_clusters.gender == 'female', 'cluster'] = 'F'
        extended_clusters.loc[extended_clusters.category == 'M Элита', 'cluster'] = 'A'
        for cluster, clust_list in official_clusters.items():
            extended_clusters.loc[
                pd.isna(extended_clusters.cluster) & (extended_clusters.name.str.startswith(clust_list)),
                'cluster'
            ] = cluster

        extended_clusters.loc[pd.isna(extended_clusters.cluster), 'cluster'] = 'C'

        return extended_clusters.loc[:, ['name', 'year_of_birth', 'cluster']]


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
    return sorted_results.loc[:, ['name', 'year_of_birth', points_column]]
