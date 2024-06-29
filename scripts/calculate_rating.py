import sys
from typing import Any
from collections import defaultdict
from pathlib import Path
import logging
import argparse
import pandas as pd
import yaml

from cr_racing_results import (
    RaceResults,
)


RENAMED_COLUMNS = {
    "name": "Гонщик",
    "year_of_birth": "Год рождения",
    "total": "Очки"
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


def load_official_clusters(clusters_path: Path | str) -> dict[str, tuple[str]]:
    " Load official clusters from file "
    logger.info(f'Loading official clusters from {str(clusters_path)}')
    with open(clusters_path, encoding='utf-8') as clusters_fp:
        clusters = yaml.safe_load(clusters_fp)
    official_clusters = defaultdict(tuple)
    for cluster, official_list in clusters.items():
        fixed_cluter = cluster.rstrip('+')
        official_clusters[fixed_cluter] = official_clusters[fixed_cluter] + tuple(official_list)
    return official_clusters


def load_races(race_configs: list[dict[str, Any]], store_localy: Path | str | None = None) -> list[RaceResults]:
    " Load race results "
    logger.info('Loading races')
    if isinstance(store_localy, str):
        store_localy = Path(store_localy)
    if store_localy:
        store_localy.mkdir(parents=True, exist_ok=True)
        if not store_localy.is_dir():
            raise NotADirectoryError(f'{str(store_localy)} must be a directory')

    races = []
    cluster_distribution = None
    official_clusters = {}
    for race in race_configs:
        logger.info(f'Processing race {race['name']}')
        if 'clusters' in race:
            official_clusters = load_official_clusters(race['clusters'])
        race = RaceResults.from_config(race, cluster_distribution, official_clusters)
        if store_localy:
            race.save(store_localy.joinpath(race.name))
        cluster_distribution = race.clusters
        races.append(race)
    return races


def calculate_cluster_standing(race_results: list[RaceResults]) -> dict[str, pd.DataFrame]:
    " Calculate standing from individual races "
    logger.info('Calculating the rating by cluster')
    cluster_standing = {}
    for race in race_results:
        for cluster, race_points in race.get_race_points().items():
            if cluster not in cluster_standing:
                cluster_standing[cluster] = race_points
                continue
            cluster_standing[cluster] = pd.merge(
                cluster_standing[cluster],
                race_points,
                how='outer',
                on=('name', 'year_of_birth')
            )

    for cluster, cluster_results in cluster_standing.items():
        all_results = cluster_results.set_index(['name', 'year_of_birth'])
        all_results['total'] = all_results.sum(axis='columns')
        cluster_standing[cluster] = all_results.sort_values('total', ascending=False)

    return cluster_standing


def save_rating(current_standing: dict[str, pd.DataFrame], rating_dir: Path):
    " Save results to rating_dir "
    logger.info(f'Saving the results to {str(rating_dir)}')
    with pd.ExcelWriter(rating_dir.joinpath('current_standing.xlsx'), engine='xlsxwriter') as writer:
        for cluster, cluster_results in current_standing.items():
            data = cluster_results.reset_index().rename(RENAMED_COLUMNS, axis='columns')
            data.to_excel(writer, sheet_name=cluster)
            data.index += 1
            data.to_csv(rating_dir.joinpath(f'cluster_{cluster}.csv'))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('race_config', help='YAML file with info about races')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    with open(args.race_config, encoding='utf-8') as fp:
        race_results_config = yaml.safe_load(fp)

    rating_dir = Path(race_results_config['rating-dir'])
    rating_dir.mkdir(parents=True, exist_ok=True)
    if not rating_dir.is_dir():
        raise NotADirectoryError(f'{str(rating_dir)} is not a directory')

    races = load_races(race_results_config['races'], race_results_config.get('local_storage'))
    cluster_standing = calculate_cluster_standing(races)
    save_rating(cluster_standing, rating_dir)
