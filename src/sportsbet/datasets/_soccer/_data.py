"""Download and transform historical and fixtures data."""

# Author: Georgios Douzas <gdouzas@icloud.com>
# License: MIT

from __future__ import annotations

import warnings
from functools import lru_cache
from json import loads
from typing import ClassVar

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from sklearn.model_selection import ParameterGrid
from typing_extensions import Self

from ... import FixturesData, ParamGrid, Schema, TrainData
from .._base import BaseDataLoader
from ._utils import OUTPUTS, _read_csv, _read_csvs, _read_urls_content, NetworkError, DataValidationError

# Primary data sources
MODELLING_URL = 'https://github.com/georgedouzas/sports-betting/tree/data/data/soccer/modelling'
TRAINING_URL = 'https://raw.githubusercontent.com/georgedouzas/sports-betting/data/data/soccer/modelling/{league}_{division}_{year}.csv'
FIXTURES_URL = 'https://raw.githubusercontent.com/georgedouzas/sports-betting/data/data/soccer/modelling/fixtures.csv'

# Backup data sources (for redundancy)
BACKUP_SOURCES = {
    'modelling': [
        'https://github.com/georgedouzas/sports-betting/tree/data/data/soccer/modelling',
        # Add additional backup sources here as they become available
    ],
    'training': [
        'https://raw.githubusercontent.com/georgedouzas/sports-betting/data/data/soccer/modelling/{league}_{division}_{year}.csv',
        # Potential backup: CDN or mirror sources
        # 'https://cdn.sportsbet-data.com/soccer/modelling/{league}_{division}_{year}.csv',
    ],
    'fixtures': [
        'https://raw.githubusercontent.com/georgedouzas/sports-betting/data/data/soccer/modelling/fixtures.csv',
        # Potential backup sources
        # 'https://backup.sportsbet-data.com/soccer/fixtures.csv',
    ]
}

# Cache configuration
import logging
logger = logging.getLogger(__name__)

class SoccerDataLoader(BaseDataLoader):
    """Dataloader for soccer data.

    It downloads historical and fixtures data for various
    leagues, years and divisions.

    Read more in the [user guide][user-guide].

    Args:
        param_grid:
            It selects the type of information that the data include. The keys of
            dictionaries might be parameters like `'league'` or `'division'` while
            the values are sequences of allowed values. It works in a similar way as the
            `param_grid` parameter of the scikit-learn's ParameterGrid class.
            The default value `None` corresponds to all parameters.

    Attributes:
        param_grid_ (ParameterGrid):
            The checked value of parameters grid. It includes all possible parameters if
            `param_grid` is `None`.

        dropped_na_cols_ (pd.Index):
            The columns with missing values that are dropped.

        drop_na_thres_(float):
            The checked value of `drop_na_thres`.

        odds_type_ (str | None):
            The checked value of `odds_type`.

        input_cols_ (pd.Index):
            The columns of `X_train` and `X_fix`.

        output_cols_ (pd.Index):
            The columns of `Y_train` and `Y_fix`.

        odds_cols_ (pd.Index):
            The columns of `O_train` and `O_fix`.

        target_cols_ (pd.Index):
            The columns used for the extraction of output and odds columns.

        train_data_ (TrainData):
            The tuple (X, Y, O) that represents the training data as extracted from
            the method `extract_train_data`.

        fixtures_data_ (FixturesData):
            The tuple (X, Y, O) that represents the fixtures data as extracted from
            the method `extract_fixtures_data`.

    Examples:
        >>> from sportsbet.datasets import SoccerDataLoader
        >>> import pandas as pd
        >>> # Get all available parameters to select the training data
        >>> SoccerDataLoader.get_all_params()
        [{'division': 1, 'league': 'Argentina', ...
        >>> # Select only the traning data for the French and Spanish leagues of 2020 year
        >>> dataloader = SoccerDataLoader(
        ... param_grid={'league': ['England', 'Spain'], 'year':[2020]})
        >>> # Get available odds types
        >>> dataloader.get_odds_types()
        ['market_average', 'market_maximum']
        >>> # Select the market average odds and drop colums with missing values
        >>> X_train, Y_train, O_train = dataloader.extract_train_data(
        ... odds_type='market_average')
        >>> # Odds data include the selected market average odds
        >>> O_train.columns
        Index(['odds__market_average__home_win__full_time_goals',...
        >>> # Extract the corresponding fixtures data
        >>> X_fix, Y_fix, O_fix = dataloader.extract_fixtures_data()
        >>> # Training and fixtures input and odds data have the same column names
        >>> pd.testing.assert_index_equal(X_train.columns, X_fix.columns)
        >>> pd.testing.assert_index_equal(O_train.columns, O_fix.columns)
        >>> # Fixtures data have always no output
        >>> Y_fix is None
        True
    """

    SCHEMA: ClassVar[Schema] = [
        ('date', np.datetime64),
        ('league', object),
        ('division', np.int64),
        ('year', np.int64),
        ('home_team', object),
        ('away_team', object),
        ('odds__market_maximum__home_win__full_time_goals', float),
        ('odds__market_maximum__draw__full_time_goals', float),
        ('odds__market_maximum__away_win__full_time_goals', float),
        ('odds__market_maximum__over_2.5__full_time_goals', float),
        ('odds__market_maximum__under_2.5__full_time_goals', float),
        ('odds__market_average__home_win__full_time_goals', float),
        ('odds__market_average__draw__full_time_goals', float),
        ('odds__market_average__away_win__full_time_goals', float),
        ('odds__market_average__over_2.5__full_time_goals', float),
        ('odds__market_average__under_2.5__full_time_goals', float),
        ('home__points__avg', float),
        ('home__adj_points__avg', float),
        ('home__goals_for__avg', float),
        ('home__goals_against__avg', float),
        ('home__adj_goals_for__avg', float),
        ('home__adj_goals_against__avg', float),
        ('home__points__latest_avg', float),
        ('home__adj_points__latest_avg', float),
        ('home__goals_for__latest_avg', float),
        ('home__goals_against__latest_avg', float),
        ('home__adj_goals_for__latest_avg', float),
        ('home__adj_goals_against__latest_avg', float),
        ('away__points__avg', float),
        ('away__adj_points__avg', float),
        ('away__goals_for__avg', float),
        ('away__goals_against__avg', float),
        ('away__adj_goals_for__avg', float),
        ('away__adj_goals_against__avg', float),
        ('away__points__latest_avg', float),
        ('away__adj_points__latest_avg', float),
        ('away__goals_for__latest_avg', float),
        ('away__goals_against__latest_avg', float),
        ('away__adj_goals_for__latest_avg', float),
        ('away__adj_goals_against__latest_avg', float),
        ('target__home_team__full_time_goals', float),
        ('target__away_team__full_time_goals', float),
        ('target__home_team__shots', float),
        ('target__away_team__shots', float),
        ('target__home_team__shots_on_target', float),
        ('target__away_team__shots_on_target', float),
        ('target__home_team__corners', float),
        ('target__away_team__corners', float),
        ('target__home_team__fouls_committed', float),
        ('target__away_team__fouls_committed', float),
        ('target__home_team__yellow_cards', float),
        ('target__away_team__yellow_cards', float),
        ('target__home_team__red_cards', float),
        ('target__away_team__red_cards', float),
    ]
    OUTPUTS = OUTPUTS

    def __init__(self: Self, param_grid: ParamGrid | None = None) -> None:
        super().__init__(param_grid)

    @classmethod
    @lru_cache
    def _get_full_param_grid(cls: type[SoccerDataLoader]) -> ParameterGrid:
        bsObj = BeautifulSoup(_read_urls_content([MODELLING_URL])[0], features='html.parser')
        element = bsObj.find('script', {'data-target': 'react-app.embeddedData'})
        param_grid = []
        for item in loads(element.text)['payload']['tree']['items']:
            if 'fixtures.csv' not in item['path']:
                league, division, year = item['name'].replace('.csv', '').split('_')
                param_grid.append(
                    {
                        'league': [league.title() if league.lower() != 'usa' else 'USA'],
                        'division': [int(division)],
                        'year': [int(year)],
                    },
                )
        return ParameterGrid(param_grid)

    def _try_backup_sources(self: Self, source_type: str, format_params: dict = None) -> pd.DataFrame:
        """Try backup sources if primary fails."""
        sources = BACKUP_SOURCES.get(source_type, [])
        
        for source_url in sources:
            try:
                if format_params:
                    url = source_url.format(**format_params)
                else:
                    url = source_url
                
                logger.info(f"Trying backup source: {url}")
                
                if source_type == 'fixtures':
                    return _read_csv(url)
                else:
                    # For training data, we need multiple URLs
                    urls = [url.format(**params) for params in self.param_grid_]
                    return pd.concat(_read_csvs(urls))
                    
            except Exception as e:
                logger.warning(f"Backup source {source_url} failed: {e}")
                continue
        
        raise NetworkError(f"All backup sources failed for {source_type}")

    @lru_cache  # noqa: B019
    def _get_data(self: Self) -> pd.DataFrame:
        training_data = None
        fixtures_data = None
        
        # Try to load training data with fallback
        try:
            urls = [TRAINING_URL.format(**params) for params in self.param_grid_]
            training_data = pd.concat(_read_csvs(urls))
            training_data['fixtures'] = False
            logger.info(f"Successfully loaded training data with {len(training_data)} records")
        except Exception as e:
            logger.error(f"Primary training data source failed: {e}")
            try:
                training_data = self._try_backup_sources('training')
                training_data['fixtures'] = False
                logger.info(f"Successfully loaded training data from backup with {len(training_data)} records")
            except Exception as backup_e:
                logger.error(f"All training data sources failed: {backup_e}")
                raise NetworkError("Failed to load training data from all sources") from backup_e
        
        # Try to load fixtures data with fallback
        try:
            fixtures_data = _read_csv(FIXTURES_URL)
            fixtures_data['fixtures'] = True
            logger.info(f"Successfully loaded fixtures data with {len(fixtures_data)} records")
        except Exception as e:
            logger.warning(f"Primary fixtures data source failed: {e}")
            try:
                fixtures_data = self._try_backup_sources('fixtures')
                fixtures_data['fixtures'] = True
                logger.info(f"Successfully loaded fixtures data from backup with {len(fixtures_data)} records")
            except Exception as backup_e:
                logger.warning(f"All fixtures data sources failed: {backup_e}")
                # Fixtures data is optional, so we can continue without it
                fixtures_data = pd.DataFrame()
        
        # Combine data
        if training_data is not None:
            if not fixtures_data.empty:
                data = pd.concat([training_data, fixtures_data]).reset_index(drop=True)
            else:
                data = training_data.reset_index(drop=True)
        else:
            raise DataValidationError("No training data available")
        
        # Enhanced date parsing with better error handling
        try:
            data['date'] = pd.to_datetime(data['date'], format='%d/%m/%Y')
            logger.info("Successfully parsed dates using primary format (%d/%m/%Y)")
        except ValueError as e:
            logger.warning(f"Primary date format failed: {e}, trying alternative parsing")
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', category=UserWarning)
                    data['date'] = pd.to_datetime(data['date'], infer_datetime_format=True)
                logger.info("Successfully parsed dates using inferred format")
            except Exception as parse_e:
                logger.error(f"Date parsing completely failed: {parse_e}")
                # Keep the original date column as string if parsing fails
                logger.warning("Keeping dates as strings due to parsing failure")
        
        # Basic data validation
        if data.empty:
            raise DataValidationError("Final dataset is empty")
        
        logger.info(f"Successfully loaded complete dataset with {len(data)} records")
        return data

    def extract_train_data(
        self: Self,
        drop_na_thres: float = 0.0,
        odds_type: str | None = None,
    ) -> TrainData:
        """Extract the training data.

        Read more in the [user guide][dataloader].

        It returns historical data that can be used to create a betting
        strategy based on heuristics or machine learning models.

        The data contain information about the matches that belong
        in two categories. The first category includes any information
        known before the start of the match, i.e. the training data `X`
        and the odds data `O`. The second category includes the outcomes of
        matches i.e. the multi-output targets `Y`.

        The method selects only the the data allowed by the `param_grid`
        parameter of the initialization method. Additionally, columns with missing
        values are dropped through the `drop_na_thres` parameter, while the
        types of odds returned is defined by the `odds_type` parameter.

        Args:
            drop_na_thres:
                The threshold that specifies the input columns to drop. It is a float in
                the `[0.0, 1.0]` range. Higher values result in dropping more values.
                The default value `drop_na_thres=0.0` keeps all columns while the
                maximum value `drop_na_thres=1.0` keeps only columns with non
                missing values.

            odds_type:
                The selected odds type. It should be one of the available odds columns
                prefixes returned by the method `get_odds_types`. If `odds_type=None`
                then no odds are returned.

        Returns:
            (X, Y, O):
                Each of the components represent the training input data `X`, the
                multi-output targets `Y` and the corresponding odds `O`, respectively.
        """
        return super().extract_train_data(drop_na_thres=drop_na_thres, odds_type=odds_type)

    def extract_fixtures_data(self: Self) -> FixturesData:
        """Extract the fixtures data.

        Read more in the [user guide][dataloader].

        It returns fixtures data that can be used to make predictions for
        upcoming matches based on a betting strategy.

        Before calling the `extract_fixtures_data` method for
        the first time, the `extract_training_data` should be called, in
        order to match the columns of the input, output and odds data.

        The data contain information about the matches known before the
        start of the match, i.e. the training data `X` and the odds
        data `O`. The multi-output targets `Y` is always equal to `None`
        and are only included for consistency with the method `extract_train_data`.

        The `param_grid` parameter of the initialization method has no effect
        on the fixtures data.

        Returns:
            (X, None, O):
                Each of the components represent the fixtures input data `X`, the
                multi-output targets `Y` equal to `None` and the
                corresponding odds `O`, respectively.
        """
        return super().extract_fixtures_data()
