import logging
import math
import pandas as pd
import typing as tp

from pmdarima import auto_arima

from src.services.stats import StatsService
from src.settings import settings


logger = logging.getLogger(__name__)


class PredictService:
    _stats_service: StatsService

    requests: tp.List[int] = []

    def __init__(self, stats_service: StatsService) -> None:
        self._stats_service: StatsService = stats_service

    def predict(self):
        self._predict_request()

    def _predict_request(self) -> None:
        if len(self._stats_service.memory) < settings.min_memory_size:
            return None

        try:
            dates = [
                item.timestamp
                for item in list(self._stats_service.memory.values())[
                    -settings.train_size:
                ]
            ]
            requests = [
                item.requests
                for item in list(self._stats_service.memory.values())[
                    -settings.train_size:
                ]
            ]

            df = pd.DataFrame(list(zip(dates, requests)), columns=["date", "requests"])
            df.set_index("date", inplace=True)

            train = df["requests"]

            model = auto_arima(
                train, trace=False, error_action="ignore", suppress_warnings=True
            )
            model.fit(train)

            result = model.predict(n_periods=6)

            self.requests = [math.ceil(i) for i in list(result)]
        except Exception as e:
            logger.error(f"Failed predict: {e}")
            self.requests = []

    @property
    def is_request_predicted(self):
        return len(self.requests) > 0
