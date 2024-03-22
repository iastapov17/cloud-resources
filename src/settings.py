from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    host: str = "https://mts-olimp-cloud.codenrock.com/"
    token: str = "TOKEN"

    max_load: int = 95
    pod_load_max: int = 90
    delta: float = 0.2
    gap: int = 4
    penalty: float = 0.001

    sleep_second: int = 15
    memory_size: int = 100

    train_size: int = 120
    max_data_size: int = 500

    min_memory_size: int = 11
    prod: bool = True

    @property
    def pod_load_max_percent(self):
        return self.pod_load_max / 100


settings = Settings()
