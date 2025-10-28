# market_data/ImbalanceMarketPrices.py
import datetime as dt
import os
from typing import Optional

import pandas as pd
from entsoe import Area, EntsoePandasClient

from market_data.AbstractQueryMarketPrices import AbstractQueryMarketPrices
from market_data.entsoe_rest import get_imbalance_prices_a85
from model import PriceScheduleDataFrame

class ImbalanceMarketPrices(AbstractQueryMarketPrices):
    DEFAULT_FILE_NAME = "market_data/data/imbalance_data.pkl"

    @classmethod
    def cold_load_data(
        cls,
        start_time: dt.datetime,
        end_time: dt.datetime,
        client: Optional[EntsoePandasClient],
        store_in_hot_load: bool,
        entsoe_area: Area = Area["NL"],
    ) -> PriceScheduleDataFrame:

        # Zorg voor tz-aware pandas tijden in gebiedstijdzone
        start_pd, end_pd = cls.convert_to_timezoned_pandas_object(
            start_time, end_time, entsoe_area
        )

        # 1) Probeer REST A85 (stabieler bij DST)
        token = os.environ.get("ENTSOE_API_KEY")
        df = pd.DataFrame()
        if token:
            df = get_imbalance_prices_a85(
                token=token,
                control_area_domain=entsoe_area.code,  # NL: 10YNL----------L
                start=start_pd,
                end=end_pd,
                tz=str(start_pd.tz),
            )

        # 2) Fallback naar entsoe-py als REST niets oplevert
        if df is None or df.empty:
            if client is None:
                raise ConnectionError("No EntsoePandasClient provided.")
            entsoe_imbalance_prices = client.query_imbalance_prices(
                country_code=entsoe_area.code, start=start_pd, end=end_pd
            )
            # entsoe-py geeft NL doorgaans 'Short'/'Long' kolommen
            df = entsoe_imbalance_prices.rename(
                {"Short": "charge_price", "Long": "discharge_price"}, axis=1
            )

        PriceScheduleDataFrame.validate(df)  # check kolommen/index

        if store_in_hot_load:
            cls.update_hot_load(df)

        return df

    @classmethod
    def expected_length_of_data(cls, start_time: dt.datetime, end_time: dt.datetime):
        # 15-min resolutie
        return int((end_time - start_time).total_seconds() / 900)
