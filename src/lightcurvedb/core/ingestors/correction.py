from lightcurvedb.core.ingestors import contexts


class LightcurveCorrector:
    def __init__(self, sqlite_path):
        self.sqlite_path = sqlite_path
        self.quality_flag_map = contexts.get_quality_flag_mapping(sqlite_path)

    def get_quality_flags(self, camera, ccd, cadences):
        qflag_series = self.quality_flag_map.loc[(camera, ccd)].loc[cadences][
            "quality_flag"
        ]
        return qflag_series.to_numpy()
