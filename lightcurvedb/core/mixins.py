import typing

import pandas as pd
import sqlalchemy as sa

from lightcurvedb import models as m


class APIMixin:
    pass


class FrameAPIMixin(APIMixin):
    def get_mid_tjd_mapping(self, frame_type: typing.Union[str, None] = None):
        if frame_type is None:
            frame_type = "Raw FFI"
        cameras = sa.select(m.Frame.camera.distinct())

        tjd_q = (
            sa.select(m.Frame.cadence, m.Frame.mid_tjd)
            .join(m.Frame.frame_type)
            .where(m.FrameType.name == frame_type)
            .order_by(m.Frame.cadence)
        )

        mapping = {}
        for (camera,) in self.execute(cameras):
            q = tjd_q.where(m.Frame.camera == camera)
            df = pd.DataFrame(
                self.execute(q),
                columns=["cadence", "mid_tjd"],
                index=["cadence"],
            )
            mapping[camera] = df

        return mapping
