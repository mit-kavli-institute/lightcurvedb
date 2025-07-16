"""Test the many-to-many relationship between Observation and FITSFrame."""

import numpy as np
import pytest
from sqlalchemy import orm

from lightcurvedb.models import FITSFrame, Instrument, Observation


class TestObservationFITSFrameManyToMany:
    """Test the many-to-many relationship between Observation and FITSFrame."""

    def test_many_to_many_relationship(self, v2_db: orm.Session):
        """Test that multiple observations can share FITS frames."""
        # Create multiple observations
        obs1 = Observation(
            type="test_observation",
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        obs2 = Observation(
            type="test_observation",
            cadence_reference=np.array([4, 5, 6], dtype=np.int64),
        )
        v2_db.add_all([obs1, obs2])
        v2_db.flush()

        # Create multiple FITS frames
        frame1 = FITSFrame(
            type="basefits",
            cadence=100,
            simple=True,
            bitpix=16,
            naxis=2,
            naxis_values=[2048, 2048],
            extend=True,
        )
        frame2 = FITSFrame(
            type="basefits",
            cadence=200,
            simple=True,
            bitpix=16,
            naxis=2,
            naxis_values=[2048, 2048],
            extend=True,
        )
        v2_db.add_all([frame1, frame2])
        v2_db.flush()

        # Associate frames with observations (many-to-many)
        # Both observations can have both frames
        obs1.fits_frames.append(frame1)
        obs1.fits_frames.append(frame2)
        obs2.fits_frames.append(frame1)  # frame1 is shared
        obs2.fits_frames.append(frame2)  # frame2 is shared
        v2_db.flush()

        # Test forward relationships (observation -> frames)
        assert len(obs1.fits_frames) == 2
        assert frame1 in obs1.fits_frames
        assert frame2 in obs1.fits_frames

        assert len(obs2.fits_frames) == 2
        assert frame1 in obs2.fits_frames
        assert frame2 in obs2.fits_frames

        # Test reverse relationships (frame -> observations)
        assert len(frame1.observations) == 2
        assert obs1 in frame1.observations
        assert obs2 in frame1.observations

        assert len(frame2.observations) == 2
        assert obs1 in frame2.observations
        assert obs2 in frame2.observations

    def test_polymorphic_many_to_many(self, v2_db: orm.Session):
        """Test polymorphic subtypes work with many-to-many."""
        # Create a base observation
        base_obs = Observation(
            type="base_observation",
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add(base_obs)
        v2_db.flush()

        # Create a base FITS frame
        base_frame = FITSFrame(
            type="basefits",
            cadence=300,
            simple=True,
            bitpix=16,
            naxis=2,
            naxis_values=[2048, 2048],
            extend=True,
        )
        v2_db.add(base_frame)
        v2_db.flush()

        # Associate them
        base_obs.fits_frames.append(base_frame)
        v2_db.flush()

        # Verify the association works with polymorphic types
        assert len(base_obs.fits_frames) == 1
        assert base_frame in base_obs.fits_frames
        assert len(base_frame.observations) == 1
        assert base_obs in base_frame.observations

        # The polymorphic identity should be preserved
        assert base_obs.type == "base_observation"
        assert base_frame.type == "basefits"

    def test_cascade_behavior(self, v2_db: orm.Session):
        """Test cascade behavior in the many-to-many relationship."""
        # Create observation and frame
        obs = Observation(
            type="test_observation",
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        frame = FITSFrame(
            type="basefits",
            cadence=400,
            simple=True,
            bitpix=16,
            naxis=2,
            naxis_values=[2048, 2048],
            extend=True,
        )
        v2_db.add_all([obs, frame])
        v2_db.flush()

        # Associate them
        obs.fits_frames.append(frame)
        v2_db.flush()

        # Get IDs for later verification
        obs_id = obs.id
        frame_id = frame.id

        # Delete the observation
        v2_db.delete(obs)
        v2_db.flush()

        # The frame should still exist but have no observations
        remaining_frame = v2_db.get(FITSFrame, frame_id)
        assert remaining_frame is not None
        assert len(remaining_frame.observations) == 0

        # Create a new observation and associate it with the existing frame
        new_obs = Observation(
            type="new_observation",
            cadence_reference=np.array([7, 8, 9], dtype=np.int64),
        )
        v2_db.add(new_obs)
        v2_db.flush()

        new_obs.fits_frames.append(remaining_frame)
        v2_db.flush()

        # Verify the frame is now associated with the new observation
        assert len(remaining_frame.observations) == 1
        assert new_obs in remaining_frame.observations
