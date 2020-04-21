from collections import defaultdict


def set_dict():
    return defaultdict(set)


class LightcurveManager(object):

    def __init__(self, lightcurves):
        self.tics = set_dict()
        self.apertures = set_dict()
        self.types = set_dict()
        self.id_map = dict()

        for lightcurve in lightcurves:
            self.tics[lightcurve.tic_id].add(lightcurve.id)
            self.apertures[lightcurve.aperture.name].add(lightcurve.id)
            self.types[lightcurve.lightcurve_type.name].add(lightcurve.id)
            self.id_map[lightcurve.id] = lightcurve

        self.searchables = (
            self.tics,
            self.apertures,
            self.types
        )

    def __getitem__(self, key):
        for searchable in self.searchables:
            if key in searchable:
                ids = searchable[key]
                if len(ids) == 1:
                    # Singular, just return the lightcurve
                    return self.id_map[next(iter(ids))]
                return LightcurveManager([self.id_map[id_] for id_ in ids])

        raise KeyError(
            'The keyword \'{}\' was not found in the query'.format(key)
        )

    def __len__(self):
        return len(self.id_map)

    def __iter__(self):
        return iter(self.id_map.values())
