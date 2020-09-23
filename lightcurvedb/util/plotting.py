class Plotter(object):
    data_cols = []

    def __init__(self, **figure_kwargs):
        self.fig = None
        self.current_figure = None

    def histogram_plot(self, columns, **plot_kwargs):
        for column in columns:
            self.current_figure.hist(
                column,
                **plot_kwargs
            )


class ASCIIPlotable(object):

    def plot(self):
        pass
