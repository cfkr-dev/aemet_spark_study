class PlotterStyle:
    def __init__(self):
        self._title = ''
        self._subtitle = ''
        self._plot_margins = (0.0, 0.0, 0.0, 0.0)  # (left, right, top, bottom)
        self._show_legend = True

    def set_title(self, title):
        self._title = title
        return self

    def set_subtitle(self, subtitle):
        self._subtitle = subtitle
        return self

    def set_plot_margins(self, left, right, top, bottom):
        self._plot_margins = (left, right, top, bottom)
        return self

    def set_show_legend(self, show_legend):
        self._show_legend = show_legend
        return self

    def get_title(self):
        return self._title

    def get_subtitle(self):
        return self._subtitle

    def get_plot_margins(self):
        return self._plot_margins

    def get_show_legend(self):
        return self._show_legend