from PlotterStyles.PlotterStyle import PlotterStyle


class LinearPlotterStyle(PlotterStyle):
    def __init__(self):
        super().__init__()

        self._xaxis_label = ''
        self._yaxis_label = ''
        self._figure_color = 'royalblue'

    def set_xaxis_label(self, label):
        self._xaxis_label = label
        return self

    def set_yaxis_label(self, label):
        self._yaxis_label = label
        return self

    def set_figure_color(self, color):
        self._figure_color = color
        return self

    # Getters (Métodos de acceso)
    def get_xaxis_label(self):
        return self._xaxis_label

    def get_yaxis_label(self):
        return self._yaxis_label

    def get_figure_color(self):
        return self._figure_color