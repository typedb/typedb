class SpansDataCollection(object):
    """
    Container for a list of SpansData objects
    Conceptually, this is used to display one row, aka Level, aka a list of children of a Span/set of spans
    """

    def __init__(self, label):
        self._spans_data_list = []
        self._label = label
        self.descends_from = None

    def add_spans_data(self, spans_data):
        """ Append a SpansData object to the internal list, preserving order """
        self._spans_data_list.append(spans_data)

    def get_child_spans_data_collection_of(self, children_of=0):
        """" Retrieve the SpansDataCollection represnting children of the indicated SpansData """
        child_spans_data_collection = self._spans_data_list[children_of].get_child_spans_data_collection()
        child_spans_data_collection.descends_from = children_of
        return child_spans_data_collection

    def iterator(self):
        """ Retrieve iterator over the internal list of SpansData """
        return iter(self._spans_data_list)

    def get_label(self):
        """ Retrieve the label for this collection of child SpansData / this Level"""
        return self._label

    def get_size(self):
        """ Get the number of children / number of SpansData in this collection """
        return len(self._spans_data_list)

    def get_contained(self, number):
        return self._spans_data_list[number]
