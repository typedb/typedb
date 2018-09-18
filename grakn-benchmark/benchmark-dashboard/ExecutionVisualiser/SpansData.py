import abc
import numpy as np
import pandas as pd
from collections import Counter

from ExecutionVisualiser.SpansDataCollection import SpansDataCollection

class SpansData(object):
    def __init__(self, name, dataframe, zipkin_ES_storage):
        self._assigned_name = name
        self._dataframe = dataframe
        self._zipkin_ES_storage = zipkin_ES_storage
        self._span_ids = self._get_span_ids()
        self._common_data = self._compute_common_data()

    def get_num_rows(self):
        return self._dataframe.shape[0]

    def get_assigned_name(self):
        return self._assigned_name

    def get_spans_name(self):
        return self._common_data['name']

    def get_repetition_count(self):
        return len(self._span_ids)

    def get_number_of_children(self):
        an_id = self._span_ids[0]
        return self._zipkin_ES_storage.get_number_of_children(parent_id=an_id)

    @abc.abstractmethod
    def _get_span_ids(self):
        pass

    @abc.abstractmethod
    def _compute_common_data(self):
        pass

    @abc.abstractmethod
    def get_values_np(self):
        pass

    @staticmethod
    def _compute_common_span_data(spans):
        all_data = []
        for span in spans:
            items = set([])
            for (key, value) in span.items():
                try:
                    items.add((key, value))
                except TypeError:
                    continue # not hashable type
            all_data.append(items)
        # perform intersection over all these sets to find common keys
        return dict(set.intersection(*all_data))

    def get_child_spans_data_collection(self):
        repetitions = self.get_repetition_count()
        columns_index = pd.RangeIndex(repetitions)
        number_of_children = self.get_number_of_children()
        rows_index = pd.MultiIndex.from_product([np.arange(number_of_children), ["duration", "span"]], names=["orderedchild", "duration_spanobject"])
        child_data = pd.DataFrame(index=rows_index, columns=columns_index)

        # This has been rewritten to do the following:
        # because not every child has a unique name
        # we need to retrieve all children of a parentID, sorted by timestamp/SOMETHING ELSE TODO
        # these are inserted into a dataframe
        # once we have a table of children vs repetition,
        # take columns of the table as SpanLists

        for i, parent_id in enumerate(self._get_span_ids()):
            sorted_child_spans = self._zipkin_ES_storage.get_spans_with_parent(parent_id, sorting={"timestamp": "asc"})
            durations = [span['duration'] for span in sorted_child_spans]
            child_data.loc[(slice(None), "duration"), i] = durations
            child_data.loc[(slice(None), "span"), i] = sorted_child_spans

        child_spans_data_collection = SpansDataCollection(
            label="children of {0}".format(self.get_spans_name())
        )

        # take Transpose, then use columns to create new spanlists
        child_data = child_data.T
        for col in child_data.columns.levels[0].unique():
            column_data = child_data.loc[:, col]
            counts = Counter(x['name'] for x in column_data.loc[:, "span"])

            # TODO short term hack to ignore out of order rows by voting
            argmax = counts.most_common(1)[0][0]
            matches = [True if span['name'] == argmax else False for span in column_data.loc[:, "span"]]
            matching_rows = column_data.loc[matches]

            if len(counts) > 1:
                print(counts)
                print("HELP! Out of order sorting??")
                print("Fixed by ignoring by voting")
                print("TODO fix elasticsearch properly...")

            child_spans_data = ChildSpansData(
                name="child",
                dataframe=matching_rows,
                zipkin_ES_storage=self._zipkin_ES_storage
            )
            child_spans_data_collection.add_spans_data(child_spans_data)

        return child_spans_data_collection


class PartitionedRootSpansData(SpansData):
    """ Specialise SpansData object for root level SpanData, that have their own indices """

    def __init__(self, name, partitioned_dataframe, zipkin_ES_storage):
        super().__init__(name, partitioned_dataframe, zipkin_ES_storage)


    def _get_span_ids(self):
        """ Override abstract, access internal dataframe indices and return all span IDS """
        # spans = self._dataframe.xs('span', level='duration_spanobject', axis=1) # get the raw _source dictionaries
        spans = self._dataframe['span']
        spans = spans.values.ravel()
        span_ids = [span['id'] for span in spans]
        return span_ids

    def _compute_common_data(self):
        """ Override abstract, access internal dataframe => spans, compute set of common attributes of spans """
        # spans = self._dataframe.xs('span', level='duration_spanobject', axis=1) # get the raw _source dictionaries
        spans = self._dataframe['span']
        spans = spans.values.ravel()
        return SpansData._compute_common_span_data(spans)

    def get_values_np(self):
        """ Override abstract, access internal dataframe => durations as flat numpy array """
        return self._dataframe['duration'].values.ravel()
        # return self._dataframe.xs('duration', level='duration_spanobject', axis=1).values.ravel()




class ChildSpansData(SpansData):
    """ Specialise SpansData object for child spans, which have simpler indices """

    def __init__(self, name, dataframe, zipkin_ES_storage):
        super().__init__(name, dataframe, zipkin_ES_storage)

    def _get_span_ids(self):
        """ Override abstract, access internal dataframe indices and return all span IDS """
        spans = self._dataframe["span"]
        spans = spans.values.ravel()
        span_ids = [span['id'] for span in spans]
        return span_ids

    def _compute_common_data(self):
        """ Override abstract, access internal dataframe => spans, compute set of common attributes of spans """
        spans = self._dataframe["span"]
        spans = spans.values.ravel()
        return SpansData._compute_common_span_data(spans)


    def get_values_np(self):
        """ Override abstract, access internal dataframe => spans, compute set of common attributes of spans """
        return self._dataframe['duration'].values.ravel()
