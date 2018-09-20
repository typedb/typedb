import pandas as pd

from ExecutionVisualiser.SpansData import SpansData
from ExecutionVisualiser.SpansDataCollection import SpansDataCollection

class RootSpansData(object):

    def __init__(self, zipkin_ES_storage, overview_data_ref, sorted_queries, sorted_concept_counts, repetitions):
        print("Begin creating RootSpansData")
        self._zipkin_ES_storage = zipkin_ES_storage
        self._overview_data_ref = overview_data_ref
        query_concepts_index, _ = pd.MultiIndex.from_product([sorted_queries, sorted_concept_counts, ["duration", "span"]], names=['query', 'concepts', 'duration_spanobject']).sortlevel() # sorted index is faster
        self._root_spans_dataframe = pd.DataFrame([], columns=query_concepts_index, index=pd.RangeIndex(repetitions))
        print("...finished creating and allocating RootSpansData pandas dataframe")

    def upsert_breakdown_for_concepts_and_queries(self, concept_count, queries):
        """ Fill in any missing data in self._toplevel_query_breakdown. Operates columnwise.
        num_concepts: int
        queries: str[]
        """
        print("Collecting query breakdown data")

        # fill in any missing data in self._toplevel_query_breakdown
        for query in queries:
            # this corresponds to a (query, num_concepts) bigcolumn in the toplevel_query_breakdown
            column = self._root_spans_dataframe[(query, concept_count)]
            not_filled = column.isnull().values.any()
            if not_filled:
                # retrieve spanId that is the parent from the duration data
                batch_span_id = self._overview_data_ref.loc[concept_count, (query, "batchSpanId")]
                # retrieve all spans with this as parent
                query_spans = self._zipkin_ES_storage.get_spans_with_parent(batch_span_id, sorting={"tags.repetition": "asc"})
                for query_span in query_spans:
                    # have to manually parse repetition into int since they're not sorted because ES isn't parsing longs correctly
                    repetition = int(query_span['tags']['repetition'])
                    self._root_spans_dataframe.loc[repetition, (query, concept_count)] = [query_span['duration'], query_span]

    def partition_for_query_and_concept_count(self, query, concept_count, partition_indices=[1]):
        """
        Split the RootSpansData into sub-sections and returns graphs (ie split rows into chunks).
        Splits UP TO the next index
        """

        partition_names = []
        partitions = []
        start_index = 0
        end_index = self._root_spans_dataframe.shape[0] # number of rows total
        partition_indices += [end_index] # edit a copy in place going to the end of the rows
        for index in partition_indices:
            partition = self._root_spans_dataframe.loc[start_index:index-1, (query, concept_count)]
            root_spans_data_collection = SpansDataCollection(label="Root")
            root_spans_data_collection.add_spans_data(
                SpansData(
                    name="0".format(start_index, index),
                    dataframe=partition,
                    zipkin_ES_storage=self._zipkin_ES_storage
                )
            )
            partitions.append(root_spans_data_collection)
            partition_names.append("Rows {0} - {1}".format(start_index, index))
            start_index = index
        return partition_names, partitions
