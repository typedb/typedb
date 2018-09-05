import pandas as pd
import numpy as np


class SpanList(object):
    """ 
    A Holder of slices of the same data from different executions of the same experiment 
    (eg. all the children of the batch span called "server queue")
    These slices can then be queries for their values
    Or can be asked to generate a list of child SpanLists than each contain conrresponding
    spans that are children of the current SpanLists

    In short, container for Spans of the same type/name, useful for aggregation & requesting children
    """

    def __init__(self, es_utility, dataframe, name):
        """
        Takes an [excerpt of] a dataframe with columns (query, concepts, ("duration", "span")) 
        Rows are elements to aggregate on retrieval 
        """
        self.es_utility = es_utility
        self.dataframe = dataframe
        self.name = name

        # obtain grandchild names for reference
        self.child_names = self._retrieve_child_names()

        # compute commonalities to this set of spans
        if type(dataframe.columns) == pd.Index:
            spans = dataframe["span"]
        elif type(dataframe.columns) == pd.MultiIndex:
            spans = dataframe.xs('span', level='duration_spanobject', axis=1)# get the raw _source dictionary
        else:
            print("Unknown column index type: {0}".format(type(dataframe.column)))
            return
        spans = spans.values.ravel()
        all_data = []
        self.all_span_ids = []
        for span in spans:
            self.all_span_ids.append(span['id'])
            items = set([]) 
            for (key, value) in span.items():
                try:
                    items.add((key, value))
                except TypeError:
                    continue # not hashable type
            all_data.append(items)
        # perform intersection over all these sets to find common keys
        self.common_data = dict(set.intersection(*all_data))

    def get_num_rows(self):
        return self.dataframe.shape[0]

    def get_name(self):
        return self.common_data['name']

    def get_mean(self):
        """ Compute mean of these durations """
        return self.dataframe['duration'].mean()

    def get_stddev(self):
        """ Compute sample standard deviation of the duration column """
        return self.dataframe['duration'].std()

    def get_values_np(self):
        if type(self.dataframe.columns) == pd.Index:
            raw_values = self.dataframe['duration'].values.ravel()
            return raw_values
        elif type(self.dataframe.columns) == pd.MultiIndex:
            raw_values = self.dataframe.xs('duration', level='duration_spanobject', axis=1).values.ravel()
            return raw_values
        else:
            print("Unknown column index type: {0}".format(type(self.dataframe.column)))
            return

    def _retrieve_child_names(self):
        """ obtain the names of the children of the children for later use """
        if len(self.dataframe) == 0:
            return

        # retrieve first row
        a_child = self.dataframe.iloc[0]
        # compute commonalities to this set of spans
        if type(a_child.index) == pd.Index:
            child_span = a_child.xs("span")
        elif type(a_child.index) == pd.MultiIndex:
            child_span = a_child.xs('span', level='duration_spanobject')[0]
        else:
            print("Unknown column index type: {0}".format(type(a_child.column)))
            return
        child_span_id = child_span['id']
        return set(self.es_utility.get_child_names(parent_id=child_span_id))

   
    def get_child_spanlists(self):
        
        repetitions = len(self.all_span_ids)
        columns_index = pd.RangeIndex(repetitions)
        number_of_children = self.es_utility.get_number_of_children(self.all_span_ids[0])
        rows_index = pd.MultiIndex.from_product([np.arange(number_of_children), ["duration", "span"]], names=["orderedchild", "duration_spanobject"])
#        rows_index = pd.MultiIndex([["child"],["duration", "span"]], labels=[[0,0], [0,1]], names=["orderedchild", "duration_spanobject"])
        child_data = pd.DataFrame(index=rows_index, columns=columns_index)

        # This has been rewritten to do the following:
        # because not every child has a unique name
        # we need to retrieve all children of a parentID, sorted by timestamp
        # these are inserted into a dataframe
        # once we have a table of children vs repetition,
        # take columns of the table as SpanLists

        for i, parent_id in enumerate(self.all_span_ids):
            sorted_child_spans = self.es_utility.get_spans_with_parent(parent_id, sorting={"timestamp": "asc"})
            durations = [span['duration'] for span in sorted_child_spans]
            child_data.loc[(slice(None), "duration"), i] = durations
            child_data.loc[(slice(None), "span"), i] = sorted_child_spans


        # take Transpose, then use columns to create new spanlists
        child_data = child_data.T
        child_spanlists = []
        from collections import Counter
        for col in child_data.columns.levels[0].unique():
            column_data = child_data.loc[:, col]
            counts = Counter(x['name'] for x in column_data.loc[:, "span"])
            print(counts)
            if len(counts) > 1:
                print("HELP! Out of order sorting??")
                print(column_data)

            child_spanlists.append(SpanList(self.es_utility, child_data.loc[:, col], name="child"))

        return child_spanlists
