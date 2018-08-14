
## --- Kibana Dev Tools commands ----

```
PUT full_indexed_fixed_7
{
  "settings": {
    "index": {
      "number_of_shards": "5",
      "requests": {
        "cache": {
          "enable": "true"
        }
      },
      "analysis": {
        "filter": {
          "traceId_filter": {
            "type": "pattern_capture",
            "preserve_original": "true",
            "patterns": [
              "([0-9a-f]{1,16})$"
            ]
          }
        },
        "analyzer": {
          "traceId_analyzer": {
            "filter": "traceId_filter",
            "type": "custom",
            "tokenizer": "keyword"
          }
        }
      },
      "number_of_replicas": "1"
    }
  },
  "defaults": {
    "index": {
      "max_ngram_diff": "1",
      "translog": {
        "generation_threshold_size": "64mb",
        "flush_threshold_size": "512mb",
        "sync_interval": "5s",
        "retention": {
          "size": "512mb",
          "age": "12h"
        },
        "durability": "REQUEST"
      },
      "auto_expand_replicas": "false",
      "max_inner_result_window": "100",
      "unassigned": {
        "node_left": {
          "delayed_timeout": "1m"
        }
      },
      "max_terms_count": "65536",
      "data_path": "",
      "highlight": {
        "max_analyzed_offset": "-1"
      },
      "routing": {
        "rebalance": {
          "enable": "all"
        },
        "allocation": {
          "enable": "all",
          "total_shards_per_node": "-1"
        }
      },
      "search": {
        "slowlog": {
          "level": "TRACE",
          "threshold": {
            "fetch": {
              "warn": "-1",
              "trace": "-1",
              "debug": "-1",
              "info": "-1"
            },
            "query": {
              "warn": "-1",
              "trace": "-1",
              "debug": "-1",
              "info": "-1"
            }
          }
        }
      },
      "fielddata": {
        "cache": "node"
      },
      "routing_partition_size": "1",
      "max_docvalue_fields_search": "100",
      "merge": {
        "scheduler": {
          "max_thread_count": "4",
          "auto_throttle": "true",
          "max_merge_count": "9"
        },
        "policy": {
          "reclaim_deletes_weight": "2.0",
          "floor_segment": "2mb",
          "max_merge_at_once_explicit": "30",
          "max_merge_at_once": "10",
          "max_merged_segment": "5gb",
          "expunge_deletes_allowed": "10.0",
          "segments_per_tier": "10.0"
        }
      },
      "max_refresh_listeners": "1000",
      "max_slices_per_scroll": "1024",
      "shard": {
        "check_on_startup": "false"
      },
      "load_fixed_bitset_filters_eagerly": "true",
      "number_of_routing_shards": "5",
      "write": {
        "wait_for_active_shards": "1"
      },
      "xpack": {
        "watcher": {
          "template": {
            "version": ""
          }
        },
        "version": ""
      },
      "percolator": {
        "map_unmapped_fields_as_text": "false",
        "map_unmapped_fields_as_string": "false"
      },
      "allocation": {
        "max_retries": "5"
      },
      "mapping": {
        "coerce": "true",
        "nested_fields": {
          "limit": "50"
        },
        "depth": {
          "limit": "20"
        },
        "ignore_malformed": "false",
        "total_fields": {
          "limit": "1000"
        }
      },
      "refresh_interval": "1s",
      "indexing": {
        "slowlog": {
          "reformat": "true",
          "threshold": {
            "index": {
              "warn": "-1",
              "trace": "-1",
              "debug": "-1",
              "info": "-1"
            }
          },
          "source": "1000",
          "level": "TRACE"
        }
      },
      "compound_format": "0.1",
      "blocks": {
        "metadata": "false",
        "read": "false",
        "read_only_allow_delete": "false",
        "read_only": "false",
        "write": "false"
      },
      "max_script_fields": "32",
      "query": {
        "default_field": [
          "*"
        ],
        "parse": {
          "allow_unmapped_fields": "true"
        }
      },
      "format": "0",
      "max_result_window": "10000",
      "sort": {
        "missing": [],
        "mode": [],
        "field": [],
        "order": []
      },
      "store": {
        "stats_refresh_interval": "10s",
        "type": "",
        "fs": {
          "fs_lock": "native"
        },
        "preload": []
      },
      "priority": "1",
      "queries": {
        "cache": {
          "everything": "false",
          "enabled": "true"
        }
      },
      "ttl": {
        "disable_purge": "false"
      },
      "warmer": {
        "enabled": "true"
      },
      "codec": "default",
      "max_rescore_window": "10000",
      "max_adjacency_matrix_filters": "100",
      "max_shingle_diff": "3",
      "gc_deletes": "60s",
      "optimize_auto_generated_id": "true",
      "query_string": {
        "lenient": "false"
      }
    }
  },
  "mappings": {
    "span": {
      "properties": {
        "duration": {
          "type": "long"
        },
        "id": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "localEndpoint": {
          "properties": {
            "ipv4": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword"
                }
              }
            },
            "serviceName": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword"
                }
              }
            }
          }
        },
        "name": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "parentId": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "tags": {
          "properties": {
            "dataSetName": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword"
                }
              }
            },
            "numConcepts": {
              "type": "long"
            },
            "query": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword"
                }
              }
            },
            "repetition": {
              "type": "long"
            },
            "runStartDateTime": {
              "type": "long"
            }
          }
        },
        "timestamp": {
          "type": "long"
        },
        "timestamp_millis": {
          "type": "long"
        },
        "traceId": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        }
      }
    }
  }
}
```

```
POST _reindex
{
  "source": {
    "index": "benchmarking:span-2018-08-08"
  },
  "dest": {
    "index": "full_indexed_fixed_7"
  }
}
```
