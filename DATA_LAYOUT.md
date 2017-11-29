# Data layout

All model data for the server should be under INQUIRE_ROOT, on commuter this is /commuter/inquire_data_root
The structure is then `/model-name/` followed by `index`, `model` and possibly `data`
```
/default/model/
/commuter/inquire_data_root/default/index/nms_10.0M_livejournal_glove.idx
/commuter/inquire_data_root/livejournal_sample
/commuter/inquire_data_root/bookCorpus


# text corpora
/datasets/
# datasets for training models for the vector model comparison paper
/datasets/comparison/livejournal_sample
/datasets/comparison/bookCorpus

# models for generating vectors
/models
/models/lstm
/models/glove
```