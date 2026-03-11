# CR-TKGQA
A Temporal Knowledge Graph Question Answering Dataset Involving Complex Reasoning

# Dataset
Split of CR-TKGQA:
- **train**
- **dev**
- **test**
- **test_sample1000_seed1**: benchmark for methods in our paper, use "random.seed(1) random.sample(test)" to generate

Domain of CR-TKGQA:
- **id**
- **question**
- **question_tagged**: Question with entities and literals marked
- **answer**
- **answer_type**: Type of answer, one or more in [Entity, Time, Number, Boolean]
- **topic_entity_label_map**: Map of topic entities in question, in the form of {QID : mention}
- **gold_entity_label_map**: Map of gold entities in sparql, in the form of {QID : label}
- **gold_relation_label_map**: Map of gold properties in sparql, in the form of {PID : label}
- **sparql**
- **question_creation_date**
- **origin**: Process of construction of the question, one in [Seed, Generation, Static Entity Augmentation, Temporal Entity Augmentation, Event Time Augmentation]

Extra domain of test:
- **comp_level**: Compositional level of the question, one in [iid, compositional, zero-shot]
- **answer_entity_labels**: labels and alians of gold answer entities, used for evaluation of DirectQA & RTQA

## For Analysis 
Please turn to `analysis/sorted_dataset_analysis.py`. 
The environment needed for this script is simple, you only need to install tqdm and networkx==3.4.2.

Results are in `analysis_results`.

- run analysis/sorted_dataset_analysis.py to get the results in Tab.3 (# calculate_splits_statics), Tab.4 (# analysis_temporal_taxonomy & # analysis_split_complexity) and Tab.5 (# result_analysis)
- run analysis/statistic.py to get the results in Tab.6