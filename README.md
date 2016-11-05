Fitamord
========


Fitamord is a framework for the intelligent transformation and modeling
of relational data.  A fit, flexible framework is an intelligent choice
leading to satisfaction and love (amor) compared to ad hoc code leading
to frustration, despair, and murder (mord).

Fitamord simplifies preparing data for machine learning, statistical
modeling, and other forms of analysis.  It supports cleaning and
translating data, transforming relational data into feature vectors, and
flexibly defining labels and study designs.  Fitamord was originally
conceived and developed for conducting comparative observational studies
of electronic medical records data using machine learning, but hopefully
you will find it generally useful for modeling with relational data.


Facts and Events
----------------

Fitamord works with data in terms of groups of facts all related to a
particular subject.  Fitamord already understands the following types of
facts and can be extended to other types of facts.  Temporal facts are
interpreted as event sequences / timelines.  The type of fact is the
relation it came from.

* Generic fact (subject_id, fact_type, data)
* Existence event (subject_id, event_type, time)
* Value event (subject_id, event_type, time, value)


Requirements
------------

* Python >= 3.4
* PyYaml >= 3.11


-----

Copyright (c) 2016 Aubrey Barnard.  This is free software released under
the MIT License.  See `LICENSE.txt` for details.
