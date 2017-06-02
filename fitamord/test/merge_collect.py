"""Runs merge-collect on test data"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


from pprint import pprint
import sys

from barnapy import logging

from fitamord import db
from fitamord import records
from fitamord import relational
from fitamord.engines import sqlite


logging.default_config()

# Define headers for test data

demographics_header = records.Header(
    ('patient_id', int),
    ('birth_year', int),
    ('gender', str),
    ('age_at_death', float),
    )

diagnoses_header = records.Header(
    ('patient_id', int),
    ('dx_code', str),
    ('age', float),
    ('facility_num', int),
    ('prov_id', int),
    ('dx_desc', str),
    ('dx_type_id', int),
    ('dx_type_desc', str),
    ('dx_subtype_id', int),
    ('dx_subtype_desc', str),
    ('dx_code_cat', str),
    ('dx_code_cat_desc', str),
    ('dx_code_subcat', str),
    ('dx_code_subcat_desc', str),
    ('data_source', int),
    )

meds_header = records.Header(
    ('patient_id', int),
    ('age', float),
    ('gcn_seq_num', int),
    ('drug_name', str),
    ('generic_name', str),
    ('dosage', str),
    ('frequency', str),
    ('action_attribute_desc', str),
    ('action_value_desc', str),
    ('action_in_inventory_code', str),
    ('action_in_plan_code', str),
    ('therapeutic_generic_id', int),
    ('therapeutic_generic_desc', str),
    ('therapeutic_specific_id', int),
    ('therapeutic_specific_desc', str),
    ('drug_source', int),
    ('data_source', int),
    )

sqldb = sqlite.SqliteDb(sys.argv[1])
demos_tab = sqldb.table('demographics')
diags_tab = sqldb.table('diagnoses')
meds_tab = sqldb.table('meds')
demos_tab._header = demographics_header
diags_tab._header = diagnoses_header
meds_tab._header = meds_header

demos = demos_tab
diags = diags_tab
meds = meds_tab

# Project tables
diags = diags.project('patient_id', 'age', 'dx_code')
meds = meds.project('patient_id', 'age', 'gcn_seq_num')

# Collect records by patient ID
mc = relational.MergeCollect(
    (demos, 'patient_id'),
    (diags, 'patient_id'),
    (meds, 'patient_id'),
    )

print()
for gr in mc:
    pprint(list(gr.names_records()))
    print()
print()
sys.stdout.flush()

sqldb.close()
