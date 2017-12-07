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


License
-------

This software is free, open source software.  It is released under the
MIT License, contained in the file `LICENSE.txt`.


Requirements
------------

* Python ~= 3.4
* PyYaml ~= 3.11
* [Barnapy](https://github.com/afbarnard/barnapy) ~= 0.0


Download, Install
-----------------

Note that you may first want to create a virtual environment, a space
that can have its own versions of Python and packages.  A summary of how
to do this is below, but [the details are
here](https://packaging.python.org/installing/#creating-virtual-environments).
The alternative is to install things into your user account (home
directory).

0. Optional steps.

   1. Create a virtual environment.

          python3 -m venv <fitamord-venv>
          cd <fitamord-venv>
          source bin/activate
          ... # install, update, do work, etc.
          deactivate; cd .. # exit virtual environment
          rm -Rf <fitamord-venv> # delete virtual environment

   2. Update Pip and Setuptools.  You can do this globally for your user
      or locally in your virtual environment.

          python3 -m pip install --upgrade pip
          python3 -m pip install --upgrade setuptools

If you are using a virtual environment, you may omit the `--user`
options from the commands below.  The `--user` option directs Python to
install things in your user's home directory rather than the system
directories, but a virtual environment has its own "system" directories.
For more information, read the [Pip documentation](https://pip.pypa.io).


### Quick and Easy ###

1. Download and install.  Fitamord depends on
   [Barnapy](https://github.com/afbarnard/barnapy) so install it too.

       python3 -m pip install --user git+https://github.com/afbarnard/fitamord.git#egg=fitamord git+https://github.com/afbarnard/barnapy.git#egg=barnapy

2. Update.

       python3 -m pip install --user --upgrade git+https://github.com/afbarnard/fitamord.git#egg=fitamord git+https://github.com/afbarnard/barnapy.git#egg=barnapy

3. Uninstall.

       python3 -m pip uninstall --yes fitamord barnapy


### For Development or Reference ###

Alternatively, use these instructions if you want access to the Fitamord
repository (e.g. for reference, debugging, or development) or if you
want more control.

1. Download and install.  Note the `--editable` option.  This is what
   tells Pip to download the repository to `src/fitamord`.  (Use the
   `--src` option to specify a different download location.)

       python3 -m pip install --user --editable git+https://github.com/afbarnard/fitamord.git#egg=fitamord git+https://github.com/afbarnard/barnapy.git#egg=barnapy

2. Update.

       python3 -m pip install --user --upgrade --editable git+https://github.com/afbarnard/fitamord.git#egg=fitamord git+https://github.com/afbarnard/barnapy.git#egg=barnapy

3. Uninstall.

       python3 -m pip uninstall --yes fitamord barnapy

For more control you can use Git and Pip directly.

    # Install dependencies (or use `requirements.txt`; see below)
    python3 -m pip install --user git+https://github.com/afbarnard/barnapy.git#egg=barnapy
    # Download Fitamord to <fitamord-dir> (`fitamord` by default)
    git clone https://github.com/afbarnard/fitamord.git [<fitamord-dir>]
    # Install Fitamord
    python3 -m pip install --user [--editable] <fitamord-dir>
    # Update
    git -C <fitamord-dir> pull
    # Upgrade if the install is not editable
    python3 -m pip install --user --upgrade <fitamord-dir>
    # Uninstall
    python3 -m pip uninstall --yes fitamord barnapy
    rm -Rf <fitamord-dir>

You can also install Fitamord and its dependencies by telling Pip to use
the requirements file after downloading the Git repository, but this
does not make the installation "editable".  Of course, you can always
convert a plain install into an editable one by doing an upgrade with
the `--editable` option.

    git clone https://github.com/afbarnard/fitamord.git [<fitamord-dir>]
    python3 -m pip install --user --requirement <fitamord-dir>/requirements.txt
    # Run this command only if you want an editable install
    python3 -m pip install --user --upgrade --editable <fitamord-dir>

The difference between an editable install and a plain install is that
an editable install points directly to the "live" files in the
repository while a plain install copies the necessary files into a
well-known location
(e.g. `~/.local/lib/python3.<minor>/site-packages/`).  An editable
install can be convenient for trying changes, but is usually unnecessary
(as one can always run Python from the repository directory, which makes
the repository files "live" because they appear first on the path) and
is unsafe for long-running jobs like Fitamord (i.e. updating the
repository or editing a module while a job is running can cause that job
to crash or give incorrect results if it loads a module while that
module is in an inconsistent state).  This is why the requirements file
specifies a non-editable install.


Usage
-----

The essential thing is to tell Fitamord how to interpret your data.  You
do this through a configuration file.  If you run Fitamord without a
configuration, it will guess at a configuration for you based on all the
CSV files in the current directory.  So, to get started, change to your
data directory and run Fitamord to create a stub configuration.

    cd <data-directory>
    python3 -m fitamord 2>fitamord.log

In `fitamord_config.generated.yaml` you will find what Fitamord is using
as its configuration.  In this case, since it started without any other
configuration, this is what it detected, guessed, and assumed.  Fitamord
will always use the configuration you specify and fill it in with what
it can detect or assume.

Fitamord will exit with an error that it did not find any tables with
examples.  This is because Fitamord can't guess whether tables should be
treated as facts, events, or examples.  You need to tell Fitamord this
information in the configuration.

Edit the configuration with a text editor to only include the relevant
files as tables and to fill in each `treat as` field with "facts",
"events", or "examples".  A fact is a data point without a timestamp
(id, data), an event is a data point with a timestamp (id, time, data),
and an example describes the study design (id, start_time, stop_time,
label).  Note that for events and examples, Fitamord expects the fields
in the order just given, so you may need to fill in the `use` option
with a list of column numbers to say which fields to use and in what
order.  Similarly, fill in `id` with the column number of the data ID if
it is not the first column.  Finally, make sure the columns you are
using have the correct data types.  Note that you can pare down your
configuration by deleting anything that will have the same value as in
the generated configuration.  Here is an example small configuration
defining the data for a study of patients based on their demographics,
drugs, and conditions.

    %YAML 1.2
    ---
    is_missing:
      - ''
      - '?'
      - na
      - nil
      - none
      - 'null'
      - '*not available'
      - '* not available'
    positive_label: G
    tables:
      demos:
        file: demographics.csv
        columns:
          pt_id: int
          birth_year: int
          sex: str
          death_age: float
        treat as: facts
      rxs:
        file: meds.csv
        columns:
          pt_id: int
          age: float
          rx_code: int
        treat as: events
      dxs:
        file: diagnoses.csv
        columns:
          pt_id: int
          dx_code: str
          age: float
        use: 1, 3, 2
        treat as: events
      periods:
        file: med_periods.csv
        columns:
          pt_id: int
          start_age: float
          stop_age: float
          dose: float
          label: str
        use: 1, 2, 3, 5
        treat as: examples
    ...

You may want to update the list of (case insensitive) values that
signify missing information, and make sure to configure the correct
postive label.

Save your edited configuration as `fitamord_config.yaml`.  (Fitamord
overwrites `fitamord_config.generated.yaml` each time with its
operational configuration.)

Now, run Fitamord again to transform your facts and events into feature
vectors.

    python3 -m fitamord 1>feature_vector_data.svmlight 2>fitamord.log

The table of features will be in `features.generated.csv`.  Lastly,
check the log for errors, warnings, and other information.


-----

Copyright (c) 2017 Aubrey Barnard.  This is free software released under
the MIT License.  See `LICENSE.txt` for details.
