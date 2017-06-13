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

* Python >= 3.4
* PyYaml >= 3.11


Download, Install
-----------------

Note that you may first want to create a virtual environment, a space
that can have its own versions of Python and packages.  A summary of how
to do this is below, but (the details are
here)[https://packaging.python.org/installing/#creating-virtual-environments].
The alternative is to install things into your user account.

0.1 Optionally create a virtual environment.

        python3 -m venv <fitamord-venv>
        cd <fitamord-venv>
        source bin/activate
        ... # install, update, do work, etc.
        deactivate; cd .. # exit virtual environment
        rm -Rf <fitamord-venv> # delete virtual environment

0.2 Optionally update Pip and Setuptools.  You can do this globally for
    your user or locally in your virtual environment.

        python3 -m pip install --upgrade pip
        python3 -m pip install --upgrade setuptools

If you are using a virtual environment, you may omit the `--user`
options from the commands below.  The `--user` option directs Python to
install things in your user's home directory rather than the system
directories, but a virtual environment has its own "system" directories.
For more information, read the (Pip documentation)[https://pip.pypa.io].

1. Download and install in one go.  The `--editable` option instructs
   Pip to make a local copy of the repository for editing (development)
   and is necessary for Pip to download Git submodules.  Pip downloads
   the repository to `src/fitamord`.

        python3 -m pip install --editable git+https://github.com/afbarnard/fitamord.git#egg=fitamord

2. Update.

        python3 -m pip install --upgrade fitamord

3. Uninstall.

        python3 -m pip uninstall fitamord

The above is equivalent to the following individual steps which give you
more control and may be more suitable outside a virtual environment or
when developing the software.

    git clone --recursive https://github.com/afbarnard/fitamord.git [<fitamord-dir>] # download (to `fitamord` by default)
    python3 -m pip install --user --editable <fitamord-dir> # install
    python3 -m pip install --upgrade fitamord # update
    python3 -m pip uninstall fitamord # uninstall
    rm -R fitamord # remove repository

Or, the same thing without Pip (not recommended, but illustrative):

    git clone --recursive https://github.com/afbarnard/fitamord.git [<fitamord-dir>] # download (to `fitamord` by default)
    cd <fitamord-dir>
    python3 setup.py develop --user # install
    git pull # update
    # manual uninstall
    cd ..
    rm -R <fitamord-dir> # remove repository
    cd ~/.loca/lib/python3.<minor>/site-packages
    sed -i.bak -e '/<fitamord-dir>/ d' easy-install.pth # delete <fitamord-dir> from `easy-install.pth`
    rm fitamord.egg-link


-----

Copyright (c) 2017 Aubrey Barnard.  This is free software released under
the MIT License.  See `LICENSE.txt` for details.
