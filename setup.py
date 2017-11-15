"""Fitamord package definition and install configuration"""

# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


import setuptools

import fitamord.version


# Define package attributes
setuptools.setup(

    # Basic characteristics
    name='fitamord',
    version=fitamord.version.__version__,
    url='https://github.com/afbarnard/fitamord',
    license='MIT',
    author='Aubrey Barnard',
    #author_email='',

    # Description
    description=(
        'Framework for the intelligent transformation and '
        'modeling of relational data'),
    #long_description='',
    keywords=[
        'relational data',
        'data preparation',
        'data modeling',
        'feature functions',
        'data science',
        'machine learning',
    ],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Scientific/Engineering :: Information Analysis',
    ],

    # Packaging
    #package_data={},
    # TODO include git submodules in source distribution

    # Requirements
    python_requires='~= 3.4',
    install_requires=[
        # Do not include scikit-learn as a prerequisite because it
        # installs well with `pip` only if NumPy and SciPy are already
        # installed.  Therefore attempting an install is unpredictable
        # and may lead to attempting to build everything from scratch,
        # which is definitely not desired.

        #'scikit-learn ~= 0.17',
        'PyYAML ~= 3.11',
        'barnapy ~= 0.0',
    ],

    # API
    packages=setuptools.find_packages(),
    #entry_points={}, # for scripts

)
