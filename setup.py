"""Fitamord package definition and install configuration"""

# Copyright (c) 2017-2018, 2023 Aubrey Barnard.
#
# This is free software released under the MIT License.  See
# `LICENSE.txt` for details.


import setuptools

import fitamord


# Extract the descriptions from the package documentation
_desc_paragraphs = fitamord.__doc__.strip().split('\n\n')
_desc_short = _desc_paragraphs[0].replace('\n', ' ') # Needs to be one line
_desc_long = '\n\n'.join(_desc_paragraphs[1:-2])


# Define package attributes
setuptools.setup(

    # Basic characteristics
    name='fitamord',
    version=fitamord.__version__,
    url='https://github.com/afbarnard/fitamord',
    license='MIT',
    author='Aubrey Barnard',
    #author_email='',

    # Description
    description=_desc_short,
    long_description=_desc_long,
    keywords=[
        'relational data',
        'data preparation',
        'data modeling',
        'feature functions',
        'data science',
        'machine learning',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Scientific/Engineering :: Information Analysis',
    ],

    # Requirements
    python_requires='~= 3.4',
    install_requires=[
        'barnapy ~= 0.1',
        'esal ~= 0.3',
        'psutil',
        'PyYAML',
    ],

    # API
    packages=setuptools.find_packages(),
    #entry_points={}, # for scripts

)
