# Common tasks


# Copyright (c) 2017 Aubrey Barnard.  This is free software released
# under the MIT License.  See `LICENSE.txt` for details.


.PHONY: clean allclean

clean:
	@rm -Rf build dist fitamord.egg-info

allclean: clean
	@rm -Rf $$(find -name '*~' -or -name '__pycache__')
