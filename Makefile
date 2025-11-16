.PHONY: install uninstall clean

install:
	pip3 install . -r requirements.txt

uninstall:
	pip3 uninstall work-database

clean:
	rm -r build work_database.egg-info
