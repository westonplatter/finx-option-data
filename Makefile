test:
	pytest .

env.update:
	pip install -r requirements/requirements.txt

#------------------------------------------------------------------------------
# Backup
#------------------------------------------------------------------------------

backup:
	cp analysis*.ipynb ../forecasters/fod
