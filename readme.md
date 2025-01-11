### Example of initialising python -m venv {name_of_environment}
python -m venv env

### Activate venv
env\Scripts\activate.bat

### Linux
# source env/bin/activate.bat

### Deactivate virtual env
deactivate

### List all packages
pip list
pip install -r requirements.txt


### Find all packages and store in requirements.txt file
pip freeze > requirements.txt
pip freeze | sed 's/==.*//' > requirements.txt


### Remove virtual env
rm -r venv