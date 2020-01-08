# Countries of interest service
The countries of interest service aims to provide insight into the countries that a specific company is interested in or currently exporting to. The service makes use several data sources including,

* OMIS orders
* Datahub company profiles

to give the most informed view into a companies intereset in particular countries.

## Installation
The backend is built in Python using the Flask framework. Authentication implemented using Hawk and OAUTH2(SSO) authentication. The majority of the functionality is through API calls but a light front end is provided for documentation and dashboarding. This front end uses React and d3 and uses the webpack javascript module bundler. 

### local installation
1. Create a python virtual environment. Below are instructions on how to do this with `Anaconda` but other virtual environments are possible.
    <br />`conda create -n countries_of_interest_service python=3`
2. Activate the virtual environment
    <br />`source activate countries_of_interest_service`
3. Install the python dependencies
    <br />`pip install -r requirements.txt`
4. Install Node package manager
    <br />https://www.npmjs.com/get-npm
5. Install the required node packages
    <br />`npm install` # from the project base directory
6. Build javascript bundles
    <br />`npm run build`
7. Setup environment variables, an example environment variable file can be found in the `envs` directory. I recommend that the virtual environment sets the evnironemnts with this file when the environment is activated. For anaconda you can do this in the `env_vars.sh` script which is located at `${ANACONDA_HOME}/envs/countries_of_interest_service/etc/conda/activate.d`
8. run the app
    <br />`python app.py`
9. go to `http://localhost:5000`

### local Docker installation
1. Move your environment variables file to `$PROJECT_HOME/.env`
2. `docker-compose build`
3. `docker-compose run`

You can configure how `docker-compose` runs with environment variables,
* change the port the application runs on with the `PORT` environment variable
* change the environment file with the `ENV_FILE` environment variable
* e.g. `PORT=8000 ENV_FILE=my_envs docker-compose build` and again when using `docker-compose up`

## Testing
From the project base directory use the command,

`FLASK_ENV=test python -m unittest`

to run tests for a specific test module, do,

`FLASK_ENV=test python -m unittest tests.<test_module>`

to run tests in a specific directory do,

`FLASK_ENV=test python -m unittest discover -s <test directory>`

### running tests in Docker
`docker-compose build; docker-compose run -e FLASK_ENV=test web python -m unittest`


### deployment

#### deploy app and build route
`cf push countries-of-interest-service`

#### configure app disk space
`cf v3-scale countries-of-interest-service -k 3G`

#### create services
`cf create-service postgres small-10 countries-of-interest-service-db`  
`cf create-service redis tiny-3.2 countries-of-interest-service-redis`

once services and app have been created

#### bind services
`cf bind-service countries-of-interest-service-staging countries-of-interest-service-db`  
`cf bind-service countries-of-interest-service-staging countries-of-interest-service-redis`

#### ssh into cloud foundry
`cf ssh`

#### activate conda envionment
`source /deps/0/conda/bin/activate`

#### activate conda environment for app
`source activate dep_env`

#### create tables
`python manage.py dev db --create_tables # create tables`

#### add hawk users
`python manage.py dev add_hawk_user --client_id=<client_id> --client_key=<client_key> --client_scope=* --description=data-flow`

#### add celery worker
`cf v3-scale countries-of-interest-service --process worker -i 1 -k 3G`

#### deploy with vault environment variables
deploy via jenkins > Build with Parameters
