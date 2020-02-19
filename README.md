# Countries of interest service
The countries of interest service aims to provide insight into the countries that a specific company is interested in or currently exporting to. The service makes use several data sources including,

* Export wins
* Data Hub interactions
* OMIS orders
* Data Hub company profiles

to give the most informed view into a companies intereset in particular countries.

## Installation
The backend is built in Python using the Flask framework. Authentication implemented using Hawk and OAUTH2(SSO) authentication. The majority of the functionality is through API calls but a light front end is provided for documentation and dashboarding. This front end uses React and d3 and uses the webpack javascript module bundler.

### local Docker installation
1. Move your environment variables file to `$PROJECT_HOME/.env`
2. `docker-compose build`
3. `docker-compose up`

You can configure how `docker-compose` runs with environment variables,
* change the port the application runs on with the `PORT` environment variable
* change the environment file with the `ENV_FILE` environment variable
* e.g. `PORT=8000 ENV_FILE=my_envs docker-compose build` and again when using `docker-compose up`

Running postgres in docker now requires a mandatory environment variable called POSTGRES_PASSWORD. This must be added to your .env file.

## Config

## When using docker-compose
Place environment variables in .env file.

## When using host machine
Config variables can be specified in a few ways and are loaded using the following order of priority:

#### 1. Look for variable in existing System environment variables
#### 2. If not found in step 1, look for variable in `.env` (this only works if USE_DOTENV is set to 1)
#### 3. If not found in step 2, look for variable in `local_testing.yml` (this only works if TESTING is set to 1)
#### 4. If not found in step 3, look for variable in `local.yml` (this only works if TESTING is set to 0)
#### 5. If not found in step 4, look for variable in `default.yml`

## Testing

Running tests locally

`make run_tests_local`

to run tests for a specific directory, do,

`make run_tests_local TEST=<tests/test_directory>`

Running tests the same way as circle ci:

`make run_tests`

## Deployment

### deploy app and build route
`cf push countries-of-interest-service`

### configure app disk space
`cf v3-scale countries-of-interest-service -k 3G`

### create services
`cf create-service postgres small-10 countries-of-interest-service-db`
`cf create-service redis tiny-3.2 countries-of-interest-service-redis`

once services and app have been created

### bind services
`cf bind-service countries-of-interest-service countries-of-interest-service-db`
`cf bind-service countries-of-interest-service countries-of-interest-service-redis`

### deploy with vault environment variables
deploy via jenkins > Build with Parameters

### ssh into cloud foundry
`cf ssh countries-of-interest-service`

### create an app shell
`/tmp/lifecycle/shell`

### create tables
`python app/manage.py dev db --create_tables # create tables`

### add hawk users
`python app/manage.py dev add_hawk_user --client_id=<client_id> --client_key=<client_key> --client_scope=* --description=data-flow`

### add celery worker
`cf v3-scale countries-of-interest-service --process worker -i 1 -k 3G`

## Running the interactions algorithm
`cf run-task --name interaction_coi_extraction -m 2G -k 3G countries-of-interest-service "./manage.py algorithm interaction_coi_extraction"`

## Recreating tables
`cf run-task --name recreate_tables countries-of-interest-service "./manage.py db --recreate_tables"`
