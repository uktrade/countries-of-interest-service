# Countries of interest service
The countries of interest service aims to provide insight into the countries that a specific company is interested in or currently exporting to. The service makes use several data sources including,

* OMIS orders
* Datahub company profiles

to give the most informed view into a companies intereset in particular countries.

## Installation
The backend is built in Python using the Flask framework. Authentication implemented using Hawk and OAUTH2(SSO) authentication. The majority of the functionality is through API calls but a light front end is provided for documentation and dashboarding. This front end uses React and d3 and uses the webpack javascript module bundler. 

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
7. Setup environment variables, the necessary environment variables can be found in the `envs/activate.d/env_vars.sh` file, you can either do this manually or if you're using `conda` environments you can create activate and deactivate scripts to set the environment variables, see below.
    1. `mkdir -p ~/anaconda3/envs/countries_of_interest_service/etc/conda/activate.d`
    2. `cp envs/activate.d/env_vars.sh ~/anaconda3/envs/countries_of_interest_service/etc/conda/activate.d/`
    3. update the secret environment variables, you can get the credentials from an administrator
    4. `mkdir -p ~/anaconda3/envs/countries_of_interest_service/etc/conda/deactivate.d`
    5. `cp envs/activate.d/env_vars.sh ~/anaconda3/envs/countries_of_interest_service/etc/conda/deactivate.d/`
8. run the app
    <br />`python app.py`
9. go to `http://localhost:5000`
  

## Deployment (to development environment)
The countries of interest service is nominally deployed in a Cloud Foundry instance. 

1. Install Cloud Foundry
    <br />https://docs.cloudfoundry.org/cf-cli/install-go-cli.html
2. Request a Government PaaS account from the web devops team
3. Request access to the `datahub-dev` and `data-workspace-apps-dev` spaces
4. Login to cloud foundry
    <br /> `cf login -u <username> -p <password>`
5. Set the cloud foundry target
    <br /> `cf target -o dit-staging -s data-workspace-apps-dev`
6. Push the application
    <br />`cf push`
## Testing
From the project base directory use the command,

`python -m unittest`

to run tests for a specific test module, do,

`python -m unittest tests.&lttest_module&gt`

to run tests in a specific directory do,

`python -m unittest discover -s &lttest directory&gt`

