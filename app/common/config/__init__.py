import collections
import os
import re
from pathlib import Path

import yaml
from dotenv import load_dotenv


class Config:
    def __init__(self, directory):
        self.directory = directory
        if bool(int(os.environ.get('USE_DOTENV', '0'))):
            # Load environment variables from .env file, this does not
            # override existing System environment variables
            env_path = Path('.') / '.env'
            load_dotenv(dotenv_path=env_path)
        default = self._get_config_from_file('defaults.yml')
        self._parse_env_vars(default)
        local = self._get_local_config()
        self._parse_env_vars(local)
        self.store = self._update_dict(default, local)

    def __getitem__(self, key):
        return self.store[key]

    def all(self):
        return self.store

    def _get_config_from_file(self, file_name, check_if_exists=False):
        config_file = Path(os.path.join(self.directory, file_name))
        if config_file.is_file() or not check_if_exists:
            with open(os.path.join(self.directory, file_name)) as ymlfile:
                return yaml.safe_load(ymlfile)
        return {}

    def _get_local_config(self):
        is_testing = bool(int(os.environ.get('TESTING', '0')))
        if is_testing:
            file_name = 'local_testing.yml'
        else:
            file_name = 'local.yml'
        return self._get_config_from_file(file_name, check_if_exists=True)

    def _update_dict(self, dest, source):
        for k, v in source.items():
            if isinstance(v, collections.abc.Mapping):
                sub_dict = self._update_dict(dest.get(k, {}), v)
                dest[k] = sub_dict
            else:
                dest[k] = source[k]
        return dest

    def _parse_env_vars(self, config):
        for k, v in config.items():
            if isinstance(v, collections.abc.Mapping):
                self._parse_env_vars(v)
            if isinstance(v, list):
                for item in v:
                    self._parse_env_vars(item)
            else:
                pattern = "\\$ENV\\{(.*), (.*)\\}"
                search = re.search(pattern, str(config[k]))
                if search:
                    env_var = search.group(1)
                    default = search.group(2)
                    value = os.getenv(env_var, default)
                    if value.isdigit():
                        value = int(value)
                    elif value in ['True', 'False']:
                        value = value == 'True'
                    config[k] = value
        return config
