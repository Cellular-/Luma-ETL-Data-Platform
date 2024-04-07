from . import json, time, r, logging, os
from . import CONFIG, setup_logging, InvalidRefreshTokenError, AccountNotAuthorised
import threading

setup_logging()
class OAuthResources:
    """
    Represents the json credentials generated from Infro and provides dot
    operator access to those credentials/Oauth resources.
    """
    def __init__(self, creds_filepath: str):
        self.resources = {
            "ti": None,
            "cn": None,
            "dt": None,
            "ci": None,
            "cs": None,
            "iu": None,
            "pu": None,
            "oa": None,
            "ot": None,
            "or": None,
            "ev": None,
            "v": None,
            "saak": None,
            "sask": None
        }

        self._valid = False
        self.load_from_file(creds_filepath)

    @property
    def valid(self):
        return self._valid

    @valid.setter
    def valid(self, value: bool):
        self._valid = value

    def load_from_file(self, filepath: str):
        """
        Loads the json credentials then sets the instance's resources
        attribute key values to the values in the json credentials file.

        Returns the instance of the class for method chaining purposes.
        """
        try:
            with open(filepath) as creds_file:
                oauth_creds = json.load(creds_file)

                for key in self.resources.keys():
                    self.resources[key] = oauth_creds[key]

                self._valid = True
        except IOError:
            self._valid = False
        finally:
            return self
        
class OAuthPayload:
    """
    Represents an instance of an OAuth payload required to communicate with
    the datalake.
    """

    def __init__(self, oauth_resources: OAuthResources):
        self.oauth_resources = oauth_resources
        
        # Base payload required by all requests to the authorization server.
        self.credentials_base_payload = {
            'client_id': self.oauth_resources.resources['ci'],
            'client_secret': self.oauth_resources.resources['cs'],
            'username': self.oauth_resources.resources['saak'],
            'password': self.oauth_resources.resources['sask']
        }

    @property
    def credentials_base_payload(self) -> dict:
        return self.credentials_payload
    
    @credentials_base_payload.setter
    def credentials_base_payload(self, value: dict):
        self.credentials_payload = value        

    @property
    def refresh_token_payload_addon(self) -> dict:
        return {'grant_type': 'refresh_token', 'refresh_token': None}

    @property
    def new_token_payload_addon(self) -> dict:
        return {'grant_type': 'password'}

    def refresh_token_payload(self, refresh_token: str) -> dict:
        """
        Payload required to refresh an existing token instead of generating a new one.
        """

        r = self.refresh_token_payload_addon
        r.update({'refresh_token': refresh_token})
        return {**self.credentials_base_payload, **r}

    @property
    def new_token_payload(self):
        """
        Payload required to generate new token.
        """

        return {**self.credentials_base_payload, **self.new_token_payload_addon}        

class OAuthEndpoints:
    """
    Object to provide dot operator access to oauth endpoints.
    """

    _BASE = 'https://mingle35-sso.inforgov.com:443/{tenant_name}/as/%s.oauth2'

    def __init__(self, tenant_name):
        self._BASE = self._BASE.format(tenant_name=tenant_name)
        self.ACCESS_TOKEN = self._BASE % ('token')
        self.REVOKE_TOKEN = self._BASE % ('revoke_token')

    def __getattr__(self, endpoint):
        return self._BASE % endpoint.lower()        

class OAuthRequest:
    """
    Represents a fully formed OAuth request required to query the datalake.
    """

    token_lock = threading.Lock()
    def __init__(self, oauth_payloads: OAuthPayload, oauth_endpoints: OAuthEndpoints):
        self.oauth_payloads = oauth_payloads
        self.oauth_endpoints = oauth_endpoints
        self._oauth_token = self.OAuthToken('','','','','')

        self._load_token_data() or self.new_access_token()

    @property
    def oauth_token(self):
        """
        Refreshes token if expired each time the token is retrieved.
        """
        if self._oauth_token.expires_at <= int(time.time()):
            self.refresh_access_token()
        return self._oauth_token

    @oauth_token.setter
    def oauth_token(self, oauth_token):
        """
        If the token's expiration isn't set, then generate
        expiration time then save the token data.
        """
        if not oauth_token.expires_at:
            token_created_time = time.time()
            expiration_time = int(token_created_time) + int(oauth_token.expires_in * .9)
            oauth_token.expires_at = expiration_time
        self._oauth_token = oauth_token
        self._save_token_data()

    def _load_token_data(self):
        if self.is_file_empty(CONFIG.get('filename_templates', 'datalake_tokens')):
            return False
        
        with open(CONFIG.get('filename_templates', 'datalake_tokens'), 'r') as file:
            token_data = self.OAuthToken(**json.loads(file.readline()))
            self._oauth_token = token_data
            return True
    
    def _get_auth_token(self, auth_server_url: str, token_req_payload: dict, type: str):
        """
        Attempts to get an OAuth token from the OAuth authorization server.

        auth_server_url   -- url of auth server
        token_req_payload -- required payload to communicate with auth server
        type              -- used to specify refresh or get new auth token. options: (refresh | new)
        """
        auth_token = r.post(auth_server_url,
            data=token_req_payload
        )
        
        json_response = json.loads(auth_token.content.decode('utf-8'))
        if auth_token.status_code == 200:
            if type == 'refresh':
                self.oauth_token = self.OAuthToken(**{**json_response, 'refresh_token': self._oauth_token.refresh_token})
            elif type == 'new':
                self.oauth_token = self.OAuthToken(**json_response)
        else:
            if 'expired refresh token' in auth_token.text:
                raise InvalidRefreshTokenError(json_response['error_description'])
            elif 'service temporarily unavailable' in auth_token.text:
                raise Exception('Service temporarily unavailable.')
            elif 'Account not authorised' in auth_token.text:
                raise AccountNotAuthorised('Invalid request: Account not authorised.')
            elif 'invalid_request: Invalid refresh_token' in auth_token.text:          
                raise InvalidRefreshTokenError(auth_token.text)              

        self._save_token_data()

    def refresh_access_token(self):
        try:
            self._get_auth_token(
                self.oauth_endpoints.ACCESS_TOKEN, 
                self.oauth_payloads.refresh_token_payload(self._oauth_token.refresh_token),
                'refresh'
            )
            logging.info('Refreshing access token')
        except InvalidRefreshTokenError as e:
            logging.error(f'Problem refreshing access token: {e}')
            self.new_access_token()

    def new_access_token(self):
        logging.info('Generating new access token')
        self._get_auth_token(
            self.oauth_endpoints.ACCESS_TOKEN, 
            self.oauth_payloads.new_token_payload,
            'new'
        )

    def _save_token_data(self):
        with self.token_lock:
            with open(CONFIG.get('filename_templates', 'datalake_tokens'), 'w') as file:
                json.dump(self._oauth_token.__dict__, file)

    def is_file_empty(self, file_path: str) -> bool:
        """
        Check if file is empty by confirming if its size is 0 bytes
        """

        return os.path.exists(file_path) and os.stat(file_path).st_size == 0

    class OAuthToken:
        def __init__(self, access_token='', refresh_token='', token_type='', expires_in='', expires_at=''):
            self.access_token = access_token
            self.refresh_token = refresh_token
            self.token_type = token_type
            self.expires_in = expires_in
            self.expires_at = expires_at

        def __str__(self):
            return f'OAuth refresh token expires at: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.expires_at))}'
        
def new_oauth_object(config=None, tenant_name=None):
    config = config or CONFIG
    oauth_endpoints = OAuthEndpoints(tenant_name=tenant_name or config.get('env_vars', 'active_tenant'))
    oauth_resources = OAuthResources(config.get('filename_templates', 'oauth_credentials'))
    oauth_payloads = OAuthPayload(oauth_resources)
    oauth_request = OAuthRequest(oauth_payloads, oauth_endpoints)

    return oauth_request