class InvalidRefreshTokenError(Exception):
    """
    OAuth invalid refresh token exception.
    """
    def __init__(self, message='Refresh token is invalid.'):
        self.message = message

class AccountNotAuthorised(Exception):
    """
    Account not authorized to access OAuth authorization server exception.
    """
    def __init__(self, message='Account is not authorised to access OAuth authorization server. Check application credentials.'):
        self.message = message  