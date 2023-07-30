from google.oauth2 import service_account


class Credentials:
    __service_account_key_path = 'pubsub-svc.json'
    @classmethod
    def get_credentails(cls):
        if not hasattr(cls, 'credentials'):
            cls.credentials = service_account.Credentials.from_service_account_file(cls.__service_account_key_path)
        return getattr(cls, 'credentials')