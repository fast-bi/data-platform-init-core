from apiflask import HTTPTokenAuth
from flask import request, jsonify
from config import Config  # make sure to import Config correctly based on your project structure

auth = HTTPTokenAuth(scheme='ApiKey', header='X-API-KEY', security_scheme_name='ApiKeyAuth')

def verify_token(token):
    # Check for API key in the header
    token = request.headers.get('X-API-KEY')

    if token is None:
        # If not found in the header, check for API key in query parameters
        token = request.args.get('X-API-KEY')

    # Check if the token matches the expected API key
    is_valid = token == Config.API_KEY
    
    return is_valid

auth.verify_token(verify_token)
