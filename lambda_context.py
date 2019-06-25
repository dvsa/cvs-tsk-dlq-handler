from dataclasses import dataclass
from typing import Dict, Any, Optional


class LambdaCognitoIdentity:
    cognito_identity_id: str = "cognito_identity_id"
    cognito_identity_pool_id: str = "cognito_identity_pool_id"


class LambdaClientContextMobileClient:
    installation_id: str = "installation_id"
    app_title: str = "app_title"
    app_version_name: str = "app_version_name"
    app_version_code: str = "app_version_code"
    app_package_name: str = "app_package_name"


class LambdaClientContext:
    client: LambdaClientContextMobileClient
    custom: Dict[str, Any] = {"custom": "value"}
    env: Dict[str, Any] = {"env": "asdf"}


@dataclass
class LambdaContext:
    function_name: str = "function_name"
    function_version: str = "function_version"
    invoked_function_arn: str = "invoked_function_arn"
    memory_limit_in_mb: int = 15
    aws_request_id: str = "aws_request_id"
    log_group_name: str = "log_group_name"
    log_stream_name: str = "log_stream_name"
    identity: Optional[LambdaCognitoIdentity] = LambdaCognitoIdentity()
    client_context: Optional[LambdaClientContext] = LambdaClientContext()

    @staticmethod
    def get_remaining_time_in_millis() -> int:
        return 0
