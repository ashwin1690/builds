"""
Configuration management for Salesforce and Atlan connections.

This module uses pydantic-settings to manage configuration from environment
variables with validation and type safety.
"""

from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    Create a .env file in the project root with the following variables:

    # Salesforce OAuth Configuration
    SALESFORCE_USERNAME=your@email.com
    SALESFORCE_PASSWORD=yourpassword
    SALESFORCE_SECURITY_TOKEN=yoursecuritytoken
    SALESFORCE_DOMAIN=login  # or 'test' for sandbox

    # OR Salesforce JWT Configuration
    SALESFORCE_CONSUMER_KEY=3MVG9...
    SALESFORCE_PRIVATE_KEY_PATH=/path/to/server.key
    SALESFORCE_USERNAME=your@email.com

    # Atlan Configuration
    ATLAN_BASE_URL=https://your-tenant.atlan.com
    ATLAN_API_KEY=your-api-key

    # Extraction Configuration
    BULK_API_BATCH_SIZE=10000
    MAX_CONCURRENT_REQUESTS=5
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Salesforce OAuth Configuration
    salesforce_username: Optional[str] = Field(
        None, description="Salesforce username for authentication"
    )
    salesforce_password: Optional[str] = Field(
        None, description="Salesforce password (omit if using JWT)"
    )
    salesforce_security_token: Optional[str] = Field(
        None, description="Salesforce security token (omit if using JWT)"
    )
    salesforce_domain: str = Field(
        default="login",
        description="Salesforce domain (login for production, test for sandbox)",
    )

    # Salesforce JWT Configuration (alternative to password)
    salesforce_consumer_key: Optional[str] = Field(
        None, description="Connected app consumer key for JWT auth"
    )
    salesforce_private_key_path: Optional[str] = Field(
        None, description="Path to private key file for JWT auth"
    )

    # Salesforce API Configuration
    salesforce_api_version: str = Field(
        default="v59.0", description="Salesforce API version to use"
    )

    # Atlan Configuration
    atlan_base_url: str = Field(..., description="Atlan instance base URL")
    atlan_api_key: str = Field(..., description="Atlan API key for authentication")

    # Extraction Configuration
    bulk_api_batch_size: int = Field(
        default=10000,
        description="Batch size for Bulk API 2.0 queries",
        ge=1,
        le=250000,
    )

    max_concurrent_requests: int = Field(
        default=5,
        description="Maximum concurrent API requests",
        ge=1,
        le=25,
    )

    incremental_lookback_days: int = Field(
        default=7,
        description="Days to look back for incremental extraction",
        ge=1,
    )

    setup_audit_retention_days: int = Field(
        default=180,
        description="Days to retain Setup Audit Trail data",
        ge=1,
        le=180,
    )

    # Output Configuration
    output_format: str = Field(
        default="jsonl", description="Output format for events (jsonl, parquet, csv)"
    )

    output_directory: str = Field(
        default="./output", description="Directory for output files"
    )

    # Logging Configuration
    log_level: str = Field(default="INFO", description="Logging level")

    @field_validator("salesforce_domain")
    @classmethod
    def validate_domain(cls, v: str) -> str:
        """Ensure domain is valid."""
        if v not in ["login", "test"]:
            raise ValueError("salesforce_domain must be 'login' or 'test'")
        return v

    @field_validator("output_format")
    @classmethod
    def validate_output_format(cls, v: str) -> str:
        """Ensure output format is supported."""
        if v not in ["jsonl", "parquet", "csv"]:
            raise ValueError("output_format must be 'jsonl', 'parquet', or 'csv'")
        return v

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Ensure log level is valid."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v_upper

    def get_salesforce_auth_config(self) -> dict:
        """Get Salesforce authentication configuration."""
        if self.salesforce_consumer_key and self.salesforce_private_key_path:
            # JWT authentication
            return {
                "username": self.salesforce_username,
                "consumer_key": self.salesforce_consumer_key,
                "privatekey_file": self.salesforce_private_key_path,
                "domain": self.salesforce_domain,
            }
        elif self.salesforce_username and self.salesforce_password:
            # Username/password authentication
            return {
                "username": self.salesforce_username,
                "password": self.salesforce_password,
                "security_token": self.salesforce_security_token or "",
                "domain": self.salesforce_domain,
            }
        else:
            raise ValueError(
                "Must provide either (username + password) or "
                "(username + consumer_key + private_key_path) for Salesforce auth"
            )

    def get_atlan_config(self) -> dict:
        """Get Atlan configuration."""
        return {
            "base_url": self.atlan_base_url,
            "api_key": self.atlan_api_key,
        }


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """
    Get or create the global settings instance.

    Returns:
        Settings: The application settings
    """
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
