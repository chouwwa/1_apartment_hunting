
###
terraform {
    backend "s3" {
        bucket = "data"                  # Name of the S3 bucket
        endpoint = "http://localhost:9000" # Minio endpoint
        key = "apartment_hunting.tfstate"        # Name of the tfstate file

        access_key="admin"           # Access and secret keys
        secret_key="password"

        region = "main"                     # Region validation will be skipped
        skip_credentials_validation = true  # Skip AWS related checks and validations
        skip_metadata_api_check = true
        skip_region_validation = true
        force_path_style = true             # Enable path-style S3 URLs (https://<HOST>/<BUCKET> https://www.terraform.io/language/settings/backends/s3#force_path_style
    }
}
###