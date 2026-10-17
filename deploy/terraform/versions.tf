# Exact versions, so `terraform init` resolves the same providers everywhere and
# .terraform.lock.hcl records their hashes. See docs/adr/0028-deployment-shape-on-aws.md
# for the release pages each pin was checked against.
terraform {
  required_version = "1.16.4"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.66.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "3.9.1"
    }
  }

  # Partial configuration: the bucket, key, region and use_lockfile = true (S3-native
  # locking) are passed with -backend-config at init, from an encrypted bucket the
  # operator owns. RUNBOOK.md requires it before any apply, because the state holds the
  # database password. CI runs `terraform init -backend=false`, which skips it.
  backend "s3" {}
}
