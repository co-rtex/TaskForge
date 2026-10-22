provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "taskforge"
      ManagedBy = "terraform"
    }
  }
}
