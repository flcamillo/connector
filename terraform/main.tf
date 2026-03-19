# define a versão do terraform e os providers usados
terraform {
  required_providers {
    aws = {
       source = "hashicorp/aws" 
       version = "~> 6.33.0"
     }
  }
  required_version = ">= 1.14.5"
}

# configura o provider da aws
provider "aws" {
  region = "sa-east-1"
  profile = "terraform_user"
}