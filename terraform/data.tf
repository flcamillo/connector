# identifica a conta que esta sendo utilizada
data "aws_caller_identity" "current" {}

# identifica a região que esta sendo utilizada
data "aws_region" "current" {}

locals {

  # le o conteúdo da chave privada
  ssh_private_key_connector = file("ssh_key_connector.txt")
  ssh_private_key_server    = file("ssh_key_server.txt")

  # le o conteúdo da chave publica
  ssh_public_key_connector = file("ssh_pub_connector.txt")
  ssh_public_key_server    = file("ssh_pub_server.txt")

  # le o conteúdo da chave publica autorizadas
  ssh_trusted_keys = file("trusted_host_keys.txt")

  # monta o segredo para o secret manager que conterá os dados do usuário do conector
  connector_secret_value = {
    Username   = "${var.connector_user}",
    PrivateKey = "${local.ssh_private_key_connector}"
    Password   = "${var.connector_password}"
  }

  # monta o segredo para o secret manager que conterá os dados do usuário do servidor
  server_secret_value = {
    Username = "${var.connector_user}",
    PublicKey = [
      "${local.ssh_public_key_connector}"
    ]
    Password = "${var.connector_password}"
  }

}
