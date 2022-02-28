resource "aws_s3_bucket" "datalakehouse" {
  bucket = "datalakehouse-eurico-enem2019"
  acl = "private"


tags = {
    IES = "IGTI",
    CURSO = "EDC"
}

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }

}