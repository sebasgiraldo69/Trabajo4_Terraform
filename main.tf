# Trabajo 4: pipeline de telemetria vehicular en AWS.
# Flujo: productor propuesto -> S3 -> SNS -> SQS -> procesador propuesto
#        -> Redshift Data API -> tabla y vista analitica.
# Se modela la identidad del procesador; no se implementa una Lambda ficticia.

terraform {
  required_version = ">= 1.11.0, < 2.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = var.project_name
      ManagedBy = "Terraform"
      Purpose   = "Trabajo4-BigData"
    }
  }
}

variable "aws_region" {
  description = "Region compartida por el bucket, SNS, SQS y Redshift."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefijo de recursos, de 3 a 20 caracteres: minusculas, numeros y guiones."
  type        = string
  default     = "flota-telemetria"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,18}[a-z0-9]$", var.project_name))
    error_message = "Usa de 3 a 20 caracteres, empieza por letra y termina en letra o numero."
  }
}

data "aws_caller_identity" "current" {}

data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "zone-type"
    values = ["availability-zone"]
  }

  lifecycle {
    postcondition {
      condition     = length(self.names) >= 3
      error_message = "Este diseno necesita tres zonas de disponibilidad en la region elegida."
    }
  }
}

locals {
  raw_prefix       = "raw/flota/"
  database_name    = "flota"
  processor_name   = "${var.project_name}-procesador"
  processor_dbuser = "IAMR:${local.processor_name}"
}

# 1. ALMACENAMIENTO: eventos originales privados e independientes del warehouse.
# bucket_prefix agrega un sufijo unico sin otro proveedor ni un nombre global fijo.
resource "aws_s3_bucket" "lake" {
  bucket_prefix = "${var.project_name}-raw-"
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket = aws_s3_bucket.lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Deniega transporte sin TLS. El comodin es una DENEGACION, no acceso publico.
resource "aws_s3_bucket_policy" "lake" {
  bucket = aws_s3_bucket.lake.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# Politica del futuro productor: se adjuntara a su identidad al implementarlo.
# El productor no necesita publicar a SNS: S3 genera la notificacion.
resource "aws_iam_policy" "producer" {
  name        = "${var.project_name}-productor"
  description = "Escribir exclusivamente eventos originales de la flota."
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:PutObject"]
      Resource = "${aws_s3_bucket.lake.arn}/${local.raw_prefix}*"
    }]
  })
}

# 2. INGESTA: SNS estandar + suscripcion SQS. S3 no soporta destino SNS FIFO.
resource "aws_sns_topic" "ingestion" {
  name = "${var.project_name}-objetos"
}

resource "aws_sns_topic_policy" "from_s3" {
  arn = aws_sns_topic.ingestion.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowOnlyLakeNotifications"
      Effect    = "Allow"
      Principal = { Service = "s3.amazonaws.com" }
      Action    = "sns:Publish"
      Resource  = aws_sns_topic.ingestion.arn
      Condition = {
        ArnEquals    = { "aws:SourceArn" = aws_s3_bucket.lake.arn }
        StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
      }
    }]
  })
}

resource "aws_sqs_queue" "dead_letter" {
  name                      = "${var.project_name}-errores"
  message_retention_seconds = 1209600 # 14 dias.
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "ingestion" {
  name                       = "${var.project_name}-procesamiento"
  message_retention_seconds  = 345600 # 4 dias.
  receive_wait_time_seconds  = 20
  visibility_timeout_seconds = 360 # Referencia: futura Lambda de hasta 60 s.
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "dead_letter" {
  queue_url = aws_sqs_queue.dead_letter.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.ingestion.arn]
  })
}

resource "aws_sqs_queue_policy" "from_sns" {
  queue_url = aws_sqs_queue.ingestion.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowOnlyIngestionTopic"
      Effect    = "Allow"
      Principal = { Service = "sns.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.ingestion.arn
      Condition = {
        ArnEquals    = { "aws:SourceArn" = aws_sns_topic.ingestion.arn }
        StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
      }
    }]
  })
}

resource "aws_sns_topic_subscription" "ingestion" {
  topic_arn            = aws_sns_topic.ingestion.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.ingestion.arn
  raw_message_delivery = true

  depends_on = [aws_sqs_queue_policy.from_sns]
}

resource "aws_s3_bucket_notification" "ingestion" {
  bucket = aws_s3_bucket.lake.id

  topic {
    topic_arn     = aws_sns_topic.ingestion.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = local.raw_prefix
    filter_suffix = ".json"
  }

  # S3 comprueba el destino al crearlo. La suscripcion debe estar lista tambien.
  depends_on = [
    aws_sns_topic_policy.from_s3,
    aws_sns_topic_subscription.ingestion,
    aws_s3_bucket_public_access_block.lake,
    aws_s3_bucket_server_side_encryption_configuration.lake,
    aws_s3_bucket_versioning.lake,
  ]
}

# 3. RED DEL WAREHOUSE: tres subredes privadas, sin Internet Gateway ni NAT.
# Data API es una API administrada; no necesita abrir el puerto SQL del workgroup.
resource "aws_vpc" "warehouse" {
  cidr_block           = "10.40.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${var.project_name}-warehouse" }
}

resource "aws_subnet" "warehouse" {
  count = 3

  vpc_id                  = aws_vpc.warehouse.id
  cidr_block              = cidrsubnet(aws_vpc.warehouse.cidr_block, 8, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = false

  tags = { Name = "${var.project_name}-privada-${count.index + 1}" }
}

resource "aws_security_group" "warehouse" {
  name        = "${var.project_name}-warehouse"
  description = "Sin acceso SQL entrante; las consultas usan Redshift Data API."
  vpc_id      = aws_vpc.warehouse.id

  # Sin reglas de ingreso ni egreso. No se realizan COPY/UNLOAD a S3 en esta base.
}

# 4. CONSUMO: Redshift Serverless, con credenciales administrativas gestionadas.
# El secreto se usa SOLO para inicializar el esquema; no se entrega al procesador.
resource "aws_redshiftserverless_namespace" "warehouse" {
  namespace_name        = var.project_name
  db_name               = local.database_name
  admin_username        = "flota_admin"
  manage_admin_password = true
}

resource "aws_redshiftserverless_workgroup" "warehouse" {
  workgroup_name      = var.project_name
  namespace_name      = aws_redshiftserverless_namespace.warehouse.namespace_name
  base_capacity       = 8
  max_capacity        = 8
  publicly_accessible = false
  subnet_ids          = aws_subnet.warehouse[*].id
  security_group_ids  = [aws_security_group.warehouse.id]

  config_parameter {
    parameter_key   = "require_ssl"
    parameter_value = "true"
  }
}

# 5. PROCESAMIENTO: identidad del futuro job Lambda (minimo del enunciado).
resource "aws_iam_role" "processor" {
  name = local.processor_name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "processor" {
  name = "accesos-pipeline"
  role = aws_iam_role.processor.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ConsumeOnlyIngestionQueue"
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        Resource = aws_sqs_queue.ingestion.arn
      },
      {
        Sid      = "ReadOnlyRawEvents"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.lake.arn}/${local.raw_prefix}*"
      },
      {
        Sid      = "ExecuteOnlyOnProjectWarehouse"
        Effect   = "Allow"
        Action   = ["redshift-data:ExecuteStatement", "redshift-serverless:GetCredentials"]
        Resource = aws_redshiftserverless_workgroup.warehouse.arn
      },
      {
        Sid      = "CheckOnlyOwnStatements"
        Effect   = "Allow"
        Action   = ["redshift-data:DescribeStatement"]
        Resource = "*" # La accion no admite un ARN individual de sentencia.
        Condition = {
          StringEquals = { "redshift-data:statement-owner-iam-userid" = "$${aws:userid}" }
        }
      },
    ]
  })
}

# 6. ESQUEMA SQL: estos recursos ejecutarian SQL al aplicar, no al validar.
# Se usa el secreto administrativo gestionado por Redshift y referencias explicitas
# para ordenar la creacion. El rol de Terraform necesita acceso a ese secreto.
resource "aws_redshiftdata_statement" "schema" {
  workgroup_name = aws_redshiftserverless_workgroup.warehouse.workgroup_name
  database       = local.database_name
  secret_arn     = aws_redshiftserverless_namespace.warehouse.admin_password_secret_arn
  sql            = "CREATE SCHEMA IF NOT EXISTS flota;"
}

resource "aws_redshiftdata_statement" "table" {
  workgroup_name = aws_redshiftserverless_workgroup.warehouse.workgroup_name
  database       = local.database_name
  secret_arn     = aws_redshiftserverless_namespace.warehouse.admin_password_secret_arn
  sql            = <<-SQL
    CREATE TABLE IF NOT EXISTS flota.eventos_telemetria (
      id_evento      VARCHAR(36)   NOT NULL,
      id_vehiculo    VARCHAR(16)   NOT NULL,
      fecha_evento   TIMESTAMPTZ   NOT NULL,
      fecha_ingesta  TIMESTAMPTZ   NOT NULL,
      velocidad      DECIMAL(6,2)  NOT NULL,
      combustible    DECIMAL(5,2)  NOT NULL,
      lat            DECIMAL(9,6)  NOT NULL,
      lon            DECIMAL(9,6)  NOT NULL
    )
    DISTSTYLE AUTO
    SORTKEY (fecha_evento, id_vehiculo);
  SQL

  depends_on = [aws_redshiftdata_statement.schema]
}

# Redshift no impone unicidad. La vista ofrece un evento logico por UUID.
resource "aws_redshiftdata_statement" "view" {
  workgroup_name = aws_redshiftserverless_workgroup.warehouse.workgroup_name
  database       = local.database_name
  secret_arn     = aws_redshiftserverless_namespace.warehouse.admin_password_secret_arn
  sql            = <<-SQL
    CREATE OR REPLACE VIEW flota.telemetria AS
    SELECT id_evento, id_vehiculo, fecha_evento, fecha_ingesta,
           velocidad, combustible, lat, lon
    FROM (
      SELECT *, ROW_NUMBER() OVER (
        PARTITION BY id_evento ORDER BY fecha_ingesta ASC
      ) AS numero_fila
      FROM flota.eventos_telemetria
    ) eventos
    WHERE numero_fila = 1;
  SQL

  depends_on = [aws_redshiftdata_statement.table]
}

# IAMR:<nombre del rol> es la identidad SQL para autenticacion temporal con el rol.
# PASSWORD DISABLE evita crear otra credencial para el procesador.
resource "aws_redshiftdata_statement" "processor_user" {
  workgroup_name = aws_redshiftserverless_workgroup.warehouse.workgroup_name
  database       = local.database_name
  secret_arn     = aws_redshiftserverless_namespace.warehouse.admin_password_secret_arn
  sql            = "CREATE USER \"${local.processor_dbuser}\" PASSWORD DISABLE;"

  depends_on = [aws_iam_role.processor]
}

# Un ExecuteStatement por sentencia: no se presupone soporte de scripts multiples.
# Se quitan permisos heredados de PUBLIC en la base dedicada del proyecto.
resource "aws_redshiftdata_statement" "sql_permissions" {
  for_each = {
    schema_usage  = "GRANT USAGE ON SCHEMA flota TO \"${local.processor_dbuser}\";"
    table_insert  = "GRANT INSERT ON TABLE flota.eventos_telemetria TO \"${local.processor_dbuser}\";"
    no_public_ddl = "REVOKE CREATE ON SCHEMA public FROM PUBLIC;"
    no_temp       = "REVOKE TEMPORARY ON DATABASE ${local.database_name} FROM PUBLIC;"
  }

  workgroup_name = aws_redshiftserverless_workgroup.warehouse.workgroup_name
  database       = local.database_name
  secret_arn     = aws_redshiftserverless_namespace.warehouse.admin_password_secret_arn
  sql            = each.value

  depends_on = [
    aws_redshiftdata_statement.table,
    aws_redshiftdata_statement.processor_user,
  ]
}

output "lake_bucket_name" {
  description = "Destino de la futura adaptacion del productor."
  value       = aws_s3_bucket.lake.id
}

output "ingestion_queue_url" {
  description = "Cola que consumiria el procesador."
  value       = aws_sqs_queue.ingestion.url
}

output "processor_role_arn" {
  description = "Rol del futuro procesador; no crea una funcion Lambda."
  value       = aws_iam_role.processor.arn
}

output "producer_policy_arn" {
  description = "Politica a adjuntar a la identidad del productor al implementarlo."
  value       = aws_iam_policy.producer.arn
}

output "warehouse_workgroup_name" {
  description = "Workgroup para llamadas a Redshift Data API."
  value       = aws_redshiftserverless_workgroup.warehouse.workgroup_name
}

output "analytics_relation" {
  description = "Vista de consulta deduplicada dentro de la base flota."
  value       = "flota.telemetria"
}
