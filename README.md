<div align="center">

# Pipeline de Telemetría Vehicular en AWS

**Arquitectura de datos como Código: ingesta, almacenamiento, procesamiento y consumo, modelados y desplegados con Terraform**

🎓 Proyecto académico — Big Data, Universidad EIA (2026-2)

[![Terraform](https://img.shields.io/badge/Terraform-1.14-844FBA?logo=terraform&logoColor=white)](https://www.terraform.io/)
[![AWS](https://img.shields.io/badge/AWS-Provider%20v6-FF9900?logo=amazonaws&logoColor=white)](https://registry.terraform.io/providers/hashicorp/aws/latest)
[![HCL](https://img.shields.io/badge/HCL-Infraestructura%20como%20Código-5C4EE5)](https://www.terraform.io/language)
[![Redshift Serverless](https://img.shields.io/badge/Amazon-Redshift%20Serverless-8C4FFF?logo=amazonredshift&logoColor=white)](https://aws.amazon.com/redshift/redshift-serverless/)
[![License](https://img.shields.io/badge/Licencia-MIT-lightgrey)](#licencia)

</div>

---

## Tabla de contenido

- [Descripción general](#descripción-general)
- [Arquitectura del pipeline](#arquitectura-del-pipeline)
- [Capas modeladas](#capas-modeladas)
- [Permisos y mínimo privilegio](#permisos-y-mínimo-privilegio)
- [Decisión de diseño: alternativa descartada](#decisión-de-diseño-alternativa-descartada)
- [Escalabilidad ante 100x más datos](#escalabilidad-ante-100x-más-datos)
- [Despliegue real (evidencia)](#despliegue-real-evidencia)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Tecnologías utilizadas](#tecnologías-utilizadas)
- [Instalación y uso](#instalación-y-uso)
- [Contraste con la plantilla de referencia (GCP)](#contraste-con-la-plantilla-de-referencia-gcp)
- [Aprendizajes clave](#aprendizajes-clave)
- [Contexto académico](#contexto-académico)
- [Licencia](#licencia)
- [Autores](#autores)

---

## Descripción general

Diseño de un pipeline de datos en la nube como **Infraestructura como Código (IaC)** con Terraform, modelando las cuatro capas de un pipeline moderno — ingesta, almacenamiento (data lake), procesamiento y consumo (warehouse) — sobre **Amazon Web Services**, más los permisos de mínimo privilegio que las conectan.

El caso de uso es la telemetría de una flota simulada de vehículos (velocidad, combustible, ubicación). El flujo separa los datos originales, la notificación de llegada, el procesamiento y la consulta histórica: un productor guarda cada evento en S3, S3 notifica por SNS, SNS entrega a una cola SQS, un procesador consume la referencia y la inserta en Redshift Serverless, y el analista consulta una vista deduplicada.

A diferencia de los talleres anteriores (MapReduce y Spark, procesamiento de datos ya almacenados), este trabajo no procesa datos: **diseña y sustenta la infraestructura en la que un pipeline real correría**, con énfasis explícito en IAM y mínimo privilegio.

## Arquitectura del pipeline

```mermaid
flowchart LR
    A[Productor de flota<br/>adaptación propuesta] -.->|PutObject| B[(Amazon S3<br/>lake: eventos originales)]
    B -->|ObjectCreated| C[Amazon SNS<br/>tópico de notificaciones]
    C --> D[Amazon SQS<br/>suscripción al tópico]
    D -.->|ReceiveMessage| E[Procesador propuesto<br/>rol IAM para Lambda]
    B -.->|GetObject: lee el JSON| E
    E -.->|ExecuteStatement| F[(Redshift Serverless<br/>Data API)]
    F --> G[Tabla flota.eventos_telemetria]
    G --> H[Vista flota.telemetria<br/>1 evento por UUID]
    D --> I[(SQS de errores<br/>tras reintentos fallidos)]
```

Los elementos discontinuos (productor y procesador) representan **identidad y permisos modelados**, no ejecución desplegada: Terraform crea el rol IAM y la política del procesador, pero no una función Lambda ficticia ni un productor real — ambos quedan como adaptación futura, según el alcance del taller.

## Capas modeladas

| Capa | Servicio AWS | Papel en el pipeline |
|---|---|---|
| **Ingesta** | Amazon SNS (tópico estándar) + Amazon SQS (suscripción) | Publica avisos cuando S3 crea un objeto; desacopla al productor de los consumidores y amortigua picos |
| **Almacenamiento** | Amazon S3 (`bucket_prefix`, privado, cifrado, versionado) | Data lake de eventos originales en JSON; permite reprocesar sin depender de las tablas analíticas |
| **Procesamiento** | Rol IAM (`aws_iam_role` + `aws_iam_role_policy`) | Identidad del futuro job (Lambda) que leería SQS y S3 y cargaría Redshift, con el mínimo acceso necesario |
| **Consumo** | Amazon Redshift Serverless (namespace + workgroup) | Warehouse con tabla `flota.eventos_telemetria` (esquema definido) y vista `flota.telemetria` deduplicada |
| **Red** | VPC + 3 subredes privadas + Security Group sin reglas | Aísla el workgroup de Redshift; sin Internet Gateway ni acceso SQL entrante — las consultas van por Data API |

## Permisos y mínimo privilegio

Sección obligatoria del taller: la pista explícita es que la parte de IAM es la que separa un buen diseño de uno mediocre.

| Acceso del procesador | Permiso y alcance | Por qué se necesita |
|---|---|---|
| **Consumir la ingesta** | `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes`, solo en la cola principal | Recibir trabajo y confirmar que terminó; no crea, modifica ni elimina colas |
| **Leer el lake** | `s3:GetObject`, únicamente sobre `raw/flota/*` | La notificación ya da la clave exacta; no necesita listar el bucket ni escribir/borrar originales |
| **Enviar SQL** | `redshift-data:ExecuteStatement` + `redshift-serverless:GetCredentials`, limitado al workgroup del proyecto | Carga datos con credenciales temporales (sin contraseña de usuario propia) |
| **Verificar el resultado** | `redshift-data:DescribeStatement` con condición de propietario de la sentencia | La ejecución es asíncrona; la condición restringe la consulta a las sentencias propias del rol |
| **Escribir en la tabla** | `USAGE` en el esquema `flota` + `INSERT` en `eventos_telemetria`, para el usuario SQL `IAMR:<rol>` | Carga telemetría sin ser administrador: no tiene `SELECT`, `UPDATE`, `DELETE` ni `DROP` |

Las políticas de conexión entre servicios también están acotadas con condiciones (`SourceArn`/`SourceAccount`): S3 solo puede publicar en el tópico propio, y el tópico solo puede entregar a la cola propia, dentro de la misma cuenta.

## Decisión de diseño: alternativa descartada

**Decisión:** SNS con una suscripción SQS para la ingesta, en lugar de mantener **Kafka** mediante **Amazon MSK**, que era el protocolo del productor del trabajo anterior.

Se descartó MSK porque añadiría configuración de conectividad, autenticación y operación de Kafka que no resulta necesaria para el objetivo del taller. SNS+SQS expresa el mismo patrón de tópico/suscripción con servicios administrados, al costo de requerir adaptar el productor actual (que ya no podría usarse sin cambios) y de perder el modelo de consumo por offsets de Kafka — el reproceso se basaría en los originales de S3 en su lugar.

## Escalabilidad ante 100x más datos

El simulador emite ~1 lectura/s de cada uno de 8 vehículos (≈8 eventos/s); 100 veces ese volumen equivale a **≈800 eventos/s**, cifra estimada para discutir el diseño, no resultado de pruebas de carga.

| Pieza | Se mantiene | Habría que ajustar |
|---|---|---|
| S3 | Almacenamiento desacoplado, crece sin cambiar de modelo | Agrupar eventos / evaluar Parquet para lotes analíticos (un JSON por lectura multiplicaría objetos pequeños) |
| SNS + SQS | Desacoplamiento y absorción de picos | Revisar cuotas, antigüedad y cantidad de mensajes pendientes; una cola creciendo indica atraso |
| Procesador | — | Tamaño de lote, concurrencia y duración por ejecución; manejo de fallos parciales |
| Redshift | — | Medir tiempos de carga/consulta antes de subir capacidad; evaluar `COPY` por lotes en vez de inserciones individuales |

## Despliegue real (evidencia)

Aunque el despliegue es **opcional** según el enunciado (no se evalúa, y no desplegar no baja la nota), se desplegó la arquitectura completa en una cuenta de AWS propia para documentar el plus:

```
terraform apply
...
Apply complete! Resources: 32 added, 0 changed, 0 destroyed.

Outputs:
analytics_relation       = "flota.telemetria"
ingestion_queue_url      = "https://sqs.us-east-1.amazonaws.com/052022403573/flota-telemetria-procesamiento"
lake_bucket_name         = "flota-telemetria-raw-26d9dc7fd1280603c74c42591b"
processor_role_arn       = "arn:aws:iam::052022403573:role/flota-telemetria-procesador"
producer_policy_arn      = "arn:aws:iam::052022403573:policy/flota-telemetria-productor"
warehouse_workgroup_name = "flota-telemetria"
```

Los 32 recursos (VPC, subredes, S3, SNS, SQS, Redshift Serverless y las sentencias SQL de esquema/tabla/vista/permisos) se crearon sin errores. Los recursos se destruyeron (`terraform destroy`) inmediatamente después de tomar la evidencia, para no mantener cargos activos de Redshift Serverless.

La captura obligatoria de `terraform validate` está en [`terraform-validate.png`](./terraform-validate.png).

## Estructura del repositorio

```
.
├── main.tf                              # Arquitectura completa: 4 capas + IAM, comentada
├── terraform-validate.png               # Captura obligatoria: terraform validate exitoso
├── Informe_Arquitectura_Flota_AWS.pdf   # Informe de arquitectura (diagrama, justificación, permisos, escalabilidad)
├── Trabajo4_Terraform_Enunciado.docx    # Enunciado del taller
├── .terraform.lock.hcl                  # Lock de versiones del provider
└── .gitignore
```

## Tecnologías utilizadas

| Categoría | Herramientas |
|---|---|
| IaC | Terraform `>= 1.11.0, < 2.0` |
| Proveedor | `hashicorp/aws ~> 6.0` |
| Ingesta | Amazon SNS (estándar) + Amazon SQS (+ DLQ con redrive policy) |
| Almacenamiento | Amazon S3 (privado, versionado, cifrado SSE, deny de transporte inseguro) |
| Procesamiento | Rol IAM + política inline (identidad del futuro job) |
| Consumo | Redshift Serverless (namespace + workgroup) vía Redshift Data API |
| Red | VPC, 3 subredes privadas, Security Group sin reglas de ingreso/egreso |

## Instalación y uso

```powershell
# 1. Instalar Terraform (o usar el binario incluido en tools/)
# https://developer.hashicorp.com/terraform/install

# 2. Inicializar el proveedor
terraform init

# 3. Validar sintaxis y lógica — gratis, no crea nada (entregable obligatorio)
terraform validate

# 4. (Opcional, genera costos reales) Revisar el plan y desplegar
terraform plan
terraform apply

# 5. Si se desplegó, destruir apenas se documente la evidencia
terraform destroy
```

## Contraste con la plantilla de referencia (GCP)

El taller ofrece una plantilla de referencia sobre Google Cloud (Pub/Sub, Cloud Storage, Service Account, BigQuery). Este proyecto traduce el mismo patrón de 4 capas a sus equivalentes administrados en AWS:

| Capa | Plantilla GCP | Este proyecto (AWS) |
|---|---|---|
| Ingesta | Pub/Sub topic + subscription | SNS topic + SQS subscription |
| Almacenamiento | Cloud Storage bucket | S3 bucket |
| Procesamiento | Service Account | IAM Role |
| Consumo | BigQuery dataset + table | Redshift Serverless namespace/workgroup + tabla |

La decisión de nube (AWS) se sustenta en el informe de arquitectura; el patrón de diseño (desacoplar ingesta de procesamiento, identidad mínima para el job, warehouse con esquema propio) es el mismo que pide la plantilla, solo que expresado con los servicios administrados de AWS.

## Aprendizajes clave

- El mínimo privilegio no es solo una política de "solo lectura": significa enumerar exactamente las acciones y el alcance (`Resource`) que una identidad necesita, y nada más — incluso dentro de un mismo servicio (p. ej., `INSERT` sin `SELECT`/`UPDATE`/`DELETE`/`DROP` en Redshift).
- Las políticas que conectan dos servicios administrados (S3→SNS, SNS→SQS) también deben acotarse con condiciones (`SourceArn`/`SourceAccount`); dejar el principal abierto (`"*"`) sin condición es un error común que una condición cierra sin perder funcionalidad.
- `terraform plan` antes de `apply` es la forma de verificar, sin costo, que lo que se va a crear coincide exactamente con lo diseñado — en este proyecto el plan mostró 32 recursos, todos esperados, 0 inesperados.
- Redshift Serverless con Data API permite ejecutar SQL de administración (crear esquema, tabla, vista, usuario IAMR y permisos) directamente desde Terraform, sin abrir un puerto SQL ni gestionar una conexión JDBC.

## Contexto académico

Desarrollado como Trabajo 4 (evaluación final) de la asignatura Big Data, Universidad EIA (2026-2), prof. Alejandro Mery Agudelo, en parejas. El taller pedía diseñar y sustentar con Terraform una arquitectura de pipeline de datos en la nube (4 capas + IAM), validar la sintaxis con `terraform validate`, y entregar un informe de arquitectura en PDF — el despliegue real era opcional.

## Licencia

Este proyecto se distribuye bajo licencia [MIT](LICENSE). Los datos de telemetría son simulados, para fines académicos.

## Autores

**Sebastián Giraldo Franco** — [@sebasgiraldo69](https://github.com/sebasgiraldo69)
**Juan José Jaramillo Mora** — [@Juanjo1414](https://github.com/Juanjo1414)
