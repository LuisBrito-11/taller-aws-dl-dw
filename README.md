# Arquitecturas de Datos en AWS: Data Lake y Data Warehouse

Taller del curso Arquitecturas de Datos 2026-2 (Ingeniería de Datos, Universidad EIA).

**Integrantes:** Luis Miguel Brito Rincón y Gabriel Alejandro Giraldo Ayerbe.

El informe completo, con la explicación de cada decisión y todas las evidencias, está en [Informe_Taller_DL_DW_AWS.docx](Informe_Taller_DL_DW_AWS.docx).

| Carpeta | Proyecto | Servicios |
|---|---|---|
| [datalake/](datalake/) | Pipeline medallón (Bronze, Silver, Gold) con noticias RSS de El Tiempo | S3, Amazon Comprehend, AWS Glue Data Catalog, Amazon Athena, Airflow en EC2 |
| [datawarehouse/](datawarehouse/) | Migración del proyecto local DE-DW a AWS | Amazon RDS for MySQL, Amazon DocumentDB, S3, Amazon Redshift Serverless, dbt y Airflow en EC2 |

Cada carpeta tiene su propio README con la arquitectura, las decisiones técnicas, los escenarios de calidad y recuperación, los costos y las instrucciones para desplegar, ejecutar y eliminar los recursos. Las evidencias (JSON, logs de Airflow y capturas) están en `docs/` dentro de cada proyecto.

La configuración no incluye secretos: los archivos `.env.example` listan las variables necesarias y las contraseñas reales se generan durante el despliegue. Amazon QuickSight quedó por fuera del taller por indicación del profesor.
