# Proyecto dbt — Transformación y Modelado de Datos

Este proyecto implementa un flujo de transformación de datos utilizando **dbt Core**, siguiendo buenas prácticas de modelado, documentación y pruebas. El objetivo es construir un conjunto de modelos analíticos limpios, auditables y listos para consumo por parte de analistas, dashboards o procesos aguas abajo.

---

## 🚀 Objetivo del Proyecto

El propósito principal es transformar datos crudos provenientes de la capa *raw* en modelos estructurados y consistentes. El proyecto aplica:

- Limpieza y estandarización de datos
- Modelado incremental o por capas (staging → intermediate → marts)
- Documentación automática mediante `manifest.json` y `catalog.json`
- Pruebas de calidad (tests de unicidad, no nulos, relaciones, etc.)

---
