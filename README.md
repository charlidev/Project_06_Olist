# Estrategia de Ambientes — Unity Catalog

## 1. Propósito

Este documento define la estrategia de ambientes y namespaces para el Project 06 — Olist.

El objetivo es establecer una separación clara entre los ambientes de desarrollo, pruebas y producción, manteniendo una arquitectura de datos consistente en todos ellos.

El diseño debe soportar:

* Aislamiento entre ambientes
* Promoción controlada de cambios desde desarrollo hasta producción
* Convenciones de nombres consistentes
* Gobierno mediante Unity Catalog
* Despliegue mediante CI/CD
* Infraestructura reproducible
* Propiedad clara de los activos de datos

---

## 2. Modelo de Ambientes

El proyecto utilizará tres ambientes:

| Ambiente | Propósito                          | Estabilidad de datos | Despliegue                |
| -------- | ---------------------------------- | -------------------- | ------------------------- |
| `dev`    | Desarrollo y experimentación       | Baja                 | Controlado por desarrollo |
| `test`   | Pruebas e integración              | Media                | CI/CD                     |
| `prod`   | Cargas productivas y consumo de BI | Alta                 | Sólo mediante CI/CD       |

Los ambientes representan el ciclo de vida del desarrollo de software y no diferentes dominios de negocio.

El mismo modelo lógico de datos existirá en los tres ambientes.

Ejemplo:

```text
dev  →  test  →  prod
```

Los cambios deben desarrollarse y validarse antes de ser promovidos a producción.

---

## 3. Estructura de Unity Catalog

El namespace principal de Unity Catalog utilizará la siguiente estructura:

```text
Metastore
│
├── olist_dev
│   ├── bronze
│   ├── silver
│   ├── gold
│   └── metadata
│
├── olist_test
│   ├── bronze
│   ├── silver
│   ├── gold
│   └── metadata
│
└── olist_prod
    ├── bronze
    ├── silver
    ├── gold
    └── metadata
```

El namespace de tres niveles será:

```text
catalog.schema.table
```

Ejemplos:

```text
olist_dev.bronze.orders
olist_dev.silver.orders
olist_dev.gold.fact_orders
```

Ejemplos para producción:

```text
olist_prod.bronze.orders
olist_prod.silver.orders
olist_prod.gold.fact_orders
```

---

## 4. ¿Por qué separar los ambientes mediante catálogos?

El aislamiento de ambientes se implementará a nivel de catálogo.

Esto proporciona un límite claro de seguridad y despliegue:

```text
olist_dev
     │
     │ desarrollo
     ▼
olist_test
     │
     │ validación
     ▼
olist_prod
```

Las cargas de desarrollo no deben modificar directamente objetos de producción.

Los objetos de producción sólo deben ser creados o modificados mediante el proceso de despliegue controlado.

Esto también mantiene consistente el namespace entre ambientes:

```text
olist_dev.gold.fact_orders
olist_test.gold.fact_orders
olist_prod.gold.fact_orders
```

La estructura y el modelo lógico de las tablas deben ser equivalentes entre ambientes, salvo que exista una excepción específica y documentada.

---

## 5. Responsabilidad de cada Schema

### 5.1 Bronze

Propósito:

Almacenar los datos provenientes de la fuente con la mínima transformación posible.

Tablas esperadas:

```text
customers
orders
order_items
order_payments
order_reviews
products
sellers
geolocation
category_translation
```

Bronze puede contener metadata técnica como:

```text
_ingestion_timestamp
_source_file
_run_id
```

Bronze no está destinado para reporting de negocio.

---

### 5.2 Silver

Propósito:

Proporcionar datasets limpios, estandarizados, validados y conformados.

Transformaciones esperadas:

```text
Conversión de tipos de datos
Normalización de strings
Normalización de fechas y timestamps
Deduplicación
Reglas de negocio
Integridad referencial
Procesamiento CDC
Procesamiento SCD Type 2
```

Las tablas Silver deben representar datasets confiables y preparados para el modelado posterior.

---

### 5.3 Gold

Propósito:

Proporcionar datasets orientados al negocio y al análisis.

Gold contendrá el modelo dimensional y las métricas de negocio.

Dimensiones esperadas:

```text
dim_date
dim_customer
dim_product
dim_seller
dim_geography
```

Tablas de hechos esperadas:

```text
fact_orders
fact_order_items
fact_payments
fact_reviews
```

Posteriormente podrían agregarse hechos relacionados con:

```text
inventory
purchases
supplier activity
```

Gold será la principal fuente de consumo para Power BI y analítica avanzada.

---

### 5.4 Metadata

Propósito:

Almacenar metadata técnica necesaria para operar la plataforma.

Ejemplos:

```text
ingestion_config
pipeline_runs
data_quality_results
schema_versions
```

Este schema soportará las capacidades de arquitectura metadata-driven y observabilidad del proyecto.

Las tablas de metadata se consideran activos de la plataforma y no datasets de negocio.

---

## 6. Estrategia de Quarantine

Los registros inválidos no deberán mezclarse con los datos confiables de Silver.

La estructura lógica será:

```text
Silver
│
├── Registros válidos
│       ↓
│      Gold
│
└── Registros inválidos
        ↓
    Quarantine
```

La implementación concreta de quarantine se definirá durante la etapa de Data Quality.

La organización física podrá ser:

```text
olist_dev.silver
olist_dev.gold
olist_dev.metadata
olist_dev.quarantine
```

Si posteriormente se requieren controles de seguridad o ciclos de vida independientes, Quarantine podrá implementarse como un schema dedicado.

En la arquitectura inicial, `quarantine` se considera una extensión opcional y no un schema obligatorio de la base.

---

## 7. Estrategia de Almacenamiento

El proyecto distinguirá entre el almacenamiento de origen y el almacenamiento administrado por el lakehouse.

### 7.1 Fuente / Landing

Los archivos CSV de Olist ya existen en ADLS Gen2:

```text
landing-zone-olist
```

Esta ubicación representa la zona de aterrizaje de los datos de origen.

Su ciclo de vida estará asociado al proceso de ingesta.

---

### 7.2 Managed Storage de Unity Catalog

Las tablas Bronze, Silver, Gold y Metadata utilizarán, siempre que sea práctico, almacenamiento administrado por Unity Catalog.

Conceptualmente:

```text
ADLS Gen2
│
├── landing-zone-olist
│      └── archivos CSV de origen
│
└── Unity Catalog Managed Storage
       ├── olist_dev
       ├── olist_test
       └── olist_prod
```

El código de la aplicación no deberá depender de rutas físicas construidas manualmente para acceder a las managed tables.

Las tablas deberán referenciarse normalmente mediante nombres de Unity Catalog:

```sql
SELECT *
FROM olist_prod.gold.fact_orders;
```

en lugar de depender de rutas físicas hardcodeadas.

---

## 8. Estrategia de External Location

La Landing Zone de Olist será accedida mediante los mecanismos de control de almacenamiento de Unity Catalog.

El patrón esperado es:

```text
ADLS Gen2
    ↓
Managed Identity / Storage Credential
    ↓
Unity Catalog External Location
    ↓
Landing Zone
```

El proyecto no deberá incluir claves de almacenamiento ni credenciales de larga duración dentro de notebooks, scripts o archivos de configuración.

Las external locations se utilizarán para aquellas rutas que deban permanecer en ubicaciones específicas del almacenamiento cloud, como la Landing Zone de Olist.

---

## 9. Aislamiento de Producción

El catálogo de producción deberá mantenerse aislado de los workloads de desarrollo y pruebas tanto como lo permita la arquitectura del workspace disponible.

Modelo objetivo:

```text
Production Workspace
        │
        ▼
   olist_prod
```

Los usuarios y workloads de desarrollo y pruebas no deberán tener acceso irrestricto de escritura sobre:

```text
olist_prod.*
```

El objetivo es evitar que un notebook, experimento o job de desarrollo pueda modificar accidentalmente los datasets de producción.

La promoción a producción ocurrirá mediante el pipeline de despliegue definido posteriormente.

---

## 10. Modelo de Permisos

El acceso seguirá el principio de mínimo privilegio.

De forma conceptual:

| Objeto              | Dev                    | Test                   | Prod        |
| ------------------- | ---------------------- | ---------------------- | ----------- |
| Bronze              | Lectura/Escritura      | Controlado             | Restringido |
| Silver              | Lectura/Escritura      | Controlado             | Restringido |
| Gold                | Lectura/Escritura      | Controlado             | Restringido |
| Metadata            | Lectura/Escritura      | Controlado             | Restringido |
| Datos de producción | Sin acceso irrestricto | Sin acceso irrestricto | Controlado  |

Los permisos concretos de Unity Catalog se implementarán durante la etapa de seguridad y gobierno.

Cuando sea posible, los permisos deberán asignarse a grupos o service principals en lugar de usuarios individuales.

---

## 11. Flujo de Desarrollo

El desarrollo seguirá conceptualmente este flujo:

```text
Developer
   │
   ▼
Git branch
   │
   ▼
Development environment
   │
   ▼
Tests / Validation
   │
   ▼
Test environment
   │
   ▼
Approval / Promotion
   │
   ▼
Production
```

No se deberá realizar desarrollo manual directamente en producción.

---

## 12. Compatibilidad con CI/CD

La arquitectura de ambientes se diseñará desde el principio para permitir despliegues automatizados.

El mismo proyecto lógico será desplegado con configuración específica por ambiente.

Ejemplo:

```text
Environment = dev
Catalog     = olist_dev
```

```text
Environment = test
Catalog     = olist_test
```

```text
Environment = prod
Catalog     = olist_prod
```

Los valores específicos del ambiente deberán ser inyectados mediante configuración de despliegue y no estar hardcodeados dentro de los notebooks.

Esto permitirá promover el mismo código fuente entre ambientes.

---

## 13. Configuración por Ambiente

La configuración específica de cada ambiente deberá mantenerse separada de la lógica de negocio.

Ejemplo conceptual:

```yaml
dev:
  catalog: olist_dev

test:
  catalog: olist_test

prod:
  catalog: olist_prod
```

El mecanismo exacto será implementado mediante la estrategia de despliegue de Databricks.

El código fuente deberá obtener dinámicamente el ambiente activo.

Ejemplo conceptual:

```python
catalog = config["catalog"]
```

en lugar de:

```python
catalog = "olist_prod"
```

---

## 14. Estándares de Nombres

Catálogos:

```text
olist_dev
olist_test
olist_prod
```

Schemas:

```text
bronze
silver
gold
metadata
quarantine
```

Tablas:

```text
customers
orders
order_items
order_payments
order_reviews
products
sellers
```

Tablas dimensionales:

```text
dim_customer
dim_product
dim_seller
dim_date
dim_geography
```

Tablas de hechos:

```text
fact_orders
fact_order_items
fact_payments
fact_reviews
```

Columnas técnicas:

```text
_ingestion_timestamp
_source_file
_run_id
_record_hash
```

Columnas SCD Type 2:

```text
effective_start_date
effective_end_date
is_current
```

La convención completa de nombres se mantendrá separadamente en:

```text
docs/naming_conventions.md
```

---

## 15. Principios Arquitectónicos

El proyecto seguirá los siguientes principios:

### Separación de responsabilidades

Cada capa tendrá una responsabilidad claramente definida:

```text
Landing → ingestión
Bronze  → persistencia de datos de origen
Silver  → limpieza y conformación
Gold    → analítica de negocio
```

### Aislamiento de ambientes

Desarrollo, pruebas y producción estarán separados a nivel de catálogo de Unity Catalog.

### Seguridad por defecto

Las credenciales no deberán estar embebidas dentro del código de la aplicación.

### Infraestructura y configuración como código

La configuración de despliegue deberá estar versionada.

### Reproducibilidad

El mismo código fuente deberá producir resultados equivalentes en diferentes ambientes cuando se utilicen configuraciones y datos equivalentes.

### Mínimo privilegio

Los usuarios y workloads recibirán únicamente los permisos necesarios para sus responsabilidades.

### Managed-first storage

Las tablas estructuradas del lakehouse deberán utilizar managed storage de Unity Catalog siempre que resulte práctico.

---

## 16. Arquitectura Objetivo

La arquitectura de ambientes resultante será:

```text
                             UNITY CATALOG
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        │                         │                         │
        ▼                         ▼                         ▼
   OLIST_DEV                OLIST_TEST                OLIST_PROD
        │                         │                         │
   ┌────┼────┐               ┌────┼────┐               ┌────┼────┐
   │    │    │               │    │    │               │    │    │
Bronze Silver Gold          Bronze Silver Gold          Bronze Silver Gold
   │    │    │               │    │    │               │    │    │
   └────┴────┘               └────┴────┘               └────┴────┘
        │                         │                         │
     Metadata                  Metadata                  Metadata
        │                         │                         │
     Quarantine                Quarantine                Quarantine
```

Fuente de datos:

```text
                    ADLS GEN2
                        │
                        ▼
               landing-zone-olist
                        │
                        ▼
              Unity Catalog access
                        │
                        ▼
                    Bronze
```

Despliegue:

```text
GitHub
   │
   ▼
CI/CD
   │
   ├── DEV
   │
   ├── TEST
   │
   └── PROD
```

---

## 17. Resumen de Decisiones

| Decisión                  | Enfoque seleccionado                    |
| ------------------------- | --------------------------------------- |
| Aislamiento de ambientes  | Catálogo por ambiente                   |
| Ambientes                 | Dev / Test / Prod                       |
| Organización Medallion    | Schemas                                 |
| Tablas estructuradas      | Managed Delta Tables                    |
| Fuente ADLS existente     | External Location                       |
| Datos de Landing          | ADLS Gen2                               |
| Aislamiento de producción | Restringido / aislado                   |
| Configuración             | Basada en ambiente                      |
| Despliegue                | CI/CD                                   |
| Gobierno                  | Unity Catalog                           |
| Credenciales              | Managed Identity / credenciales seguras |
| Naming                    | snake_case + convenciones dimensionales |

---

## 18. Extensiones Futuras

Esta arquitectura está preparada para incorporar las siguientes etapas:

```text
Data Quality
Quarantine
CDC
SCD Type 2
Metadata-driven ingestion
Observability
Performance Optimization
CI/CD
Inventory Analytics
Forecasting
Power BI
```

Ningún componente futuro deberá incorporarse únicamente por motivos tecnológicos. Cada componente debe tener un propósito definido y justificar su impacto arquitectónico.

---

## 19. Definition of Done

La etapa 0.3 se considera terminada cuando:

```text
[ ] Modelo de ambientes documentado
[ ] Estructura de Unity Catalog definida
[ ] Naming de catálogos definido
[ ] Responsabilidad de cada schema documentada
[ ] Estrategia de almacenamiento documentada
[ ] Estrategia de aislamiento de producción documentada
[ ] Estrategia de permisos documentada
[ ] Compatibilidad con CI/CD documentada
[ ] Estrategia de configuración por ambiente documentada
[ ] Arquitectura objetivo documentada
```

Una vez completada esta etapa, el proyecto puede continuar con:

**Etapa 0.4 — Acceso a Storage y Seguridad**
