# GOV.UK Polling Dataform

The dataform configuration for modelling GOV.UK polling data. The output tables are made available in BigQuery and Looker.

## Nomenclature

TBC

## Technical documentation

### Data Model

```mermaid
erDiagram
    survey_waves {
        STRING id PK
        STRING name
        STRING provider
        DATE start_date
        DATE end_date
    }

    survey_responses {
        STRING id PK
        STRING survey_wave_id FK
        DATE date
        FLOAT weight
        STRING quartile_country
        STRING gender
        STRING age
        STRING qualification_2020
        STRING gor_code
        STRING ethnicity
    }

    survey_waves ||--o{ survey_responses : "has"
```

### Running the test suite

TBC

## Licence

[LICENCE](LICENSE)
