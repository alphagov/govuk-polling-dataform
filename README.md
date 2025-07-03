# GOV.UK Polling Dataform

The dataform configuration for modelling GOV.UK polling data. The output tables are made available in BigQuery and Looker.

## Nomenclature

TBC

## Technical documentation

### Data Model

```mermaid
erDiagram
    survey_waves {
        STRING survey_wave_id PK
        STRING name
        STRING provider
        DATE start_date
        DATE end_date
    }

    survey_responses {
        STRING survey_response_id PK
        STRING survey_wave_id FK
        STRING src_response_id
        DATE date
        FLOAT weight
        INTEGER imd_quartile_country
        STRING gender
        STRING age_group
        STRING qualification_2020
        STRING ethnicity
    }

    question_responses {
        STRING question_response_id PK
        STRING survey_response_id FK
        STRING question_id FK
    }

    questions {
        STRING question_id PK
        STRING src_question_id
        STRING question_text
        STRING question_type
    }

    question_response_choices {
        STRING question_response_choice_id PK
        STRING question_response_id FK
        STRING question_id FK
        STRING src_question_response_column_name
        STRING choice_value
        STRING choice_text
    }

    survey_wave_questions {
        STRING survey_wave_question_id PK
        STRING survey_wave_id FK
        STRING question_id FK
    }

    survey_waves ||--o{ survey_responses : ""
    survey_responses ||--o{ question_responses : ""
    question_response_choices ||--o{ question_responses : ""
    questions ||--o{ question_responses : ""
    questions ||--o{ survey_wave_questions : ""
    survey_waves ||--o{ survey_wave_questions : ""
```

### Development

TBC

### Deployment
Once you PR is reviewed and approved, merge into `main`.

The production release configuration is based on `main` and will compile once a day. To manually compile, go to [Release Configurations](https://console.cloud.google.com/bigquery/dataform/locations/europe-west2/repositories/polling/details/release-scheduling?hl=en&inv=1&invt=Ab1Ofw&project=gds-bq-reporting).
Then select the `production` configuration and `Start Execution`.

## Licence

[LICENCE](LICENSE)
