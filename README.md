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
        FLOAT weight
        INTEGER imd_quartile_country
        STRING gender
        STRING age_group
        STRING qualification
        STRING ethnicity
        STRING government_office_region
        STRING is_english_main_language
    }

    question_responses {
        STRING question_response_id PK
        STRING survey_response_id FK
        STRING question_id FK
    }

    questions {
        STRING question_id PK
        STRING src_question_id
        STRING question_group_text
        STRING question_text
        STRING question_type
    }

    question_response_choices {
        STRING question_choice_id PK
        STRING question_id FK
        STRING choice_value
        STRING choice_text
    }

    question_response_selections {
        STRING question_response_selection_id PK
        STRING question_response_id FK
        STRING selection_text
    }

    survey_wave_questions {
        STRING survey_wave_question_id PK
        STRING survey_wave_id FK
        STRING question_id FK
    }

    survey_waves ||--o{ survey_responses : ""
    survey_responses ||--o{ question_responses : ""
    question_responses ||--o{ question_response_selections : ""
    questions ||--o{ question_responses : ""
    questions ||--o{ survey_wave_questions : ""
    survey_waves ||--o{ survey_wave_questions : ""
    questions ||--o{ question_response_choices : "has a lookup of"

    %% Styling for the question_response_choices table
    style question_response_choices fill:#F9F2B2
```

### Development

#### Adding a new Wave
1. Create a new row in `definitions/lookups/lookup_survey_waves.sqlx`.
2. Upload the source CSV to the `govuk_polling_responses` dataset with the name `src_{provider}_wave_{number}`. For example, `src_bmg_wave_13`.
2. For each question, add a new row in the `definitions/lookups/lookup_survey_wave_questions.sqlx` file including the `wave_name` and `src_question_id`.
    If this wave has the same questions as previous waves, just copy and paste being sure to update the values in `wave_name`.
3. Execute the workflow to ensure `survey_wave_questions` is populated as expected.

#### Adding a new Question
1. Create a new row in `definitions/lookups/lookup_questions.sqlx`.
2. Create a new row in `definitions/lookups/lookup_question_response_choices.sqlx` using the corresponding `src_question_id`.
You'll need to know the name of the column in the source data which contains the values along with the coded values and the associated selection text.
3. Execute the workflow and inspect the output of the `question_response_choices` table in BigQuery.
4. Add the column which contains the coded values to the `UNPIVOT` code in `definitions/staging/stg_unpack_question_responses.sqlx`.

### Deployment
Once you PR is reviewed and approved, merge into `main`.

The production release configuration is based on `main` and will compile once a day. To manually compile, go to [Release Configurations](https://console.cloud.google.com/bigquery/dataform/locations/europe-west2/repositories/polling/details/release-scheduling?hl=en&inv=1&invt=Ab1Ofw&project=gds-bq-reporting).
Then select the `production` configuration and select `New compliation` which will sync to the latest changes on `main` branch. Then, go back to the "Releases & Scheduling" section and choose `Start Execution`.

## Licence

[LICENCE](LICENSE)
