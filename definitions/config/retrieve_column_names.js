





// This list is derived from source table's schema, captured via INFORMATION_SCHEMA, and must be updated each time 
const allStgColumns =['survey_response_id'
,'survey_wave_id'
,'date'
,'wave_name'
,'src_response_id'
,'weight'
,'imd_quartile_country'
,'gender'
,'age_code'
,'qualification_2020'
,'gor_code'
,'ethnicity'
,'ql5'
,'ql7'
,'ql7a_1'
,'ql7a_2'
,'ql7a_3'
,'ql7a_4'
,'ql7a_5'
,'ql7a_6'
,'ql7a_7'
,'ql7a_8'
,'ql1a_3'
,'ql1c_3_1'
,'ql1c_3_2'

];

const demographicColumns = ['survey_response_id'
,'survey_wave_id'
,'date'
,'wave_name'
,'src_response_id'
,'weight'
,'imd_quartile_country'
,'gender'
,'age_code'
,'qualification_2020'
,'gor_code'
,'ethnicity'
];

// Here I have manually defined the responseColumns
const manualResponseColumns = ['ql5'
,'ql7'
,'ql7a_1'
,'ql7a_2'
,'ql7a_3'
,'ql7a_4'
,'ql7a_5'
,'ql7a_6'
,'ql7a_7'
,'ql7a_8'
,'ql1a_3'
,'ql1c_3_1'
,'ql1c_3_2'
];
// this exports the variable to global so other files can use files (would this overwrite existing module.exports?)

// Here use filter for where they are not present in the demographic columns
const automaticResponseColumns = allStgColumns.filter(column => !demographicColumns.includes(column));


module.exports = {
  manualResponseColumns: manualResponseColumns,
  demographicColumns: demographicColumns,
  allStgColumns: allStgColumns,
  automaticResponseColumns: automaticResponseColumns
};