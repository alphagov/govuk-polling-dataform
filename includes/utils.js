 const cleanAndCastColumn = (columnName) => {
    return `NULLIF(TRIM(CAST(${columnName} AS STRING)), '') AS ${columnName}`;


};

module.exports = {
cleanAndCastColumn: cleanAndCastColumn

};