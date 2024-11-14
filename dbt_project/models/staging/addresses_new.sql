SELECT
    SUPPLIER_ID,
    UPPER(COUNTRY) AS COUNTRY_UPPERCASE
FROM {{ source('my_source', 'MyExternalTable') }};