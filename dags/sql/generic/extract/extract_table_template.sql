SELECT
    *
FROM generic
WHERE (modified_date BETWEEN '{{params.extract_from}}' AND '{{params.extract_to}}')
    OR (created_date BETWEEN '{{params.extract_from}}' AND '{{params.extract_to}}')