SELECT DISTINCT
    operateur,
    CASE
        WHEN operateur IN ('ORANGE', 'SFR') THEN 'Opérateur historique'
        ELSE 'Opérateur alternatif'
    END AS categorie_operateur
FROM {{ ref('stg_emetteurs_5g') }}
