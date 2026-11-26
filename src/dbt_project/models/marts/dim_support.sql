SELECT
    support_id,
    departement,
    code_insee,
    max(hauteur_m) AS hauteur_m,
    avg(latitude) AS latitude,
    avg(longitude) AS longitude
FROM {{ ref('stg_emetteurs_5g') }}
GROUP BY support_id, departement, code_insee
