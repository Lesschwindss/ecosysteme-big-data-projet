WITH supports_actifs AS (
    SELECT
        departement,
        support_id,
        count(DISTINCT operateur) AS nb_operateurs,
        count(DISTINCT bande_frequence) AS nb_bandes
    FROM {{ ref('fct_emetteurs_5g') }}
    WHERE est_operationnel = 1
    GROUP BY departement, support_id
)
SELECT
    departement,
    CASE departement WHEN '075' THEN 'Paris' WHEN '091' THEN 'Essonne' END AS nom_departement,
    nb_operateurs,
    count(*) AS nb_supports,
    round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY departement), 1) AS pct_supports
FROM supports_actifs
GROUP BY departement, nom_departement, nb_operateurs
ORDER BY departement, nb_operateurs
