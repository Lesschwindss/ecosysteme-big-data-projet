SELECT
    f.departement,
    CASE f.departement WHEN '075' THEN 'Paris' WHEN '091' THEN 'Essonne' END AS nom_departement,
    o.operateur,
    o.categorie_operateur,
    count(*) AS total_emetteurs,
    sum(f.est_operationnel) AS emetteurs_actifs,
    sum(1 - f.est_operationnel) AS projets_approuves,
    count(DISTINCT f.support_id) AS sites_uniques,
    sum(CASE WHEN f.est_operationnel = 1 AND f.est_bande_3500 = 1 THEN 1 ELSE 0 END) AS emetteurs_3500_actifs,
    sum(CASE WHEN f.est_operationnel = 1 THEN f.nb_secteurs ELSE 0 END) AS secteurs_actifs,
    round(avg(s.hauteur_m), 1) AS hauteur_moy_m,
    round(100.0 * sum(f.est_operationnel) / count(*), 1) AS taux_activation_pct,
    round(
        100.0 * sum(CASE WHEN f.est_operationnel = 1 AND f.est_bande_3500 = 1 THEN 1 ELSE 0 END)
        / nullif(sum(f.est_operationnel), 0),
        1
    ) AS part_3500_actifs_pct
FROM {{ ref('fct_emetteurs_5g') }} f
JOIN {{ ref('dim_operateur') }} o ON f.operateur = o.operateur
JOIN {{ ref('dim_support') }} s ON f.support_id = s.support_id AND f.departement = s.departement
GROUP BY f.departement, nom_departement, o.operateur, o.categorie_operateur
ORDER BY f.departement, emetteurs_actifs DESC
