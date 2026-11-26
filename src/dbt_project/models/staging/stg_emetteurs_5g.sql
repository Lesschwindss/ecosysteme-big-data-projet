SELECT
    cast(id AS integer) AS emetteur_id,
    trim(adm_lb_nom) AS operateur,
    trim(sup_id) AS support_id,
    trim(emr_lb_systeme) AS bande_frequence,
    emr_dt AS date_activation,
    extract(year FROM emr_dt)::integer AS annee_activation,
    trim(sta_nm_dpt) AS departement,
    trim(code_insee) AS code_insee,
    trim(sta_nm_anfr) AS station_anfr,
    hauteur_m,
    latitude,
    longitude,
    trim(statut) AS statut,
    nb_secteurs,
    anomalie_date_projet,
    CASE WHEN trim(statut) = 'Techniquement opérationnel' THEN 1 ELSE 0 END AS est_operationnel,
    CASE WHEN trim(emr_lb_systeme) = '5G NR 3500' THEN 1 ELSE 0 END AS est_bande_3500
FROM {{ source('silver', 'emetteurs_5g') }}
QUALIFY row_number() OVER (
    PARTITION BY operateur, support_id, bande_frequence
    ORDER BY date_activation DESC NULLS LAST
) = 1
