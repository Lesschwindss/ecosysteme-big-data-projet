SELECT
    emetteur_id,
    operateur,
    support_id,
    departement,
    code_insee,
    bande_frequence,
    statut,
    date_activation,
    annee_activation,
    nb_secteurs,
    est_operationnel,
    est_bande_3500,
    anomalie_date_projet
FROM {{ ref('stg_emetteurs_5g') }}
