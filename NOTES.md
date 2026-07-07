Notes techniques — Pipeline FHIR

1. Stratégie de traitement

Réconciliation des identités : Utilisation d'une jointure sur identifiants_ipp.csv afin de mapper les IPP dépréciés vers les IPP actifs.

Déduplication : Utilisation d'une fenêtre Spark (row_number) partitionnée par ipp_actif et triée par date_fin_validite pour ne conserver que la version la plus récente de chaque patient.

Format de sortie : Choix du format JSON. Ce format est le standard natif pour l'API FHIR R4, ce qui garantit une interopérabilité immédiate et simplifie l'ingestion par les serveurs cibles.

2. Anomalies détectées et résolutions

Formats de date : Grande disparité (dd/MM/yyyy, yyyy/MM/dd, etc.). Résolution via une fonction de normalisation utilisant coalesce sur plusieurs formats to_date.

Sexe : Données hétérogènes (1, 2, M, F, Homme, Femelle). Standardisation effectuée via une logique when/otherwise pour converger vers les valeurs attendues par FHIR (male, female).

IPP en doublon : Résolus par la logique de mapping des identifiants (Golden Record).

3. Améliorations Potentiels (Roadmap)

Tests unitaires : Implémenter des tests avec ScalaTest pour valider les transformations sur des jeux de données mockés.

Gestion des erreurs : Mise en place d'une "Dead Letter Queue" (DLQ) pour isoler et inspecter les lignes ne respectant pas les schémas attendus (ex: date de naissance invalide).

Scalabilité : Pour un passage à l'échelle (volumétrie massive), implémenter un partitionnement de la sortie sur le disque par birthDate ou resourceType.