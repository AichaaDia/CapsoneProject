# Rapport de Qualité des Données

## Identifiant d’exécution
2026-01-30

## Objectif
Ce rapport présente les contrôles de qualité appliqués au jeu de données SILVER JOINED
avant son exploitation dans la couche GOLD.

## Contrôles réalisés
8 contrôles qualité ont été exécutés, couvrant :
- la complétude des données
- la cohérence temporelle
- l’unicité des identifiants
- la validité des montants
- des règles métiers

## Résultats
La majorité des contrôles sont validés (PASS).
Aucune anomalie critique bloquante n’a été détectée.

## Analyse
- Les identifiants de transactions sont uniques et non nuls
- Les montants sont tous positifs
- Les dates sont cohérentes avec l’année déclarée
- Le ratio de transactions à forte valeur est conforme aux règles métiers

## Conclusion
Le jeu de données est conforme aux exigences de qualité et peut être utilisé
pour la construction des marts analytiques et des exports BI.
