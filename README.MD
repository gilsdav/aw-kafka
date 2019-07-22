# .NET Kafka Workshop

![Logo](https://proxy.duckduckgo.com/iu/?u=https%3A%2F%2Fwww.heise.de%2Fimgs%2F18%2F1%2F5%2F4%2F6%2F3%2F6%2F1%2Fkafka-logo-wide-0dbc478e9de4b0d9.png)

## Démarrer les applications
Exécutez ces commandes dans des terminaux séparés:
* `npm run start-docker` 
* `npm run start-front-server`
* `npm run start-history-server`
* `npm run start-front`

## Découpe

Ce projet contient deux micro-services:
- **FrontendServer**: Server servant à intéragir avec les utilisateurs
- **HistoryServer**: Server qui stock l'historique des intéractions et permet de récupérer cet historique

## But
Remplacer tous les TODOs par ce qu'il faut.

## Documentation
https://github.com/confluentinc/confluent-kafka-dotnet
