# 911 Calls avec ElasticSearch

## Import du jeu de données

Pour importer le jeu de données, complétez le script `import.js` (ici aussi, cherchez le `TODO` dans le code :wink:).

Exécutez-le ensuite :

```bash
npm install
node import.js
```

Vérifiez que les données ont été importées correctement grâce au shell (le nombre total de documents doit être `153194`) :

```
GET <nom de votre index>/_count
```

## Requêtes

À vous de jouer ! Écrivez les requêtes ElasticSearch permettant de résoudre les problèmes posés.

```
TODO : 

* Compter le nombre d'appels par catégorie

POST /911-calls/_search
{
  "size": 0,
  "aggs" : {
         "cat" : {
            "terms": {
                "field": "cat.keyword"
            }
        }
    }
}

* Trouver les 3 mois ayant comptabilisés le plus d'appels

POST /911-calls/_search
{
  "size": 0,
  "aggs" : {
         "timeStamp" : {
            "terms": {
                "field": "timeStamp",
                "value_type": "date",
                "size": 3
            }
        }
    }
}

* Trouver le top 3 des villes avec le plus d'appels pour overdose

POST /911-calls/_search
{
  "query": {
    "match": {
      "title.keyword": {
        "query": "EMS: OVERDOSE"
      }
    }
  },
  "size": 0,
  "aggs" : {
         "twp" : {
            "terms": {
                "field": "twp.keyword",
                "size": 3
            }
        }
    }
}

* Compter le nombre d'appels autour de Lansdale dans un rayon de 500 mètres

GET /911-calls/_count
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_distance": {
          "distance": "500m",
          "location": [-75.283783, 40.241493]
        }
      }
    }
  }
}
```

## Kibana

Dans Kibana, créez un dashboard qui permet de visualiser :

* Une carte de l'ensemble des appels
* Un histogramme des appels répartis par catégories
* Un Pie chart réparti par bimestre, par catégories et par canton (township)

Pour nous permettre d'évaluer votre travail, ajoutez une capture d'écran du dashboard dans ce répertoire [images](images).

### Bonus : Timelion
Timelion est un outil de visualisation des timeseries accessible via Kibana à l'aide du bouton : ![](images/timelion.png)

Réalisez le diagramme suivant :
![](images/timelion-chart.png)

Envoyer la réponse sous la forme de la requête Timelion ci-dessous:  

```
TODO : ajouter la requête Timelion ici
```
