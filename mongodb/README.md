# 911 Calls avec MongoDB

## Import du jeu de données

Pour importer le jeu de données, complétez le script `import.js` (cherchez le `TODO` dans le code :wink:).

Exécutez-le ensuite :

```bash
npm install
node import.js
```

Vérifiez que les données ont été importées correctement grâce au shell (le nombre total de documents doit être `153194`) :

```
use 911-calls
db.calls.count()
```

## Index géographique et index textuel

Afin de répondre aux différents problèmes, vous allez avoir besoin de créer deux index particuliers sur la collection des appels :

* Un index géographique de type `2dsphere` pour les coordonnées GPS des appels.
  * https://docs.mongodb.com/manual/core/2dsphere/#create-a-2dsphere-index
* Un index textuel sur le titre des appels pour pouvoir faire des recherches full-text sur ce champ (recherche des overdoses par exemple)
  * https://docs.mongodb.com/manual/core/index-text/#create-text-index

## Requêtes

À vous de jouer ! Écrivez les requêtes MongoDB permettant de résoudre les problèmes posés.

```
TODO : 

* Compter le nombre d'appels par catégorie

db.calls.find({"title":{$regex:/^EMS/}}).count()
db.calls.find({"title":{$regex:/^Fire/}}).count()
db.calls.find({"title":{$regex:/^Traffic/}}).count()

* Trouver les 3 mois ayant comptabilisés le plus d'appels

db.calls.aggregate({$group:{_id:"$timeStamp", nb_appel:{$sum:1}}}, {$sort:{nb_appel:-1}},{$limit:3})

* Trouver le top 3 des villes avec le plus d'appels pour overdose

db.calls.aggregate({$match:{title:"EMS: OVERDOSE"}},{$group:{_id:"$twp", nb_overdose:{$sum:1}}}, {$sort:{nb_overdose:-1}}, {$limit:3})

* Compter le nombre d'appels autour de Lansdale dans un rayon de 500 mètres

db.calls.createIndex( { location : "2dsphere" } )

db.calls.find(
   {"location":
       { $near :
          {
            $geometry: { type: "Point",  coordinates: [ -75.283783, 40.241493 ] },
            $maxDistance: 500
          }
       }
   }
).count()

Vous allez sûrement avoir besoin de vous inspirer des points suivants de la documentation :

* Proximity search : https://docs.mongodb.com/manual/tutorial/query-a-2dsphere-index/#proximity-to-a-geojson-point
* Text search : https://docs.mongodb.com/manual/text-search/#text-operator
* Aggregation Pipeline : https://docs.mongodb.com/manual/core/aggregation-pipeline/
* Aggregation Operators : https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/
