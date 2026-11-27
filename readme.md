# TP1 - Traitement Batch avec Hadoop (HDFS et MapReduce) en Python

Version Python du TP1 Big Data : un cluster Hadoop de 3 noeuds lancé avec Docker, et des jobs MapReduce écrits en **Python** avec **Hadoop Streaming** (au lieu de Java).

Deux traitements sont réalisés :

1. **WordCount** : compter les occurrences de chaque mot.
2. **Application** : calculer le **total des ventes par magasin** à partir de `purchases.txt`.

## Structure du projet

```
hadoop-python/
├── wordcount_mapper.py      # Map du WordCount
├── wordcount_reducer.py     # Reduce du WordCount
├── magasin_mapper.py        # Map : emet (magasin, cout)
├── magasin_reducer.py       # Reduce : somme des couts par magasin
├── run_local.py             # (optionnel) simule map -> sort -> reduce en local
├── sample_purchases.txt     # (optionnel) petit fichier de test
└── README.md
```

## Prérequis

- Docker installé et fonctionnel
- Cluster Hadoop 2.7.2 (image `liliasfaxi/spark-hadoop:hv-2.7.2`) : 1 master + 2 slaves
- Python 3 (dans le conteneur : **Python 3.5.2**)
- Le fichier `purchases.txt` (environ 211 Mo), format : `date  temps  magasin  produit  cout  paiement`, colonnes séparées par des **tabulations**

> **Important : Python 3.5.** Le conteneur n'accepte pas les f-strings (`print(f"...")`, Python 3.6+). Les scripts utilisent donc `print("%s\t%d" % (...))`, compatible avec toutes les versions.

## 1. Lancer le cluster

Sur la machine hôte (PowerShell / terminal) :

```bash
docker pull liliasfaxi/spark-hadoop:hv-2.7.2
docker network create --driver=bridge hadoop

docker run -itd --net=hadoop -p 50070:50070 -p 8088:8088 -p 7077:7077 \
  --name hadoop-master --hostname hadoop-master liliasfaxi/spark-hadoop:hv-2.7.2
docker run -itd -p 8040:8042 --net=hadoop --name hadoop-slave1 \
  --hostname hadoop-slave1 liliasfaxi/spark-hadoop:hv-2.7.2
docker run -itd -p 8041:8042 --net=hadoop --name hadoop-slave2 \
  --hostname hadoop-slave2 liliasfaxi/spark-hadoop:hv-2.7.2

docker ps                              # vérifier que les 3 conteneurs sont "Up"
docker exec -it hadoop-master bash     # entrer dans le master
```

Dans le conteneur :

```bash
./start-hadoop.sh      # démarre HDFS et YARN
jps                    # NameNode, SecondaryNameNode, ResourceManager
```

> **Repère :** prompt `PS C:\...` = machine hôte (commandes `docker ...`). Prompt `root@hadoop-master` = conteneur (commandes `hadoop ...`).

## 2. Charger les données dans HDFS

Dans le conteneur :

```bash
hadoop fs -mkdir -p input
hadoop fs -put -f /root/purchases.txt input
hadoop fs -ls input
hadoop fs -tail input/purchases.txt
```

Si `mkdir` répond `No such file or directory` : `hadoop fs -mkdir -p /user/root`.

## 3. Tester en local (sans Hadoop)

Le `sort` simule la phase shuffle & sort de Hadoop.

```bash
# Linux / macOS / Git Bash / conteneur
cat file.txt | python3 wordcount_mapper.py | sort | python3 wordcount_reducer.py
cat sample_purchases.txt | python3 magasin_mapper.py | sort | python3 magasin_reducer.py
```

Sous Windows (PowerShell) :

```powershell
python run_local.py wordcount_mapper.py wordcount_reducer.py file.txt
python run_local.py magasin_mapper.py magasin_reducer.py sample_purchases.txt
```

Les fichiers de test doivent contenir de vraies **tabulations** (en PowerShell : `` `t ``).

## 4. Copier les scripts dans le conteneur

Depuis la **machine hôte** (jamais dans le conteneur) :

```powershell
docker cp . hadoop-master:/root/hadoop-python
```

Puis, dans le conteneur, vérifier l'absence de f-strings :

```bash
cd /root/hadoop-python
grep -n 'print(f' *.py      # ne doit rien afficher
```

## 5. Exécuter sur le cluster

### WordCount

```bash
hadoop fs -rm -r -f output_wc

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
  -files wordcount_mapper.py,wordcount_reducer.py \
  -mapper "python3 wordcount_mapper.py" \
  -reducer "python3 wordcount_reducer.py" \
  -input input/purchases.txt \
  -output output_wc

hadoop fs -tail output_wc/part-00000
```

### Application : total des ventes par magasin

```bash
hadoop fs -rm -r -f output_magasin

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
  -files magasin_mapper.py,magasin_reducer.py \
  -mapper "python3 magasin_mapper.py" \
  -reducer "python3 magasin_reducer.py" \
  -input input/purchases.txt \
  -output output_magasin

hadoop fs -cat output_magasin/part-00000 | head -20
```

Variante avec **combiner** (même résultat, moins de données transférées) : ajouter `-combiner "python3 magasin_reducer.py"`.

Notes :

- Hadoop refuse d'écrire dans un dossier de sortie existant : supprimer avant chaque relance (`hadoop fs -rm -r -f ...`).
- Avec Streaming, le fichier résultat s'appelle `part-00000` (et non `part-r-00000` comme en Java).
- Suivi des jobs : http://localhost:8088 (état `FINISHED` / `SUCCEEDED`).

## 6. Récupérer les résultats sur Windows

```powershell
docker exec hadoop-master hadoop fs -cat output_magasin/part-00000 | Out-File -Encoding utf8 resultat_magasin.txt
docker exec hadoop-master hadoop fs -cat output_wc/part-00000 | Out-File -Encoding utf8 resultat_wordcount.txt
```

## Résultats obtenus

Total des ventes par magasin (extrait, `purchases.txt`) :

| Magasin | Total |
|---|---|
| Albuquerque | 10052311.42 |
| Anaheim | 10076416.36 |
| Anchorage | 9933500.40 |
| Arlington | 10072207.97 |
| Atlanta | 9997146.70 |

Validation sur un petit fichier (`text.txt`, 4 lignes) : `MagasinA = 100 + 150 = 250.00` et `MagasinB = 200 + 50 = 250.00`, ce qui correspond au résultat du job.

WordCount : les villes à deux mots sont coupées (`Las` et `Vegas` ont chacun 80178 occurrences), car le découpage se fait sur les espaces, comme `StringTokenizer` dans la version Java du TP.

## Principe de l'application (MapReduce)

| Phase | Traitement |
|---|---|
| Map | Découper la ligne sur la tabulation, vérifier 6 colonnes, émettre `(colonne 3 = magasin, colonne 5 = coût)` |
| Shuffle & sort | Hadoop regroupe et trie les couples par magasin |
| Reduce | Additionner les coûts de chaque magasin (2 décimales) |

## Problèmes fréquents

| Symptôme | Cause | Solution |
|---|---|---|
| `subprocess failed with code 1` (toutes les tentatives de map échouent) | f-strings avec Python 3.5 | Utiliser `print("%s\t%s" % (...))` |
| `bash: docker: command not found` | `docker cp` tapé dans le conteneur | Le lancer depuis PowerShell, ou créer le fichier dans le conteneur avec `cat > f.py <<'EOF'` |
| `hadoop` non reconnu (PowerShell) | Hadoop n'existe que dans le conteneur | `docker exec hadoop-master hadoop ...` |
| `File exists` au `put` | Fichier déjà dans HDFS | Rien à faire, ou `hadoop fs -put -f` |
| `Output directory ... already exists` | Dossier de sortie existant | `hadoop fs -rm -r -f output_xxx` |
| Résultat vide pour `magasin` | Colonnes séparées par des espaces | Utiliser des tabulations (`cat -A fichier` affiche `^I`) |
| `BrokenPipeError` avec `head` | `head` ferme le tube | Normal, à ignorer |

Pour diagnostiquer un job qui échoue :

```bash
hadoop fs -cat input/purchases.txt | head -1000 | python3 magasin_mapper.py | head -5
yarn logs -applicationId application_XXXX
```

## Homework : AWS EMR (RosettaHub)

Les mêmes scripts s'utilisent avec une étape **Streaming program** :

1. Déposer `magasin_mapper.py`, `magasin_reducer.py` et `purchases.txt` dans un bucket S3.
2. Créer un cluster EMR depuis RosettaHub.
3. Ajouter l'étape Streaming : mapper et reducer en `s3://...`, entrée `s3://.../purchases.txt`, sortie dans un dossier S3 inexistant.
4. Lire `part-00000` dans le dossier de sortie une fois l'étape `Completed`.
5. **Terminer le cluster** après l'expérience pour ne pas consommer de crédits.

Toujours tester le code en local avant de lancer un job sur le cluster.