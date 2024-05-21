---
title: Movie Graph
description: Demo Graph DB
tags:
  - Concerto
  - Neo4J
  - Concerto Graph
---

# Movie Graph

    Create your own personal Movie Knowledge Graph using data from IMDB!

This project uses [Concerto Graph](https://github.com/accordproject/lab-concerto-graph) to load
data about Movies, actors, and plot summaries into a Neo4J graph database and then presents a
command line interface to query the data using natural language.

![demo](demo.png)
[Code](src/index.ts)

## Install

### Download IMDB Data

Download the following data sets (free for non-commercial use) from [IMDB](https://developer.imdb.com/non-commercial-datasets/):
- title.basics
- name.basics
- title.principals
- title.ratings

Each file is a zipped tsv (tab-separated-values) file. Save the files in the ./imdb folder.

### Download Wiki Movie Plots CSV

The plot summaries for a selection of movies (not all) are not part of the public IMDB data sets so must be downloaded separately from Kaggle.

> A (free) Kaggle account is required

https://www.kaggle.com/datasets/jrobischon/wikipedia-movie-plots

Save the downloaded csv file in the ./imdb folder.

### Load Data Into SQLite

> Note that on Mac OS X SQLite is installed by default. On other platforms you may have to install it manually.

Launch SQLite:

```bash
sqlite3 im.db
```

Then in the SQLite shell, run the following commands. Run each command separately; some of the commands may take several minutes to complete:

```
.mode ascii
.separator "\t" "\n"
.import ./imdb/title.basics.tsv titles
.import ./imdb/name.basics.tsv names
.import ./imdb/title.principals.tsv principals
.import ./imdb/title.ratings.tsv ratings
.mode csv
.import ./imdb/wiki_movie_plots_deduped.csv plots

create index titles_id on titles(tconst);
create index names_id on names(nconst);
create index principals_id on principals(tconst);
create index ratings_id on ratings(tconst);
create index names_primaryName on names(primaryName);
create index principals_name on principals(nconst);
```

You should now have an ±8GB SQLite database containing most of the IMDB data, indexed for retrieval,
and ready to be inserted into your Knowledge Graph.

## Set Environment Variables

Export the following environment variables to your shell. 

Unix:

```bash
export NEO4J_URL=YOUR_URL
export NEO4J_PASS=YOUR_PASS
export OPENAI_API_KEY=YOUR_API_KEY
```

### GraphDB

- NEO4J_URL: the NEO4J URL. E.g. `neo4j+s://<DB_NAME>.databases.neo4j.io` if you are using AuraDB.
- NEO4J_PASS: your neo4j password.
- NEO4J_USER: <optional> defaults to `neo4j`

### Text Embeddings & Chat With Data
- OPENAI_API_KEY: <optional> the OpenAI API key. If not set embeddings are not computed and written to the agreement graph and similarity search is not possible.

## Running

```bash
npm start
```

Then use the following commands:
- add: adds all the movies related to a specific person to the graph database
- delete: deletes all nodes from the graph database
- search: full text search over movie nodes
- query: similarity (conceptual) search over movie nodes
- other: converts natural language queries to graph queries and runs them
- quit: to exit