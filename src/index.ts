import { GraphModel, GraphModelOptions, getOpenAiEmbedding } from '@accordproject/concerto-graph';

import * as fs from "fs";
import { parse } from 'csv-parse';
import sqlite3 from 'sqlite3';

type TitleBasics = {
  identifier: string
  tconst?: string // alphanumeric unique identifier of the title
  titleType: string // the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
  primaryTitle: string // the more popular title / the title used by the filmmakers on promotional materials at the point of release
  originalTitle: string // original title, in the original language
  isAdult: boolean // 0: non-adult title; 1: adult title
  startYear: number //YYYY – represents the release year of a title. In the case of TV Series, it is the series start year
  endYear: number // YYYY – TV Series end year. ‘\N’ for all other title types
  runtimeMinutes: number // primary runtime of the title, in minutes
  genres: Array<string> // includes up to three genres associated with the title
};

const MODEL = `
namespace imdb.graph@1.0.0
import org.accordproject.graph@1.0.0.{GraphNode}

concept Genre extends GraphNode {
}

concept Title extends GraphNode {
  o String titleType
  o String primaryTitle
  o String originalTitle
  o Boolean isAdult
  o Integer startYear optional
  o Integer endYear optional
  o Long runtimeMinutes optional
  o String[] genres optional
}
`;

function checkEnv(name: string) {
  if (!process.env[name]) {
    throw new Error(`Environment variable ${name} has not been set`);
  }
}

async function run() {
  checkEnv('NEO4J_PASS');
  checkEnv('NEO4J_URL');

  const options: GraphModelOptions = {
    NEO4J_USER: process.env.NEO4J_USER,
    NEO4J_PASS: process.env.NEO4J_PASS,
    NEO4J_URL: process.env.NEO4J_URL,
    logger: console,
    logQueries: false,
    embeddingFunction: process.env.OPENAI_API_KEY ? getOpenAiEmbedding : undefined
  }
  const graphModel = new GraphModel([MODEL], options);
  await graphModel.connect();
  await graphModel.dropIndexes();
  await graphModel.createConstraints();
  await graphModel.createVectorIndexes();
  const context = await graphModel.openSession();

  const MAX_TITLES = 100;
  const { session } = context;
  const parser = fs
    .createReadStream('./imdb/title.basics.tsv')
    .pipe(parse({
      delimiter: '\t',
      columns: true,
      trim: true,
      toLine: MAX_TITLES,
      cast: (value, context) => {
        if (context.header) return value;
        if (value === '\\N') return null;
        if (context.column === 'isAdult') {
          return value === '1';
        }
        if (context.column === 'startYear') {
          return Number.parseInt(value);
        }
        if (context.column === 'endYear') {
          return Number.parseInt(value);
        }
        if (context.column === 'runtimeMinutes') {
          return Number.parseInt(value);
        }
        if (context.column === 'genres') {
          return value.split(',');
        }
        return String(value);
      }
    }));

    const db = new sqlite3.Database(':memory:');
    db.close();

  for await (const record of parser) {
    await session.executeWrite(async transaction => {
      const titleData: TitleBasics = record;
      titleData.identifier = titleData.tconst ?? '';
      delete titleData.tconst;
      console.log(titleData.primaryTitle);
      return graphModel.mergeNode(transaction, 'Title', titleData);
    });
  }
  console.log('Created graph...');
  await graphModel.closeSession(context);
  console.log('done');
}

run();