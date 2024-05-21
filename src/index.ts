import * as readline from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import { GraphModel, GraphModelOptions, getOpenAiEmbedding } from '@accordproject/concerto-graph';
import Database from 'better-sqlite3';

/**
 * The SQL query that joins across the various tables
 */
const SELECT_TITLES_BY_PARTICIPANT = `
select * from titles 
    inner join principals on titles.tconst = principals.tconst 
	  inner join ratings on ratings.tconst = titles.tconst 
    inner join names on principals.nconst = names.nconst
    left join plots on titles.primaryTitle = plots.Title and titles.startYear = plots."Release Year"
where 
  names.primaryName=?`;

/**
 * Type for a consolidated result row from SELECT_TITLES_BY_PARTICIPANT
 */
type ImdbRow = {
  // titles
  tconst: string,
  titleType: string,
  primaryTitle: string
  originalTitle: string
  isAdult: string,
  startYear: string,
  endYear: string,
  runtimeMinutes: string,
  genres: string,
  // principals
  ordering: string,
  category: string,
  job: string,
  characters: string,
  // names (person)
  nconst: string,
  primaryName: string,
  birthYear: string,
  deathYear: string,
  primaryProfession: string,
  knownForTitles: string,
  // ratings
  averageRating: string,
  numVotes: string,
  // plots
  Plot: string,
  Director: string,
  "Release Year": string,
  Title: string,
  "Origin/Ethnicity": string,
  Cast: string,
  Genre: string,
  "Wiki Page": string,
}

const NS = 'demo.graph@1.0.0';

const MODEL = `
namespace ${NS}
import org.accordproject.graph@1.0.0.{GraphNode}

concept Person extends GraphNode {
  o String name
  o Integer birthYear
  o Integer deathYear optional
  o String primaryProfession
  @label("RELATED_TO")
  --> Movie[] movies optional
  @label("KNOWN_FOR")
  --> Movie[] knownFor optional
  @label("HAS_PROFESSION")
  --> Profession[] professions optional
}

concept Genre extends GraphNode {
}

concept Profession extends GraphNode {
}

concept Movie extends GraphNode {
  o String title
  o Boolean isAdult
  o Integer startYear
  o Integer endYear optional
  o Integer runtimeMinutes optional
  o Double averageRating optional
  o Integer numVotes optional
  o Double[] embedding optional
  @vector_index("embedding", 1536, "COSINE")
  @fulltext_index
  o String summary optional
  @label("IN_GENRE")
  --> Genre[] genres optional
}
`;

function checkEnv(name: string) {
  if (!process.env[name]) {
    throw new Error(`Environment variable ${name} has not been set`);
  }
}

function parseInt(str) {
  const result = Number.parseInt(str);
  return Number.isNaN(result) ? 0 : result;
}

function parseDouble(str) {
  const result = Number.parseFloat(str);
  return Number.isNaN(result) ? 0 : result;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function addRowToGraph(graphModel: GraphModel, row: ImdbRow) {
  // console.log(JSON.stringify(row, null, 2));
  const context = await graphModel.openSession();
  const { session } = context;
  await session.executeWrite(async transaction => {
    await graphModel.mergeNode(transaction, 'Movie', {
      identifier: row.tconst,
      title: row.primaryTitle,
      averageRating: parseDouble(row.averageRating),
      numVotes: parseInt(row.numVotes),
      isAdult: row.isAdult === '1',
      startYear: parseInt(row.startYear),
      endYear: row.endYear ? row.endYear !== "\\N" ? parseInt(row.endYear) : null : null,
      runtimeMinutes: row.runtimeMinutes !== "\\N" ? parseInt(row.runtimeMinutes) : null,
      summary: row.Plot ? row.Plot : undefined
    });
    const genres = row.genres.split(',');
    for (let n = 0; n < genres.length; n++) {
      await graphModel.mergeNode(transaction, 'Genre', { identifier: genres[n] });
      await graphModel.mergeRelationship(transaction, 'Movie', row.tconst, 'Genre', genres[n], 'genres');
    }
    await graphModel.mergeNode(transaction, 'Person', {
      identifier: row.nconst,
      name: row.primaryName,
      primaryProfession: row.primaryProfession,
      birthYear: parseInt(row.birthYear),
      deathYear: row.deathYear ? row.deathYear !== "\\N" ? parseInt(row.deathYear) : null : null,
    });
    await graphModel.mergeRelationship(transaction, 'Person', row.nconst, 'Movie', row.tconst, 'movies');
    const knownFor = row.knownForTitles.split(',');
    for (let n = 0; n < knownFor.length; n++) {
      await graphModel.mergeRelationship(transaction, 'Person', row.nconst, 'Movie', knownFor[n], 'knownFor');
    }
    const primaryProfessions = row.primaryProfession.split(',');
    for (let n = 0; n < primaryProfessions.length; n++) {
      await graphModel.mergeNode(transaction, 'Profession', { identifier: primaryProfessions[n] });
      await graphModel.mergeRelationship(transaction, 'Person', row.nconst, 'Profession', primaryProfessions[n], 'professions');
    }
    console.log(`${row.primaryTitle} (${row.startYear})`)
  });
  await session.close();
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
  await graphModel.createFullTextIndexes();
  const db = new Database('im.db', { readonly: true, fileMustExist: true });

  const stmt = db.prepare(SELECT_TITLES_BY_PARTICIPANT);

  let done = false;
  const rl = readline.createInterface({ input, output });
  while (!done) {
    try {
      const command = await rl.question('Enter command (add,search,query,delete,quit) or a natural language query: ');
      switch (command) {
        case 'add': {
          const actor = await rl.question('Enter the name of a movie participant: ');
          console.log(`Loading IMDB data for movie participant: ${actor}`);
          const rows = stmt.all(actor);
          console.log(`Found ${rows.length} titles.`);
          for (let n = 0; n < rows.length; n++) {
            await addRowToGraph(graphModel, rows[n]);
          }
        }
          break;
        case 'quit':
          done = true;
          break;
        case 'delete': {
          const confirm = await rl.question('Enter \'Y\' to confirm deletion of all data: ');
          if(confirm === 'Y') {
            await graphModel.deleteGraph();
            console.log('All graph data deleted.');
          }
          break;
        }
        case 'search': {
          if (process.env.OPENAI_API_KEY) {
            const search = await rl.question('Enter search string: ');
            console.log(`Fulltext search for movies using: '${search}'`);
            const results = await graphModel.fullTextQuery('Movie', search, 3);
            console.log(results);
          }
          break;
        }
        case 'query': {
          if (process.env.OPENAI_API_KEY) {
            const search = await rl.question('Enter query string: ');
            console.log(`Searching for movies similar to: '${search}'`);
            const results = await graphModel.similarityQuery('Movie', 'summary', search, 3);
            console.log(results);
          }
          break;
        }
        default: {
          if (process.env.OPENAI_API_KEY) {
            const results = await graphModel.chatWithData(command);
            console.log(JSON.stringify(results, null, 2));
          }
          break;
        }
      }
    }
    catch (err) {
      console.log(err);
    }
  }
  rl.close();
  console.log('done');
  process.exit();
}

run();