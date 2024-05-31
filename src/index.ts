import * as readline from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import { ConsoleLogger, Conversation, GraphModel, GraphModelOptions, getOpenAiEmbedding } from '@accordproject/concerto-graph';
import Database from 'better-sqlite3';

/**
 * List of people associated with the Dune movies.
 */
const DUNE_PEOPLE = ["David Lynch", "Denis Villeneuve", "Kyle MacLachlan", "Francesca Annis",
  "Brad Dourif", "José Ferrer", "Linda Hunt",
  "Freddie Jones", "Richard Jordan", "Everett McGill", "Silvana Mangano", "Virginia Madsen",
  "Sting", "Kenneth McMillan", "Jack Nance", "Siân Phillips", "Jürgen Prochnow",
  "Paul L. Smith", "Patrick Stewart", "Dean Stockwell", "Max von Sydow", "Alicia Witt",
  "Sean Young", "Timothée Chalamet", "Rebecca Ferguson", "Oscar Isaac", "Josh Brolin",
  "Stellan Skarsgård", "Dave Bautista", "Stephen McKinley Henderson", "Zendaya", "David Dastmalchian",
  "Chang Chen", "Sharon Duncan-Brewster", "Charlotte Rampling", "Jason Momoa", "Javier Bardem"];

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
@description("Ask a question about a movie or a person related to movies...")
namespace ${NS}
import org.accordproject.graph@1.0.0.{GraphNode}

@questions("How many people are in the database?", 
"What year was Eva Green born?",
"What movies is Kevin Bacon known for?",
"What is the shortest path from Eva Green to Kevin Bacon?"
)
concept Person extends GraphNode {
  @vector_index("embedding", 1536, "COSINE")
  @fulltext_index
  o String name
  o Double[] embedding optional
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

@questions("What people are related to the movie Dune 2021?", 
"What is the highest rated movie about natural disasters released after 2000",
"What is the longest movie that has more than 1000 votes?",
"What is a movie set in the capital of France?"
)
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
  o String wikiPage optional
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
      summary: row.Plot ? row.Plot : undefined,
      wikiPage: row['Wiki Page']
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
    const knownFor = row.knownForTitles.trim().split(',');
    for (let n = 0; n < knownFor.length; n++) {
      await graphModel.mergeRelationship(transaction, 'Person', row.nconst, 'Movie', knownFor[n], 'knownFor');
    }
    const primaryProfessions = row.primaryProfession.trim().split(',');
    for (let n = 0; n < primaryProfessions.length; n++) {
      const profession = primaryProfessions[n];
      if (profession && profession.length > 0) {
        await graphModel.mergeNode(transaction, 'Profession', { identifier: profession });
        await graphModel.mergeRelationship(transaction, 'Person', row.nconst, 'Profession', primaryProfessions[n], 'professions');
      }
    }
    console.log(`${row.primaryTitle} (${row.startYear})`)
  });
  await session.close();
}

async function run() {
  checkEnv('NEO4J_PASS');
  checkEnv('NEO4J_URL');

  const logger = ConsoleLogger;
  const options: GraphModelOptions = {
    NEO4J_USER: process.env.NEO4J_USER,
    NEO4J_PASS: process.env.NEO4J_PASS,
    NEO4J_URL: process.env.NEO4J_URL,
    logger,
    logQueries: false,
    embeddingFunction: process.env.OPENAI_API_KEY ? getOpenAiEmbedding : undefined
  }
  const graphModel = new GraphModel([MODEL], options);
  await graphModel.connect();
  await graphModel.mergeConcertoModels();
  await graphModel.dropIndexes();
  await graphModel.createIndexes();
  const db = new Database('im.db', { readonly: true, fileMustExist: true });

  const stmt = db.prepare(SELECT_TITLES_BY_PARTICIPANT);
  const convoOptions = {
    toolOptions: {
      getById: true,
      chatWithData: true,
      fullTextSearch: true,
      similaritySearch: true
    },
    maxContextSize: 64000,
    logger
  };

  let convo = new Conversation(graphModel, convoOptions);
  let done = false;
  const rl = readline.createInterface({ input, output });
  while (!done) {
    try {
      const command = await rl.question('Enter command (dune,add,search,query,delete,quit,reset) or just chat: ');
      switch (command) {
        case 'add': {
          const actor = await rl.question('Enter the name of a movie participant: ');
          graphModel.options.logger?.log(`Loading IMDB data for movie participant: ${actor}`);
          const rows = stmt.all(actor);
          graphModel.options.logger?.log(`Found ${rows.length} titles.`);
          for (let n = 0; n < rows.length; n++) {
            await addRowToGraph(graphModel, rows[n]);
          }
        }
          break;
        case 'dune': {
          for (let n = 0; n < DUNE_PEOPLE.length; n++) {
            const person = DUNE_PEOPLE[n];
            graphModel.options.logger?.log(`Loading movies for ${person}...`);
            const rows = stmt.all(person);
            for (let i = 0; i < rows.length; i++) {
              const row = rows[i];
              try {
                await addRowToGraph(graphModel, row);
              }
              catch (err) {
                console.log(err);
              }
            }
          }
        }
          break;
        case 'quit':
          done = true;
          break;
        case 'delete': {
          const confirm = await rl.question('Enter \'Y\' to confirm deletion of all graph data: ');
          if (confirm === 'Y') {
            await graphModel.deleteGraph();
            graphModel.options.logger?.log('All graph data deleted.');
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
        case 'reset': {
          if (process.env.OPENAI_API_KEY) {
            convo = new Conversation(graphModel, convoOptions);
            graphModel.options.logger?.log('Reset conversation');
          }
          break;
        }
        default: {
          if (process.env.OPENAI_API_KEY) {
            const result = await convo.appendUserMessage(command);
            console.log(result);
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