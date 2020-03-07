# wiki_import

Three small scripts to import wikipedia and wikidata dumps into postgres:

## import_wikipedia

Schema:
```
CREATE TABLE wikipedia (
  title TEXT PRIMARY KEY,
  infobox TEXT,
  wikitext TEXT,
  templates TEXT[] NOT NULL DEFAULT '{}',
  categories TEXT[] NOT NULL DEFAULT '{}',
  general TEXT[] NOT NULL DEFAULT '{}'
)
```

You'll need a dump of the wikipedia (something-pages-articles.xml.bz2) and a postgres connection
 to something that has postgis installed. On OSX with brew:
 
     brew install postgres
     brew install postgis
  
  
 It will import
all the pages and make them searchable using the name, the id of the infobox (which more or less is equal to the
primary type of the thing described in the article), the categories, the templates and a generalized version of
the categories. Categories are generalized by taking the bit of the category before 'of' and 'in'. This can be
useful since many wikipedia categories are of the type [Cities in the Netherlands].

```select title from wikipedia where general @> ARRAY['cities'] limit 10```

Will get you a list of some cities, while:

```select title from  wikipedia where infobox = 'writer' and categories @> ARRAY['1905 births']```

Will get you a list of writers that were born in 1905

## import_wikidata

Schema:

```
CREATE TABLE wikidata (
    wikipedia_id TEXT PRIMARY KEY,
    title TEXT,
    wikidata_id TEXT,
    description TEXT,
    properties JSONB
)
```

The import_wikidata script is similar, but in a way more interesting. Wikidata is an explicit attempt to create a semantic representation of the data in all the wiki's. It uses a [Triplestore](https://en.wikipedia.org/wiki/Triplestore) which is
really powerful. It not only describes properties of objects, but also where the information came from, where more can be found, the sources and everything. It's great but often times way more detailed than needed.

Import wikidata does away with all this subtlety and instead maps every object in wikidata to a row in postgres under the id of the english wikipedia article. It also creates a neat json column called properties where most of the information from wikidata goes. There are some drawbacks to this approach - sometimes a field has one value in one record, but multiple values in another record. But always using lists everywhere is just plain uggly.

For any property that is not a reference to another object, it picks one value. Wikidata often has multiple values, either as part of a time series (the population of New York in 1900) or if there are multiple sources with different values. For fields that are a reference to another object, if there are multiple values, it will store those as a list, otherwise as a single value.

Once you've run the import you can run queries to get all information we have on something:

```select * from wikidata where wikipedia_id = 'Socrates'```

Since both tables are keyed on wikipedia_id's you can easily run joins too. The real fun is with the properties column which is of type jsonb and indexed. For example, things that are Red, White and Blue:

```SELECT title FROM wikidata WHERE properties @> '{"color": ["Blue", "Red", "White"]}'```

Or the German states and their population:

```SELECT title, properties->>'population' FROM wikidata WHERE properties @> '{"located in the administrative territorial entity":"Germany"}```

Or all the cities that have both Berlin and Los Angelos as their sister cities:

```SELECT title FROM wikidata WHERE properties @> '{"sister city": ["Los Angeles", "Berlin"]}```

(Mexico City, London, Tehran and Jakarta is the answer)


## import_stats

Wikipedia also published dumps of their hourly pageview counts in https://dumps.wikimedia.org/other/pagecounts-ez/
While not perfect, it does give you an idea of the relative importance of a specific wikipedia page. The schema is quite simple:

```
CREATE TABLE wikidata (
    title TEXT PRIMARY KEY,
    viewcount INTEGER
)
```

To make things a little easier, I've added a flag --dumps_to_fetch to the import_stats.py script. This will fetch that many hourly dumps from roughly the last year. They are randomly selected. After that it will import them into the postgres db you point it at.

The table itself is not that interesting, but you can do joins to find out who are the most popular philosopers:

```
select wikipedia.title, wikistats.viewcount from wikipedia join wikistate wikipedia.infobox = 'philosopher' order by wikistats.viewcount desc limit 25
```

Or you can go all fancy and do a three way join to get the top capitals with their population.
