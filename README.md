# Identity Matching Enterprise Edition Extension

[![Travis build status](https://travis-ci.com/src-d/eee-identity-matching.svg?token=WzaxY77NzbmrefwxuhAh&branch=master)](https://travis-ci.com/src-d/eee-identity-matching)

Match different identities of the same person using 🤖.

[Overview](#overview) • [How To Use](#how-to-use) • [Science](#science) • [Design Document](#design-document)

## Overview

People are using different e-mails and names (aka identities) when they commit their work to git. 
E-mails can be corporate, personal, special like users.noreply.github.com, etc. 
Names can be with Surname or without, with typos, no name, etc. 
Thus to get precise information about developer it is required to gather his/her identities 
and separate them from another person identities. 
This tool developed to solve exactly this problem that we called Identity Matching.

## How To Use

**Right now no pre-built binaries are available.**
Please refer to [How to build from source code](#how-to-build-from-source-code) section to build an executable.

Run `match-identities --help` to see all the parameters that you can configure. 

There are two use cases supported for `match-identities`.
1. [With gitbase](#use-with-gitbase)
1. [Without gitbase](#use-without-gitbase)

In both cases, you get a parquet file as a result. 
Read more in [Output format](#output-format) section.

### Use with gitbase

`match-identities` is designed to be used with [gitbase](https://github.com/src-d/gitbase). 
First of all, make sure you have a gitbase instance running with all repositories you are going to analyze.
Please refer to [gitbase](https://github.com/src-d/gitbase) documentation to get more information. 

Usage example:
```
match-identities --output matched_identities.parquet
```

You can also configure credentials with `--host`, `--port`, `--user` and `--password` flags. 

This command will query gitbase for commit identities with the following SQL: 
```sql
SELECT DISTINCT repository_id, commit_author_name, commit_author_email
FROM commits;
```

If you want to cache the gitbase output you can use the `--cache` flag. 
After the identities are fetched from gitbase, the matching process is run. 
Read [Science](#Science) section to learn more.

### Use without gitbase
If you run `match-identities` with the `--cache` option enabled you get a `csv` file with the cached [gitbase](https://github.com/src-d/gitbase) output.
Besides, if you already have a list of identities it is possible to run `match-identities` without gitbase involved.
Create a CSV file with columns `repo, email, name` and feed it to `--cache` parameter.

Example:
```
match-identities \
    --cache path/to/csv/file.csv \
    --output matched_identities.parquet
```
Note that you should replace `path/to/csv/file.csv` with a real path. 

### Output format 
As a result, you get a parquet file with 4 columns: 
1. `id` (`int64`) -- unique identifier of the person with the corresponding identity. 
1. `email` (`utf8`) -- e-mail of the identity.
1. `name` (`utf8`) -- name of the identity.
1. `repo` (`utf8`) -- repository of the commit.


Columns `email`, `name` and `repo` may contain empty values which mean no constraints.
For example:
```
id,email,name,repo
1,alice@gmail.com,"",""
1,"",alice,""
2,bob@gmail.com,"",""
2,"",bob,""
2,bob@inbox.com,"",""
2,"",no-name,bob/bobs-project
```

The output means the following.
There are two developers. 
Let's name them Alice (id is 1) and Bob (id is 2). 
If you see `alice@gmail.com` in commit author e-mail than the author is Alice.
It does not matter which name was used or the repository name was.
If you see `alice` in the commit author name then the author is Alice for any e-mail and repository.
The same for Bob, though Bob uses two email addresses `bob@gmail.com` and `bob@inbox.com`.
If you encounter a commit with the `no-name` author in `bob/bobs-project` repository then it is Bob's commit. 

### Convert parquet to CSV

If parquet is not convenient,
you can convert it to CSV using the python script in `research` directory:
```bash 
python3 ./research/parquet2csv.py matched_identities.parquet
```
The result will be saved in `matched_identities.csv` directory.
Run `python3 ./research/parquet2csv.py` to see more options.

Please note that pyspark must be installed. 

### External matching option

If the organization is using GitHub, Gitlab or Bitbucket,
it is possible to use their API to match identities by emails. 

**TODO**

## How to build

```bash
git clone https://github.com/src-d/eee-identity-matching
cd eee-identity-matching
make build
```

You'll see two directories with Linux and Macos binaries inside `build` directory. 

## Science

**TODO:** describe the approach.

There are two stages to match identities. 
The first is the precomputation which is run once on the whole dataset and remains unchanged during the subsequent steps. 
The second is the matching itself.
1. Precomputation:
    1. Gather lists of the most popular e-mails and names (by frequencies) on the whole dataset.
    1. Gather lists of emails and names that will be ignored (aka blacklists) on the whole dataset.
       They are related to CI, bots, etc.
1. Analysis:
   1. Gather the list of triplets `{email, name, repository}` from all the commits using gitbase.
   1. Filter the triplets by blacklists. 
      If a name or an e-mail is in the blacklist then the triplet is deleted. 
   1. Match identities by the same e-mail if this email is unpopular. 
      Unpopular means there is no such email in the collected list of popular e-mails.
   1. Match identities by the same name if it is unpopular. 
      For popular names we use (name, repository) pairs. 
   1. Save the result to a parquet file.


## Design Document

Can be found in 
[source{d} gdrive](https://docs.google.com/document/d/1oNo_rS5mHqEVk_yug8hbMWIpQaJeOUYZitR3jWnHJCs/edit#heading=h.qhzm4nnshexd).
